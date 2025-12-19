import os
import re
import math
import sqlite3
import pickle
from collections import Counter, defaultdict


class Index:
    def __init__(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.data_path = os.path.join(base_dir, 'data', 'documents')
        self.db_path = os.path.join(base_dir, 'backend', 'core', 'index', 'inverted_index.db')
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_db()

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS index_table (term TEXT PRIMARY KEY, postings BLOB)')
        cur.execute('CREATE TABLE IF NOT EXISTS doc_meta (filename TEXT PRIMARY KEY, norm REAL)')
        cur.execute('CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value INTEGER)')
        conn.commit()
        conn.close()

    def tokenize(self, text):
        from backend.core.text_preprocess import TextPreprocessor
        preprocessor = TextPreprocessor()
        return re.findall(r'\w+', preprocessor.preprocess(text))

    def build_index(self):
        os.makedirs(self.data_path, exist_ok=True)
        files = [f for f in os.listdir(self.data_path) if f.endswith('.txt')]
        
        term_docs = defaultdict(set)
        doc_freqs = {}
        doc_lengths = {}
        
        for filename in files:
            path = os.path.join(self.data_path, filename)
            with open(path, 'r', encoding='utf-8') as f:
                tokens = self.tokenize(f.read())
            freqs = Counter(tokens)
            doc_name = filename[:-4]
            doc_freqs[doc_name] = freqs
            doc_lengths[doc_name] = len(tokens)
            for term in freqs:
                term_docs[term].add(doc_name)
        
        total = len(doc_freqs)
        if total == 0:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute('DELETE FROM index_table')
            cur.execute('DELETE FROM doc_meta')
            cur.execute('INSERT OR REPLACE INTO metadata VALUES ("total_docs", ?)', (0,))
            conn.commit()
            conn.close()
            return
        
        avg_doc_length = sum(doc_lengths.values()) / len(doc_lengths) if doc_lengths else 1
        
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        for term, docs in term_docs.items():
            df = len(docs)
            idf = math.log((total + 1) / (df + 1)) + 1
            postings = [(doc, doc_freqs[doc][term]) for doc in docs]
            cur.execute('INSERT OR REPLACE INTO index_table VALUES (?, ?)', (term, pickle.dumps(postings)))
        
        for doc_name, freqs in doc_freqs.items():
            norm = 0
            doc_len = doc_lengths[doc_name]
            length_norm = 0.5 + 0.5 * (doc_len / avg_doc_length) if avg_doc_length > 0 else 1.0
            
            for term, tf in freqs.items():
                df = len(term_docs[term])
                idf = math.log((total + 1) / (df + 1)) + 1
                tfidf = (1 + math.log(tf)) * idf / length_norm
                norm += tfidf * tfidf
            norm = math.sqrt(norm)
            cur.execute('INSERT OR REPLACE INTO doc_meta VALUES (?, ?)', (doc_name, norm))
        
        cur.execute('INSERT OR REPLACE INTO metadata VALUES ("total_docs", ?)', (total,))
        cur.execute('INSERT OR REPLACE INTO metadata VALUES ("avg_doc_length", ?)', (int(avg_doc_length),))
        conn.commit()
        conn.close()

    def get_total_docs(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('SELECT value FROM metadata WHERE key="total_docs"')
        row = cur.fetchone()
        conn.close()
        return int(row[0]) if row else 0

    def get_idf(self, term):
        total = self.get_total_docs()
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('SELECT postings FROM index_table WHERE term=?', (term,))
        row = cur.fetchone()
        conn.close()
        if row:
            postings = pickle.loads(row[0])
            df = len(postings)
            return math.log((total + 1) / (df + 1)) + 1
        return math.log((total + 1) / 1) + 1

    def create_vector(self, text):
        tokens = self.tokenize(text)
        if not tokens:
            return {}
        freqs = Counter(tokens)
        vector = {}
        for term, tf in freqs.items():
            idf = self.get_idf(term)
            vector[term] = (1 + math.log(tf)) * idf
        return vector

    def extract_keywords(self, text, top_n=5):
        from backend.core.text_preprocess import TextPreprocessor
        preprocessor = TextPreprocessor()
        
        original_words = re.findall(r'\w+', text)
        vector = self.create_vector(text)
        if not vector:
            return []
        
        sorted_terms = sorted(vector.items(), key=lambda x: x[1], reverse=True)
        
        keywords_original = []
        seen_stems = set()
        seen_lower = set()
        
        for stem, score in sorted_terms:
            if stem in seen_stems:
                continue
            
            best_word = None
            best_len = 0
            
            for word in original_words:
                word_lower = word.lower()
                if word_lower in seen_lower:
                    continue
                if preprocessor.preprocess(word) == stem:
                    if len(word) > best_len:
                        best_word = word
                        best_len = len(word)
            
            if best_word:
                keywords_original.append(best_word)
                seen_stems.add(stem)
                seen_lower.add(best_word.lower())
                
                if len(keywords_original) >= top_n:
                    break
        
        return keywords_original

    def get_postings(self, terms):
        if not terms:
            return {}
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        placeholders = ','.join('?' for _ in terms)
        cur.execute(f'SELECT term, postings FROM index_table WHERE term IN ({placeholders})', terms)
        rows = cur.fetchall()
        conn.close()
        return {term: pickle.loads(blob) for term, blob in rows}

    def get_doc_norm(self, doc_name):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('SELECT norm FROM doc_meta WHERE filename = ?', (doc_name,))
        row = cur.fetchone()
        conn.close()
        return float(row[0]) if row else 0.0
