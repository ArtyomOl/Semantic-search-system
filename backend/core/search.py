import os
import math
import sqlite3
import datetime


class SearchResult:
    def __init__(self, items):
        self.items = items


class SearchHistory:
    def __init__(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.db_path = os.path.join(base_dir, 'backend', 'core', 'index', 'search_history.db')
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_db()

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, query TEXT UNIQUE, timestamp TEXT)')
        conn.commit()
        conn.close()

    def add(self, query):
        if not query or not query.strip():
            return
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('SELECT id FROM history WHERE query = ?', (query,))
        if not cur.fetchone():
            cur.execute('INSERT INTO history (query, timestamp) VALUES (?, ?)', 
                       (query, datetime.datetime.now().isoformat()))
            conn.commit()
        conn.close()

    def get_all(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('SELECT query FROM history ORDER BY id DESC')
        queries = [row[0] for row in cur.fetchall()]
        conn.close()
        return queries

    def clear(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('DELETE FROM history')
        conn.commit()
        conn.close()


class SearchEngine:
    def __init__(self):
        from backend.core.index import Index
        self.index = Index()
        self.history = SearchHistory()

    def search(self, query_text, filters=None, add_to_history=True):
        if not query_text or not query_text.strip():
            raise ValueError("Поисковый запрос не может быть пустым")
        
        if add_to_history:
            self.history.add(query_text)
        
        from backend.core.text_preprocess import TextPreprocessor
        from backend.core.document_manager import Document
        
        preprocessor = TextPreprocessor()
        processed = preprocessor.preprocess(query_text)
        query_vector = self.index.create_vector(processed)
        
        if not query_vector:
            return []
        
        query_terms = list(query_vector.keys())
        postings_map = self.index.get_postings(query_terms)
        
        scores = {}
        for term, postings in postings_map.items():
            q_val = query_vector.get(term, 0)
            idf = self.index.get_idf(term)
            for doc_name, tf in postings:
                if tf > 0:
                    scores[doc_name] = scores.get(doc_name, 0) + q_val * (1 + math.log(tf)) * idf
        
        norm_q = math.sqrt(sum(v * v for v in query_vector.values()))
        if norm_q == 0:
            return []
        
        items = []
        all_docs = {d.name: d for d in Document.get_all()}
        
        for doc_name, score in scores.items():
            norm_d = self.index.get_doc_norm(doc_name)
            if norm_d == 0:
                continue
            
            similarity = score / (norm_q * norm_d)
            if similarity <= 0.1:
                continue
            
            doc = all_docs.get(doc_name)
            if not doc:
                continue
            
            if filters and not doc.matches_filters(filters):
                continue
            
            items.append((doc, similarity))
        
        items.sort(key=lambda pair: pair[1], reverse=True)
        return SearchResult(items)

    def get_similar_documents(self, doc_name, top_n=5):
        from backend.core.document_manager import Document
        
        doc = Document.get_by_name(doc_name)
        if not doc:
            return []
        
        text = doc.get_preprocessed_text()
        if not text:
            return []
        
        query_vector = self.index.create_vector(text)
        if not query_vector:
            return []
        
        query_terms = list(query_vector.keys())
        postings_map = self.index.get_postings(query_terms)
        
        scores = {}
        for term, postings in postings_map.items():
            q_val = query_vector.get(term, 0)
            idf = self.index.get_idf(term)
            for d_name, tf in postings:
                if d_name != doc_name and tf > 0:
                    scores[d_name] = scores.get(d_name, 0) + q_val * (1 + math.log(tf)) * idf
        
        norm_q = math.sqrt(sum(v * v for v in query_vector.values()))
        if norm_q == 0:
            return []
        
        items = []
        all_docs = {d.name: d for d in Document.get_all()}
        
        for d_name, score in scores.items():
            norm_d = self.index.get_doc_norm(d_name)
            if norm_d == 0:
                continue
            
            similarity = score / (norm_q * norm_d)
            if similarity <= 0:
                continue
            
            d = all_docs.get(d_name)
            if d:
                items.append((d, similarity))
        
        items.sort(key=lambda pair: pair[1], reverse=True)
        return SearchResult(items[:top_n])
