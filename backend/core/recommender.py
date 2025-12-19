import os
import sqlite3
import math
from collections import defaultdict


class Recommender:
    def __init__(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.db_path = os.path.join(base_dir, 'backend', 'core', 'index', 'recommender.db')
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.engine = None
        self.init_db()

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS rec_scores (name TEXT PRIMARY KEY, score REAL, view_count INTEGER DEFAULT 0, last_score REAL DEFAULT 0)')
        cur.execute('CREATE TABLE IF NOT EXISTS doc_relations (doc1 TEXT, doc2 TEXT, strength REAL, PRIMARY KEY (doc1, doc2))')
        conn.commit()
        conn.close()

    def set_engine(self, engine):
        self.engine = engine

    def learn_from_results(self, results):
        items = []
        if results is None:
            return
        if hasattr(results, "items"):
            items = results.items
        elif isinstance(results, list):
            if results and hasattr(results[0], "document"):
                for r in results:
                    items.append((r.document, getattr(r, "score", 0.0)))
            else:
                items = results
        if not items:
            return
        
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        try:
            decay = 0.92
            cur.execute('UPDATE rec_scores SET score = score * ?', (decay,))
            
            doc_names = []
            for rank, pair in enumerate(items, 1):
                doc, score = pair
                try:
                    name = doc.name
                    doc_names.append(name)
                    
                    position_weight = 1.0 / math.sqrt(rank)
                    score_weight = float(score) * 2.0
                    delta = (position_weight + score_weight) / 2.0
                    
                except Exception:
                    continue
                if delta <= 0:
                    continue
                
                cur.execute(
                    'INSERT INTO rec_scores(name, score, view_count, last_score) VALUES(?, ?, 1, ?) '
                    'ON CONFLICT(name) DO UPDATE SET '
                    'score = rec_scores.score + excluded.score, '
                    'view_count = rec_scores.view_count + 1, '
                    'last_score = excluded.last_score',
                    (name, delta, float(score))
                )
            
            for i, doc1 in enumerate(doc_names):
                for doc2 in doc_names[i+1:]:
                    relation_strength = 0.1
                    cur.execute(
                        'INSERT INTO doc_relations(doc1, doc2, strength) VALUES(?, ?, ?) '
                        'ON CONFLICT(doc1, doc2) DO UPDATE SET strength = doc_relations.strength + excluded.strength',
                        (doc1, doc2, relation_strength)
                    )
                    cur.execute(
                        'INSERT INTO doc_relations(doc1, doc2, strength) VALUES(?, ?, ?) '
                        'ON CONFLICT(doc1, doc2) DO UPDATE SET strength = doc_relations.strength + excluded.strength',
                        (doc2, doc1, relation_strength)
                    )
            
            conn.commit()
        finally:
            conn.close()

    def get_document_recommendations(self, top_n=5):
        if top_n <= 0:
            return []
        from backend.core.document_manager import Document
        
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        cur.execute('SELECT name, score, view_count, last_score FROM rec_scores WHERE score > 0 ORDER BY score DESC LIMIT ?', (int(top_n * 5),))
        rows = cur.fetchall()
        
        candidate_scores = defaultdict(float)
        for name, score, view_count, last_score in rows:
            recency_boost = 1.0 + (last_score * 0.3)
            frequency_boost = 1.0 + (math.log(view_count + 1) * 0.2)
            final_score = score * recency_boost * frequency_boost
            candidate_scores[name] = final_score
        
        top_candidates = sorted(candidate_scores.items(), key=lambda x: x[1], reverse=True)[:max(3, top_n)]
        
        for doc_name, base_score in top_candidates[:3]:
            cur.execute('SELECT doc2, strength FROM doc_relations WHERE doc1 = ? ORDER BY strength DESC LIMIT 3', (doc_name,))
            related = cur.fetchall()
            for related_name, strength in related:
                if related_name not in candidate_scores or candidate_scores[related_name] < base_score * 0.5:
                    candidate_scores[related_name] += strength * base_score * 0.3
        
        conn.close()
        
        final_ranking = sorted(candidate_scores.items(), key=lambda x: x[1], reverse=True)
        
        docs = []
        for name, score in final_ranking:
            d = Document.get_by_name(name)
            if d:
                docs.append(d)
            if len(docs) >= top_n:
                break
        
        return docs
