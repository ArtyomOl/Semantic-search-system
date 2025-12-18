import os
import sqlite3


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
        cur.execute('CREATE TABLE IF NOT EXISTS rec_scores (name TEXT PRIMARY KEY, score REAL)')
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
            decay = 0.95
            cur.execute('UPDATE rec_scores SET score = score * ?', (decay,))
            for rank, pair in enumerate(items, 1):
                doc, score = pair
                try:
                    name = doc.name
                    delta = float(score) / float(rank)
                except Exception:
                    continue
                if delta <= 0:
                    continue
                cur.execute(
                    'INSERT INTO rec_scores(name, score) VALUES(?, ?) '
                    'ON CONFLICT(name) DO UPDATE SET score = rec_scores.score + excluded.score',
                    (name, delta)
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
        cur.execute('SELECT name FROM rec_scores ORDER BY score DESC LIMIT ?', (int(top_n * 3),))
        rows = cur.fetchall()
        conn.close()
        docs = []
        for row in rows:
            d = Document.get_by_name(row[0])
            if d:
                docs.append(d)
            if len(docs) >= int(top_n):
                break
        return docs
