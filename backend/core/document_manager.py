import os
import re
import uuid
import sqlite3


class Document:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DOCUMENTS_PATH = os.path.join(BASE_DIR, 'data', 'documents')
    DB_PATH = os.path.join(BASE_DIR, 'data', 'documents.db')

    def __init__(self, doc_id, name, path):
        self.id = doc_id
        self.name = name
        self.path = path
        self.keywords = []
        self.load_keywords()

    @staticmethod
    def init_storage():
        os.makedirs(Document.DOCUMENTS_PATH, exist_ok=True)
        conn = sqlite3.connect(Document.DB_PATH)
        conn.execute('''CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            file_path TEXT NOT NULL
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS keywords (
            document_id TEXT NOT NULL,
            keyword TEXT NOT NULL,
            FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
        )''')
        conn.commit()
        conn.close()

    @staticmethod
    def get_all():
        Document.init_storage()
        conn = sqlite3.connect(Document.DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('SELECT id, name, file_path FROM documents ORDER BY name')
        rows = cur.fetchall()
        docs = []
        for row in rows:
            doc = Document(row['id'], row['name'], row['file_path'])
            docs.append(doc)
        conn.close()
        return docs

    @staticmethod
    def get_by_id(doc_id):
        conn = sqlite3.connect(Document.DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('SELECT id, name, file_path FROM documents WHERE id = ?', (doc_id,))
        row = cur.fetchone()
        conn.close()
        if row:
            return Document(row['id'], row['name'], row['file_path'])
        return None

    @staticmethod
    def get_by_name(name):
        conn = sqlite3.connect(Document.DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('SELECT id, name, file_path FROM documents WHERE name = ?', (name,))
        row = cur.fetchone()
        conn.close()
        if row:
            return Document(row['id'], row['name'], row['file_path'])
        return None

    def load_keywords(self):
        conn = sqlite3.connect(Document.DB_PATH)
        cur = conn.cursor()
        cur.execute('SELECT keyword FROM keywords WHERE document_id = ?', (self.id,))
        self.keywords = [row[0] for row in cur.fetchall()]
        conn.close()

    def save_to_db(self, keywords):
        conn = sqlite3.connect(Document.DB_PATH)
        cur = conn.cursor()
        cur.execute('INSERT OR REPLACE INTO documents (id, name, file_path) VALUES (?, ?, ?)',
                    (self.id, self.name, self.path))
        cur.execute('DELETE FROM keywords WHERE document_id = ?', (self.id,))
        for kw in keywords:
            cur.execute('INSERT INTO keywords (document_id, keyword) VALUES (?, ?)', (self.id, kw))
        conn.commit()
        conn.close()
        self.keywords = keywords

    def delete_from_db(self):
        conn = sqlite3.connect(Document.DB_PATH)
        cur = conn.cursor()
        cur.execute('DELETE FROM documents WHERE id = ?', (self.id,))
        conn.commit()
        conn.close()

    def get_text(self):
        if not os.path.exists(self.path):
            alt = os.path.join(Document.DOCUMENTS_PATH, f"{self.name}.txt")
            if os.path.exists(alt):
                self.path = alt
                try:
                    conn = sqlite3.connect(Document.DB_PATH)
                    cur = conn.cursor()
                    cur.execute('UPDATE documents SET file_path = ? WHERE id = ?', (self.path, self.id))
                    conn.commit()
                    conn.close()
                except Exception:
                    pass
            else:
                raise FileNotFoundError(f"Файл не найден: {self.path}")
        with open(self.path, 'r', encoding='utf-8') as f:
            return f.read()

    def get_preprocessed_text(self):
        from backend.core.text_preprocess import TextPreprocessor
        preprocessor = TextPreprocessor()
        return preprocessor.preprocess(self.get_text())

    def matches_filters(self, filters):
        if not filters:
            return True
        from backend.core.text_preprocess import TextPreprocessor
        preprocessor = TextPreprocessor()
        
        doc_text = self.get_preprocessed_text()
        doc_words = set(doc_text.split())
        
        kw_lower = set(kw.lower() for kw in self.keywords)
        kw_stems = set(preprocessor.preprocess(kw) for kw in self.keywords)
        
        original_text = self.get_text()
        original_words_lower = set(re.findall(r'\w+', original_text.lower()))
        
        for f in filters:
            f_lower = f.lower()
            f_stem = preprocessor.preprocess(f)
            
            if f_lower in kw_lower:
                continue
            if f_stem and f_stem in kw_stems:
                continue
            if f_stem and f_stem in doc_words:
                continue
            if f_lower in original_words_lower:
                continue
            
            return False
        return True

    @staticmethod
    def create_new(name, text):
        if not name or not name.strip():
            raise ValueError("Имя документа не может быть пустым")
        if not text or not text.strip():
            raise ValueError("Текст документа не может быть пустым")
        if re.search(r'[<>:"/\\|?*]', name):
            raise ValueError("Имя документа содержит недопустимые символы")
        
        path = os.path.join(Document.DOCUMENTS_PATH, f"{name}.txt")
        if os.path.exists(path):
            raise FileExistsError(f"Документ с именем '{name}' уже существует")
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        
        doc = Document(str(uuid.uuid4()), name, path)
        doc.add_to_index()
        return doc

    def add_to_index(self):
        from backend.core.index import Index
        
        index = Index()
        original_text = self.get_text()
        keywords = index.extract_keywords(original_text, top_n=7)
        
        self.save_to_db(keywords)
        index.build_index()

    def delete(self):
        from backend.core.index import Index
        self.delete_from_db()
        if os.path.exists(self.path):
            os.remove(self.path)
        Index().build_index()

    @staticmethod
    def update_text(doc_id, new_text):
        if not new_text or not new_text.strip():
            raise ValueError("Текст документа не может быть пустым")
        
        doc = Document.get_by_id(doc_id)
        if not doc:
            doc = Document.get_by_name(doc_id)
        if not doc:
            raise ValueError(f"Документ '{doc_id}' не найден")
        
        path = doc.path
        if not os.path.exists(path):
            path = os.path.join(Document.DOCUMENTS_PATH, f"{doc.name}.txt")
            os.makedirs(Document.DOCUMENTS_PATH, exist_ok=True)
            doc.path = path
            try:
                conn = sqlite3.connect(Document.DB_PATH)
                cur = conn.cursor()
                cur.execute('UPDATE documents SET file_path = ? WHERE id = ?', (doc.path, doc.id))
                conn.commit()
                conn.close()
            except Exception:
                pass
        with open(path, 'w', encoding='utf-8') as f:
            f.write(new_text)
        doc.add_to_index()

    @staticmethod
    def delete_document(doc_id_or_name):
        doc = Document.get_by_id(doc_id_or_name)
        if not doc:
            doc = Document.get_by_name(doc_id_or_name)
        if not doc:
            raise ValueError(f"Документ '{doc_id_or_name}' не найден")
        doc.delete()
