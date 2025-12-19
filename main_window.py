import sys
import os
from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication, QMessageBox, QListWidgetItem

from backend.core.document_manager import Document
from backend.core.search import SearchEngine
from text_reader_form import TextReaderForm
from backend.core.recommender import Recommender

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Поисковая система")
        self.resize(1100, 700)
        self.setMinimumSize(900, 600)
        
        self.engine = SearchEngine()
        self.documents = Document.get_all()
            
        self.current_doc_id = None
        self.pages = {}
        self.history = [0]
        self.current_idx = 0
        
        self.recommender = Recommender()
        self.recommender.set_engine(self.engine)

        central = QtWidgets.QWidget()
        central.setStyleSheet("""
            QWidget { background-color: #F8F9FC; color: #334155; font-family: -apple-system, 'SF Pro Text', 'Helvetica Neue', Helvetica, Arial, 'Roboto', 'Ubuntu', 'Cantarell', 'Noto Sans', 'DejaVu Sans', sans-serif; font-size: 14px; }
            QScrollBar:vertical { background: #F1F5F9; width: 10px; margin: 0; border-radius: 5px; }
            QScrollBar::handle:vertical { background: #CBD5E1; min-height: 30px; border-radius: 5px; }
        """)
        self.setCentralWidget(central)
        
        main_layout = QtWidgets.QVBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        self.nav_bar = self.create_nav_bar()
        main_layout.addWidget(self.nav_bar)

        self.stack = QtWidgets.QStackedWidget()
        main_layout.addWidget(self.stack)

        self.create_pages()
        
        self.go_to(self.pages["home"])

    def create_nav_bar(self):
        bar = QtWidgets.QWidget()
        bar.setFixedHeight(65)
        bar.setStyleSheet("background-color: #FFFFFF; border-bottom: 1px solid #E2E8F0;")
        layout = QtWidgets.QHBoxLayout(bar)
        layout.setContentsMargins(25, 0, 25, 0)
        
        self.btn_back = self.create_button("Назад", self.go_back, style="nav")
        self.btn_exit = self.create_button("Выйти", self.close, style="nav")
        
        layout.addWidget(self.btn_back)
        layout.addStretch()
        layout.addWidget(self.btn_exit)
        return bar

    def create_button(self, text, func, style="primary"):
        btn = QtWidgets.QPushButton(text)
        btn.setCursor(Qt.PointingHandCursor)
        btn.clicked.connect(func)
        
        base_style = "border-radius: 8px; font-weight: 600; padding: 0 20px;"
        
        if style == "nav":
            btn.setFixedHeight(40)
            btn.setStyleSheet(f"""
                QPushButton {{ background: transparent; color: #64748B; border: none; font-size: 14px; {base_style} }}
                QPushButton:hover {{ background: #F1F5F9; color: #1E293B; }}
                QPushButton:disabled {{ color: #CBD5E1; }}
            """)
        elif style == "primary":
            btn.setFixedHeight(45)
            btn.setStyleSheet(f"""
                QPushButton {{ background: #6C5CE7; color: white; border: none; font-size: 15px; {base_style} }}
                QPushButton:hover {{ background: #5b4cc4; }}
                QPushButton:pressed {{ background: #4e42a6; }}
            """)
        elif style == "secondary":
            btn.setFixedHeight(40)
            btn.setStyleSheet(f"""
                QPushButton {{ background: #FFFFFF; color: #64748B; border: 1px solid #E2E8F0; font-size: 14px; {base_style} }}
                QPushButton:hover {{ background: #F8FAFC; color: #1E293B; border-color: #CBD5E1; }}
            """)
        elif style == "menu":
            btn.setFixedHeight(55)
            btn.setStyleSheet(f"""
                QPushButton {{ background: #FFFFFF; color: #334155; border: 1px solid #E2E8F0; font-size: 16px; text-align: left; padding-left: 25px; {base_style} }}
                QPushButton:hover {{ background: #F8FAFC; border-color: #6C5CE7; color: #6C5CE7; }}
            """)
            
        return btn

    def create_pages(self):
        self.add_page("home", self.page_home)
        self.add_page("search", self.page_search)
        self.add_page("results", self.page_results)
        self.add_page("view", self.page_view)
        self.add_page("all_docs", self.page_all_docs)
        self.add_page("add_doc", self.page_add_doc)
        self.add_page("history", self.page_history)

    def add_page(self, name, func):
        widget = func()
        self.stack.addWidget(widget)
        self.pages[name] = self.stack.count() - 1

    def page_home(self):
        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(60, 40, 60, 40)
        layout.setAlignment(Qt.AlignTop)
        
        title = QtWidgets.QLabel("Поисковая система")
        title.setStyleSheet("font-size: 32px; color: #1E293B; font-weight: 700; margin-top: 20px;")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)
        
        subtitle = QtWidgets.QLabel("Семантический поиск по документам")
        subtitle.setStyleSheet("font-size: 16px; color: #94A3B8; margin-bottom: 30px;")
        subtitle.setAlignment(Qt.AlignCenter)
        layout.addWidget(subtitle)

        menu_container = QtWidgets.QWidget()
        menu_layout = QtWidgets.QVBoxLayout(menu_container)
        menu_layout.setSpacing(15)
        menu_layout.setContentsMargins(100, 0, 100, 0)

        actions = [
            ("Найти документ", lambda: self.go_to(self.pages["search"])),
            ("Все документы", lambda: self.go_to(self.pages["all_docs"])),
            ("Создать документ", lambda: self.go_to(self.pages["add_doc"])),
            ("История запросов", lambda: self.go_to(self.pages["history"]))
        ]

        for text, func in actions:
            btn = self.create_button(text, func, style="menu")
            menu_layout.addWidget(btn)
            
        layout.addWidget(menu_container)

        layout.addSpacing(10)

        rec_label = QtWidgets.QLabel("Рекомендуемое:")
        rec_label.setStyleSheet("color: #64748B; font-weight: 600; font-size: 14px;")
        layout.addWidget(rec_label)

        self.recommend_list = QtWidgets.QListWidget()
        self.recommend_list.setStyleSheet(self.list_style())
        self.recommend_list.setFixedHeight(150)
        self.recommend_list.itemClicked.connect(self.open_doc)
        layout.addWidget(self.recommend_list)
        
        return page

    def page_search(self):
        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(50, 40, 50, 40)
        layout.setSpacing(20)
        
        title = QtWidgets.QLabel("Поиск")
        title.setStyleSheet("font-size: 26px; font-weight: 700; color: #1E293B;")
        layout.addWidget(title)

        self.search_input = QtWidgets.QLineEdit()
        self.search_input.setPlaceholderText("Введите запрос...")
        self.search_input.setMinimumHeight(50)
        self.search_input.setStyleSheet(self.input_style())
        layout.addWidget(self.search_input)

        self.filter_input = QtWidgets.QLineEdit()
        self.filter_input.setPlaceholderText("Фильтр (теги через запятую)")
        self.filter_input.setMinimumHeight(45)
        self.filter_input.setStyleSheet(self.input_style())
        layout.addWidget(self.filter_input)
        
        btn_search = self.create_button("Найти", self.do_search)
        layout.addWidget(btn_search)
        layout.addStretch()
        return page

    def page_results(self):
        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(50, 40, 50, 40)
        
        title = QtWidgets.QLabel("Результаты")
        title.setStyleSheet("font-size: 26px; font-weight: 700; color: #1E293B;")
        layout.addWidget(title)
        
        self.results_list = QtWidgets.QListWidget()
        self.results_list.setStyleSheet(self.list_style())
        self.results_list.itemClicked.connect(self.open_doc)
        layout.addWidget(self.results_list)
        return page

    def page_view(self):
        self.reader_form = TextReaderForm()
        self.reader_form.btn_delete.clicked.connect(self.delete_current_doc)
        self.reader_form.similar_list.itemClicked.connect(self.open_similar_doc)
        self.reader_form.setStyleSheet("background: transparent;")
        return self.reader_form

    def page_all_docs(self):
        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(50, 40, 50, 40)
        
        title = QtWidgets.QLabel("Библиотека")
        title.setStyleSheet("font-size: 26px; font-weight: 700; color: #1E293B;")
        layout.addWidget(title)
        
        self.all_docs_search = QtWidgets.QLineEdit()
        self.all_docs_search.setPlaceholderText("Фильтрация по названию...")
        self.all_docs_search.setStyleSheet(self.input_style())
        self.all_docs_search.textChanged.connect(self.filter_docs)
        layout.addWidget(self.all_docs_search)
        
        self.all_docs_list = QtWidgets.QListWidget()
        self.all_docs_list.setStyleSheet(self.list_style())
        self.all_docs_list.itemClicked.connect(self.open_doc)
        layout.addWidget(self.all_docs_list)
        return page

    def page_add_doc(self):
        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(50, 40, 50, 40)
        layout.setSpacing(15)
        
        title = QtWidgets.QLabel("Новый документ")
        title.setStyleSheet("font-size: 26px; font-weight: 700; color: #1E293B;")
        layout.addWidget(title)
        
        self.add_title = QtWidgets.QLineEdit()
        self.add_title.setPlaceholderText("Название документа")
        self.add_title.setStyleSheet(self.input_style())
        layout.addWidget(self.add_title)
        
        self.add_content = QtWidgets.QTextEdit()
        self.add_content.setPlaceholderText("Содержимое текста...")
        self.add_content.setStyleSheet(self.text_edit_style())
        layout.addWidget(self.add_content)
        
        btn_import = self.create_button("Импорт из файла", self.import_text_from_file, "secondary")
        layout.addWidget(btn_import)
        
        btn_save = self.create_button("Сохранить", self.save_new_doc)
        layout.addWidget(btn_save)
        return page

    def page_history(self):
        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(50, 40, 50, 40)
        
        title = QtWidgets.QLabel("История")
        title.setStyleSheet("font-size: 26px; font-weight: 700; color: #1E293B;")
        layout.addWidget(title)
        
        self.history_list = QtWidgets.QListWidget()
        self.history_list.setStyleSheet(self.list_style())
        self.history_list.itemClicked.connect(self.repeat_search)
        layout.addWidget(self.history_list)
        
        btn_clear = self.create_button("Очистить историю", self.clear_history, "secondary")
        layout.addWidget(btn_clear)
        return page


    def go_to(self, idx, add=True):
        current_page = self.history[self.current_idx] if self.history else None
        if add and idx != current_page:
            self.history = self.history[:self.current_idx + 1] + [idx]
            self.current_idx = len(self.history) - 1
        
        self.stack.setCurrentIndex(idx)
        self.btn_back.setEnabled(self.current_idx > 0)
        
        if idx == self.pages.get("all_docs"):
            try:
                self.documents = Document.get_all()
            except Exception:
                self.documents = []
            self.filter_docs("")
        elif idx == self.pages.get("history"):
            self.history_list.clear()
            try:
                for q in self.engine.history.get_all():
                    self.history_list.addItem(q)
            except Exception:
                pass
        elif idx == self.pages.get("home"):
            self.update_recommendations()

    def go_back(self):
        if self.current_idx > 0:
            self.current_idx -= 1
            idx = self.history[self.current_idx]
            self.stack.setCurrentIndex(idx)
            if hasattr(self, "doc_history"):
                self.doc_history = []
        self.btn_back.setEnabled(self.current_idx > 0)

    def update_recommendations(self):
        self.recommend_list.clear()
        try:
            recs = self.recommender.get_document_recommendations(top_n=5)
            for rec in recs:
                name = rec.name if hasattr(rec, "name") else str(rec)
                item = QtWidgets.QListWidgetItem(name)
                item.setData(Qt.UserRole, name)
                self.recommend_list.addItem(item)
        except Exception:
            pass

    def do_search(self):
        query = self.search_input.text().strip()
        
        if not query:
            QMessageBox.warning(self, "Внимание", "Введите поисковой запрос.")
            return
        
        filter_text = self.filter_input.text().strip()
        if filter_text:
            import re
            if re.search(r'[^а-яёa-z,\s]', filter_text, re.IGNORECASE):
                QMessageBox.warning(self, "Ошибка", "Фильтр может содержать только буквы и запятые.")
                return
            filters = [f.strip() for f in filter_text.split(',') if f.strip()]
        else:
            filters = None
        
        try:
            results = self.engine.search(query, filters)
            self.recommender.learn_from_results(results)
            self.results_list.clear()
            
            items = results.items if hasattr(results, "items") else (results or [])
            if not items:
                QMessageBox.information(self, "Результаты", "Документы не найдены.")
                return
            
            print(f"[DEBUG] Результаты поиска: запрос='{query}', найдено={len(items)}")
            for doc, score in items:
                try:
                    print(f"[DEBUG] точность={score:.4f} документ='{doc.name}'")
                except Exception:
                    pass
                item = QtWidgets.QListWidgetItem(doc.name)
                item.setData(Qt.UserRole, doc.name)
                self.results_list.addItem(item)
            
            self.go_to(self.pages["results"])
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при поиске: {str(e)}")

    def open_doc(self, item, add_to_history=True):
        doc_id = item.data(Qt.UserRole)
        try:
            self.documents = Document.get_all()
        except Exception:
            pass
        doc = next((d for d in self.documents if d.name == doc_id or getattr(d, 'id', None) == doc_id), None)
        if not doc:
            return
        
        self.current_doc_id = doc.name
        self.reader_form.set_document(doc)
        
        try:
            self.reader_form.similar_list.clear()
            similar = self.engine.get_similar_documents(doc.name)
            sim_items = similar.items if hasattr(similar, "items") else (similar or [])
            for d, sc in sim_items:
                it = QtWidgets.QListWidgetItem(d.name)
                it.setData(Qt.UserRole, d.name)
                self.reader_form.similar_list.addItem(it)
        except Exception as e:
            print(f"Ошибка при загрузке похожих документов: {e}")
        
        self.go_to(self.pages["view"], add=True)

    def open_similar_doc(self, item):
        self.open_doc(item, add_to_history=True)

    def delete_current_doc(self):
        if not self.current_doc_id:
            return
        
        reply = QMessageBox.question(
            self,
            "Подтверждение",
            f"Вы уверены, что хотите удалить документ '{self.current_doc_id}'?",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                Document.delete_document(self.current_doc_id)
                self.documents = Document.get_all()
                QMessageBox.information(self, "Успех", "Документ удален.")
                self.go_to(self.pages["all_docs"])
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось удалить документ: {str(e)}")

    def save_new_doc(self):
        name = self.add_title.text().strip()
        text = self.add_content.toPlainText().strip()
        
        if not name:
            QMessageBox.warning(self, "Ошибка", "Введите название документа.")
            return
        
        if not text:
            QMessageBox.warning(self, "Ошибка", "Введите текст документа.")
            return
        
        import re
        invalid_chars = r'[<>:"/\\|?*]'
        if re.search(invalid_chars, name):
            QMessageBox.warning(self, "Ошибка", "Название содержит недопустимые символы: < > : \" / \\ | ? *")
            return
        
        try:
            Document.create_new(name, text)
            self.documents = Document.get_all()
            self.add_title.clear()
            self.add_content.clear()
            QMessageBox.information(self, "Успех", "Документ успешно создан.")
            self.go_to(self.pages["home"])
        except FileExistsError:
            QMessageBox.warning(self, "Ошибка", f"Документ с именем '{name}' уже существует.")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось создать документ: {str(e)}")

    def import_text_from_file(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "Выберите текстовый файл",
            "",
            "Текстовые файлы (*.txt)"
        )
        if not path:
            return
        try:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    content = f.read()
            except UnicodeDecodeError:
                with open(path, 'r', encoding='cp1251') as f:
                    content = f.read()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось прочитать файл: {str(e)}")
            return
        self.add_content.setPlainText(content)
        if not self.add_title.text().strip():
            base = os.path.basename(path)
            if base.lower().endswith(".txt"):
                base = base[:-4]
            self.add_title.setText(base)

    def repeat_search(self, item):
        self.search_input.setText(item.text())
        self.do_search()

    def clear_history(self):
        reply = QMessageBox.question(
            self,
            "Подтверждение",
            "Вы уверены, что хотите очистить всю историю запросов?",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                self.engine.history.clear()
                self.history_list.clear()
                QMessageBox.information(self, "Успех", "История очищена.")
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось очистить историю: {str(e)}")

    def filter_docs(self, text):
        self.all_docs_list.clear()
        try:
            self.documents = Document.get_all()
        except Exception:
            self.documents = self.documents if hasattr(self, "documents") else []
        for d in self.documents:
            if text.lower() in d.name.lower():
                item = QtWidgets.QListWidgetItem(d.name)
                item.setData(Qt.UserRole, d.name)
                self.all_docs_list.addItem(item)

    def input_style(self):
        return "QLineEdit { background: white; border: 2px solid #E2E8F0; border-radius: 8px; padding: 0 12px; font-size: 14px; color: #334155; } QLineEdit:focus { border-color: #6C5CE7; }"

    def text_edit_style(self):
        return "QTextEdit { background: white; border: 2px solid #E2E8F0; border-radius: 8px; padding: 10px; font-size: 14px; color: #334155; } QTextEdit:focus { border-color: #6C5CE7; }"

    def list_style(self):
        return "QListWidget { background: white; border: 1px solid #E2E8F0; border-radius: 8px; outline: none; } QListWidget::item { padding: 12px; border-bottom: 1px solid #F1F5F9; color: #334155; } QListWidget::item:selected { background: #EEF2FF; color: #6C5CE7; border-left: 4px solid #6C5CE7; }"

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    app.setStyleSheet("""
        /* Базовые настройки шрифта и фона для всех окон */
        QWidget {
            font-family: -apple-system, 'SF Pro Text', 'Helvetica Neue', Helvetica, Arial, 'Roboto', 'Ubuntu', 'Cantarell', 'Noto Sans', 'DejaVu Sans', sans-serif;
            font-size: 14px;
        }
        
        /* Стили специально для QMessageBox (всплывающих окон) */
        QMessageBox {
            background-color: #FFFFFF;
            border: 1px solid #E2E8F0;
        }
        
        /* Текст внутри сообщений */
        QMessageBox QLabel {
            color: #334155;
            background-color: transparent;
            font-weight: 500;
        }
        
        /* Кнопки внутри сообщений (Yes, No, OK) */
        QMessageBox QPushButton {
            background-color: #F1F5F9;
            color: #334155;
            border: 1px solid #CBD5E1;
            border-radius: 6px;
            padding: 6px 15px;
            min-width: 65px;
            font-weight: 600;
        }
        
        QMessageBox QPushButton:hover {
            background-color: #6C5CE7;
            color: #FFFFFF;
            border-color: #6C5CE7;
        }
        
        QMessageBox QPushButton:pressed {
            background-color: #5b4cc4;
        }
    """)
    
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
