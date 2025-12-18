from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QMessageBox

class TextReaderForm(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.document = None
        self.is_edit_mode = False
        self.title = None
        self.text = None
        self.btn_edit = None
        self.btn_delete = None
        self.similar_list = None
        self.keywords_label = None
        self.init_ui()

    def init_ui(self):
        self.scroll = QtWidgets.QScrollArea(self)
        self.scroll.setWidgetResizable(True)
        self.scroll.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.scroll.setStyleSheet("QScrollArea { background: transparent; border: none; }")

        container = QtWidgets.QWidget()
        container.setStyleSheet("background: transparent;")
        self.scroll.setWidget(container)

        layout = QtWidgets.QVBoxLayout(container)
        layout.setContentsMargins(40, 20, 40, 40)
        layout.setSpacing(20)

        self.title = QtWidgets.QLabel("Загрузка...")
        self.title.setStyleSheet("font-size: 24px; color: #1E293B; font-weight: 700; margin-bottom: 10px;")
        self.title.setWordWrap(True)
        layout.addWidget(self.title)

        self.text = QtWidgets.QTextEdit()
        self.text.setReadOnly(True)
        self.text.setMinimumHeight(400)
        self.text.setStyleSheet(
            """
            QTextEdit {
                background: #FFFFFF;
                border: 1px solid #E2E8F0;
                border-radius: 10px;
                padding: 20px;
                font-size: 15px;
                color: #334155;
            }
            QTextEdit:focus {
                border: 1px solid #6C5CE7;
            }
            """
        )
        layout.addWidget(self.text)

        row = QtWidgets.QHBoxLayout()
        row.setSpacing(15)

        self.btn_edit = QtWidgets.QPushButton("Редактировать")
        self.btn_edit.setMinimumHeight(45)
        self.btn_edit.setCursor(Qt.PointingHandCursor)
        self.btn_edit.setStyleSheet(self.get_btn_style("default"))
        row.addWidget(self.btn_edit)

        self.btn_delete = QtWidgets.QPushButton("Удалить")
        self.btn_delete.setMinimumHeight(45)
        self.btn_delete.setCursor(Qt.PointingHandCursor)
        self.btn_delete.setStyleSheet(self.get_btn_style("danger"))
        row.addWidget(self.btn_delete)

        row.addStretch()
        layout.addLayout(row)

        layout.addSpacing(10)

        kw_title = QtWidgets.QLabel("Ключевые слова:")
        kw_title.setStyleSheet("font-size: 15px; color: #475569; font-weight: 600;")
        layout.addWidget(kw_title)

        self.keywords_label = QtWidgets.QLabel("—")
        self.keywords_label.setWordWrap(True)
        self.keywords_label.setStyleSheet(
            """
            QLabel {
                background: #FFFFFF;
                border: 1px solid #E2E8F0;
                border-radius: 10px;
                padding: 12px 16px;
                color: #334155;
            }
            """
        )
        layout.addWidget(self.keywords_label)

        layout.addSpacing(10)

        lbl = QtWidgets.QLabel("Похожие документы:")
        lbl.setStyleSheet("font-size: 15px; color: #475569; font-weight: 600;")
        layout.addWidget(lbl)

        self.similar_list = QtWidgets.QListWidget()
        self.similar_list.setMinimumHeight(150)
        self.similar_list.setMaximumHeight(250)
        self.similar_list.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.similar_list.setStyleSheet(
            """
            QListWidget {
                background: #FFFFFF;
                border: 1px solid #E2E8F0;
                border-radius: 10px;
                outline: none;
            }
            QListWidget::item {
                padding: 12px 16px;
                border-bottom: 1px solid #F1F5F9;
                color: #334155;
            }
            QListWidget::item:hover {
                background: #F8FAFC;
            }
            QListWidget::item:selected {
                background: #EEF2FF;
                color: #6C5CE7;
            }
            """
        )
        layout.addWidget(self.similar_list)

        main_layout = QtWidgets.QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.addWidget(self.scroll)

    def set_document(self, doc):
        self.document = doc
        if self.title:
            self.title.setText(doc.name)
        if self.text:
            try:
                text_content = doc.get_text()
                self.text.setPlainText(text_content)
                self.text.setReadOnly(True)
                self.update_keywords(text_content)
            except Exception:
                self.text.setPlainText("")
                self.update_keywords("")
        self.is_edit_mode = False
        if self.btn_edit:
            self.btn_edit.setText("Редактировать")
            self.btn_edit.setStyleSheet(self.get_btn_style("default"))
            try:
                self.btn_edit.clicked.disconnect()
            except TypeError:
                pass
            self.btn_edit.clicked.connect(self.toggle_edit)

    def toggle_edit(self):
        self.is_edit_mode = not self.is_edit_mode
        self.text.setReadOnly(not self.is_edit_mode)
        if self.is_edit_mode:
            self.btn_edit.setText("Сохранить")
            self.btn_edit.setStyleSheet(self.get_btn_style("primary"))
        else:
            self.btn_edit.setText("Редактировать")
            self.btn_edit.setStyleSheet(self.get_btn_style("default"))
        try:
            self.btn_edit.clicked.disconnect()
        except TypeError:
            pass
        self.btn_edit.clicked.connect(self.save if self.is_edit_mode else self.toggle_edit)

    def save(self):
        if not self.document:
            return
        txt = self.text.toPlainText().strip()
        if not txt:
            QMessageBox.warning(self, "Ошибка", "Текст не может быть пустым.")
            return
        try:
            from backend.core.document_manager import Document
            doc_id = getattr(self.document, 'id', self.document.name)
            Document.update_text(doc_id, txt)
            QMessageBox.information(self, "Готово", "Документ сохранён.")
            self.update_keywords(txt)
            self.toggle_edit()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить: {str(e)}")

    def update_keywords(self, text: str):
        if not self.keywords_label:
            return
        try:
            from backend.core.index import Index
            index = Index()
            kws = index.extract_keywords(text or "", top_n=10)
            self.keywords_label.setText(", ".join(kws) if kws else "—")
        except Exception:
            self.keywords_label.setText("—")

    def get_btn_style(self, variant="default"):
        base = "QPushButton { border: none; border-radius: 8px; font-size: 14px; font-weight: 600; padding: 0 24px; }"
        if variant == "primary":
            return base + " QPushButton { background: #6C5CE7; color: white; } QPushButton:hover { background: #5b4cc4; }"
        elif variant == "danger":
            return base + " QPushButton { background: #EF4444; color: white; } QPushButton:hover { background: #DC2626; }"
        else:
            return base + " QPushButton { background: #F1F5F9; color: #475569; } QPushButton:hover { background: #E2E8F0; color: #1E293B; }"
