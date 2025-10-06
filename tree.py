import sys
import os
import json
import shutil
import csv
from pathlib import Path
from collections import deque, defaultdict

from PySide6.QtWidgets import (
    QApplication, QWidget, QHBoxLayout, QVBoxLayout, QPushButton, QLineEdit, QLabel,
    QFileDialog, QTreeView, QMenu, QMessageBox, QColorDialog, QDialog, QListWidget,
    QListWidgetItem, QCheckBox, QProgressBar, QInputDialog, QAbstractItemView
)
from PySide6.QtGui import QDesktopServices, QColor, QKeySequence, QAction, QPalette, QPainter
from PySide6.QtCore import QUrl, Qt, QThread, Signal, Slot, QCoreApplication, QTimer
from PySide6.QtWidgets import QFileSystemModel, QStyledItemDelegate
from PySide6.QtWidgets import QRubberBand
from PySide6.QtCore import QRect, QPoint, QSize, QItemSelectionModel

class RubberBandTreeView(QTreeView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.rubberBand = QRubberBand(QRubberBand.Rectangle, self.viewport())
        self.origin = QPoint()
        self._dragging = False

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.origin = event.position().toPoint()
            self.rubberBand.setGeometry(QRect(self.origin, QSize()))
            self.rubberBand.show()
            self._dragging = True
            # clear existing selection when starting a new box
            self.selectionModel().clearSelection()
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self._dragging and (event.buttons() & Qt.LeftButton):
            rect = QRect(self.origin, event.position().toPoint()).normalized()
            self.rubberBand.setGeometry(rect)

            # Select all visible rows intersecting the rubber band
            selection = QItemSelectionModel.Select | QItemSelectionModel.Rows
            for y in range(rect.top(), rect.bottom(), 16):  # step through visible rows
                idx = self.indexAt(QPoint(rect.left(), y))
                if idx.isValid():
                    self.selectionModel().select(idx, selection)

        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.rubberBand.hide()
            self._dragging = False
            self.origin = QPoint()
        super().mouseReleaseEvent(event)


APP_SETTINGS = "settings.json"


# --- Application Icon ---
from PySide6.QtGui import QIcon
APP_ICON = os.path.join(os.path.dirname(__file__), "app_icon.ico")


# ---------- Helpers ----------
def load_settings():
    defaults = {
        "highlight_map": {},
        "precount_for_progress": False,
        "compute_folder_sizes": False
    }
    p = Path(APP_SETTINGS)
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            defaults.update(data)
        except Exception:
            pass
    return defaults


def save_settings(s):
    try:
        Path(APP_SETTINGS).write_text(json.dumps(s, indent=2), encoding="utf-8")
    except Exception as e:
        print("Failed saving settings:", e)


def format_size_decimal(nbytes: int) -> str:
    """Format bytes using decimal units: B, kB, MB, GB, TB (1000 base)."""
    if nbytes is None:
        return ""
    try:
        n = float(nbytes)
    except Exception:
        return ""
    units = ["B", "kB", "MB", "GB", "TB"]
    i = 0
    while n >= 1000 and i < len(units) - 1:
        n /= 1000.0
        i += 1
    if i == 0:
        return f"{int(n)} {units[i]}"
    else:
        return f"{n:.1f} {units[i]}"


# ---------- Highlight delegate ----------
class HighlightDelegate(QStyledItemDelegate):
    def __init__(self, model, settings):
        super().__init__()
        self.model = model
        self.settings = settings

    def paint(self, painter: QPainter, option, index):
        if index.column() == 0:
            try:
                path = self.model.filePath(index)
                _, ext = os.path.splitext(path.lower())
                color_hex = self.settings.get("highlight_map", {}).get(ext)
                if color_hex:
                    opt = option
                    palette = opt.palette
                    palette.setColor(QPalette.Text, QColor(color_hex))
                    opt.palette = palette
                    super().paint(painter, opt, index)
                    return
            except Exception:
                pass
        super().paint(painter, option, index)


# ---------- Size delegate ----------
class SizeDelegate(QStyledItemDelegate):
    def __init__(self, model, folder_sizes_dict):
        super().__init__()
        self.model = model
        self.folder_sizes = folder_sizes_dict

    def paint(self, painter: QPainter, option, index):
        # Only customize column 1 (Size)
        if index.column() == 1:
            try:
                path = os.path.normcase(os.path.normpath(self.model.filePath(index)))
                size = None
                try:
                    is_dir = self.model.isDir(index)
                except Exception:
                    is_dir = os.path.isdir(path)

                if is_dir:
                    # compare normalized keys
                    for key, val in self.folder_sizes.items():
                        if os.path.normcase(os.path.normpath(key)) == path:
                            size = val
                            break
                else:
                    try:
                        size = os.path.getsize(path)
                    except Exception:
                        size = None

                text = format_size_decimal(size) if size is not None else ""
                painter.save()
                rect = option.rect.adjusted(4, 0, -4, 0)
                painter.drawText(rect, Qt.AlignVCenter | Qt.AlignLeft, text)
                painter.restore()
                return
            except Exception:
                pass
        super().paint(painter, option, index)

class SearchWorker(QThread):
    progress = Signal(int, str)        # emit progress updates
    found = Signal(str)                # emit each found file/folder path
    done = Signal(list)                # emit final results list

    def __init__(self, root_path, needle):
        super().__init__()
        self.root_path = root_path
        self.needle = needle.lower()
        self.results = []
        self._running = True

    def stop(self):
        self._running = False

    def run(self):
        count = 0
        try:
            for r, dirs, files in os.walk(self.root_path):
                if not self._running:
                    break
                for name in dirs + files:
                    if not self._running:
                        break
                    count += 1
                    if self.needle in name.lower():
                        full = os.path.join(r, name)
                        self.results.append(full)
                        self.found.emit(full)
                    if count % 500 == 0:
                        self.progress.emit(count, r)
                        self.msleep(1)
        except Exception as e:
            print("SearchWorker error:", e)
        self.done.emit(self.results)

# ---------- Expand All Worker ----------
class ExpandAllThread(QThread):
    # emits batches (list) and done
    progress = Signal(list)
    done = Signal()

    def __init__(self, root_path, batch_size=1000):
        super().__init__()
        self.root_path = root_path
        self._running = True
        self.batch_size = batch_size

    def run(self):
        # walk tree and emit batches of folder paths (absolute)
        batch = []
        try:
            for root, dirs, _ in os.walk(self.root_path):
                if not self._running:
                    break
                for d in dirs:
                    if not self._running:
                        break
                    batch.append(os.path.join(root, d))
                    if len(batch) >= self.batch_size:
                        # emit a batch and clear
                        try:
                            self.progress.emit(batch)
                        except Exception:
                            pass
                        batch = []
                        self.msleep(10)
            if batch:
                try:
                    self.progress.emit(batch)
                except Exception:
                    pass
        except Exception as e:
            print("ExpandAllThread error:", e)
        finally:
            self.done.emit()

    def stop(self):
        self._running = False


# ---------- Folder Size Worker ----------
class FolderSizeWorker(QThread):
    progress = Signal(int, int, str)      # fine: counts
    folder_done = Signal(str, object)     # changed from int -> object
    done = Signal()

    def __init__(self, root_path, precount=False):
        super().__init__()
        self.root_path = root_path
        self._running = True
        self.precount = precount

    def run(self):
        folders = []
        file_sizes_per_folder = {}
        try:
            for dirpath, dirs, files in os.walk(self.root_path):
                if not self._running:
                    return
                try:
                    folders.append(dirpath)
                    s = 0
                    for fname in files:
                        if not self._running:
                            return
                        try:
                            s += os.path.getsize(os.path.join(dirpath, fname))
                        except Exception:
                            pass
                    file_sizes_per_folder[dirpath] = s
                except Exception:
                    pass
        except Exception:
            pass

        total = len(folders)
        folders_sorted = sorted(folders, key=lambda p: p.count(os.sep), reverse=True)
        totals = dict(file_sizes_per_folder)

        try:
            for i, d in enumerate(folders_sorted):
                if not self._running:
                    return
                try:
                    parent = os.path.dirname(d)
                    if parent in totals:
                        totals[parent] += totals.get(d, 0)
                    self.folder_done.emit(d, totals.get(d, 0))
                    self.progress.emit(i + 1, total, d)
                except Exception:
                    pass
                self.msleep(1)
        finally:
            self.done.emit()

    def stop(self):
        self._running = False


# ---------- Main Window ----------
class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Folder Tree Viewer")
        self.resize(1200, 800)
        self.settings = load_settings()

        self.clipboard_files = []
        self.clipboard_mode = None

        # folder sizes map (absolute path -> bytes)
        self.folder_sizes = {}
        self.current_root_path = ""

        # threads
        self.expand_thread = None
        self.size_thread = None

        # queue for expansion processing (main-thread)
        self._expand_queue = deque()
        self._expand_attempts = defaultdict(int)
        self._expanding = False

        self._build_ui()
        self._connect_signals()

    def _build_ui(self):
        v = QVBoxLayout(self)

        toolbar = QHBoxLayout()
        self.path_edit = QLineEdit()
        self.browse_btn = QPushButton("Browse")
        self.settings_btn = QPushButton("Settings")
        self.export_btn = QPushButton("Export Visible -> CSV/TXT")
        toolbar.addWidget(QLabel("Folder:"))
        toolbar.addWidget(self.path_edit)
        toolbar.addWidget(self.browse_btn)
        toolbar.addWidget(self.settings_btn)
        toolbar.addWidget(self.export_btn)
        v.addLayout(toolbar)

        search_row = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.find_next_btn = QPushButton("Find ↓")
        self.find_prev_btn = QPushButton("Find ↑")
        self.expand_all_btn = QPushButton("Expand All")
        self.collapse_all_btn = QPushButton("Collapse All")
        self.match_label = QLabel("")
        search_row.addWidget(QLabel("Search:"))
        search_row.addWidget(self.search_edit)
        search_row.addWidget(self.find_next_btn)
        search_row.addWidget(self.find_prev_btn)
        search_row.addWidget(self.expand_all_btn)
        search_row.addWidget(self.collapse_all_btn)
        search_row.addWidget(self.match_label)
        v.addLayout(search_row)

        self.model = QFileSystemModel()
        self.model.setReadOnly(False)
        self.tree = RubberBandTreeView()
        palette = self.tree.palette()
        palette.setColor(QPalette.Highlight, QColor("#3874f2"))      # bright blue selection
        palette.setColor(QPalette.HighlightedText, QColor("#ffffff"))  # white text on selection
        self.tree.setPalette(palette)
        self.tree.setModel(self.model)
        self.tree.setAlternatingRowColors(True)
        self.tree.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tree.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tree.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.tree.setDragEnabled(False)  # disable drag-move behavior
        self.tree.setAcceptDrops(False)
        self.tree.setDragDropMode(QAbstractItemView.NoDragDrop)
        self.tree.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.tree.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tree.setUniformRowHeights(True)
        self.tree.setAnimated(False)
        self.tree.setAllColumnsShowFocus(False)
        self.tree.setContextMenuPolicy(Qt.CustomContextMenu)

        # delegates
        self.tree.setItemDelegateForColumn(0, HighlightDelegate(self.model, self.settings))
        self.tree.setItemDelegateForColumn(1, SizeDelegate(self.model, self.folder_sizes))

        for i, w in enumerate([400, 110, 160, 200]):
            try:
                self.tree.setColumnWidth(i, w)
            except Exception:
                pass

        v.addWidget(self.tree)

        st = QHBoxLayout()
        self.status_label = QLabel("Idle")
        self.progress = QProgressBar()
        self.progress.setMinimum(0)
        self.progress.setMaximum(100)
        self.progress.setValue(0)
        st.addWidget(self.status_label)
        st.addStretch()
        st.addWidget(self.progress)
        v.addLayout(st)

    def _connect_signals(self):
        self.browse_btn.clicked.connect(self.on_browse)
        self.settings_btn.clicked.connect(self.on_settings)
        self.export_btn.clicked.connect(self.on_export)
        self.expand_all_btn.clicked.connect(self.expand_all_async)
        self.collapse_all_btn.clicked.connect(self.collapse_all)
        self.tree.customContextMenuRequested.connect(self.on_context_menu)
        self.find_next_btn.clicked.connect(self.find_next)
        self.find_prev_btn.clicked.connect(self.find_prev)
        self.search_edit.returnPressed.connect(self.find_next)

        copy_short = QAction(self)
        copy_short.setShortcut(QKeySequence.Copy)
        copy_short.triggered.connect(self.copy_selected)
        self.addAction(copy_short)

        cut_short = QAction(self)
        cut_short.setShortcut(QKeySequence.Cut)
        cut_short.triggered.connect(self.cut_selected)
        self.addAction(cut_short)

        paste_short = QAction(self)
        paste_short.setShortcut(QKeySequence.Paste)
        paste_short.triggered.connect(self.paste_to_current)
        self.addAction(paste_short)

        del_short = QAction(self)
        del_short.setShortcut(QKeySequence.Delete)
        del_short.triggered.connect(self.delete_selected)
        self.addAction(del_short)

        self.tree.doubleClicked.connect(self.open_index)
        # repaint when expanding to show sizes for precomputed folders
        self.tree.expanded.connect(lambda: self.tree.viewport().update())

        self.show_computer_view()

    def show_computer_view(self):
        """Show all drives at startup (like 'This PC')."""
        try:
            import psutil
            drives = [p.device for p in psutil.disk_partitions(all=False)]
        except Exception:
            # fallback if psutil missing
            from string import ascii_uppercase
            drives = [f"{d}:\\\\" for d in ascii_uppercase if os.path.exists(f"{d}:\\")]
        self.model.setRootPath("")  # clear
        self.tree.setRootIndex(self.model.index(""))
        self.path_edit.setText("This PC")
        self.status_label.setText("Showing available drives")
        self.matches = []
        self.match_pos = -1
        self.match_label.setText("")
        try:
            if hasattr(self, "search_thread") and self.search_thread and self.search_thread.isRunning():
                self.search_thread.stop()
                self.search_thread.wait(200)
        except Exception:
            pass
        # populate pseudo-items
        for d in drives:
            idx = self.model.index(d)
            if idx.isValid():
                self.tree.expand(idx)


    # ---------- actions ----------
    def on_browse(self):
        p = QFileDialog.getExistingDirectory(self, "Select folder or drive root")
        if not p:
            return
        self.path_edit.setText(p)
        try:
            self.model.setRootPath(p)
            self.tree.setRootIndex(self.model.index(p))
        except Exception:
            try:
                self.tree.setRootIndex(self.model.index(p))
            except Exception:
                pass
        self.status_label.setText(f"Loaded: {p}")
        self.matches = []
        self.match_pos = -1
        self.match_label.setText("")
        try:
            if hasattr(self, "search_thread") and self.search_thread and self.search_thread.isRunning():
                self.search_thread.stop()
                self.search_thread.wait(200)
        except Exception:
            pass
        if self.settings.get("compute_folder_sizes"):
            if os.path.isdir(p):
                self.start_folder_size_scan(p)

    def on_settings(self):
        root_idx = self.tree.rootIndex()
        dlg = SettingsDialog(self.settings, parent=self, root_index_valid=root_idx.isValid())
        if dlg.exec() == QDialog.Accepted:
            self.settings = dlg.get_settings()
            save_settings(self.settings)
            self.tree.setItemDelegateForColumn(0, HighlightDelegate(self.model, self.settings))
            if self.settings.get("compute_folder_sizes") and root_idx.isValid():
                root_path = self.model.filePath(root_idx)
                if os.path.isdir(root_path):
                    self.start_folder_size_scan(root_path)

    def on_export(self):
        root_index = self.tree.rootIndex()
        if not root_index.isValid():
            QMessageBox.information(self, "No root", "Please select a folder first.")
            return
        file, _ = QFileDialog.getSaveFileName(self, "Export visible to CSV", filter="CSV Files (*.csv);;Text Files (*.txt)")
        if not file:
            return
        rows = []

        def walk(idx):
            try:
                fi = self.model.filePath(idx)
                rows.append((fi, self.model.fileName(idx)))
                if self.tree.isExpanded(idx) or idx == root_index:
                    r = self.model.rowCount(idx)
                    for i in range(r):
                        child = self.model.index(i, 0, idx)
                        walk(child)
            except Exception:
                pass

        walk(root_index)
        try:
            if file.lower().endswith('.csv'):
                with open(file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['path', 'name'])
                    writer.writerows(rows)
            else:
                with open(file, 'w', encoding='utf-8') as f:
                    for p, n in rows:
                        f.write(f"{p}\n")
            QMessageBox.information(self, "Exported", f"Exported {len(rows)} rows to {file}")
        except Exception as e:
            QMessageBox.critical(self, "Export failed", str(e))

    # ---------- context menu ----------
    def on_context_menu(self, point):
        ix = self.tree.indexAt(point)
        menu = QMenu(self)

        if not ix.isValid():
            # right-click empty space
            if self.clipboard_files:
                menu.addAction("Paste", lambda: self.paste_to_path(os.path.expanduser("~")))
            else:
                menu.addAction("Refresh", self.show_computer_view)
            menu.exec(self.tree.viewport().mapToGlobal(point))
            return

        path = self.model.filePath(ix)
        menu = QMenu(self)
        menu.addAction("Open", lambda: QDesktopServices.openUrl(QUrl.fromLocalFile(path)))
        menu.addAction("Open location", lambda: self.open_location(path))
        menu.addAction("Copy", self.copy_selected)
        menu.addAction("Cut", self.cut_selected)
        menu.addAction("Paste", lambda: self.paste_to_path(path))
        menu.addAction("Rename", lambda: self.rename_item(ix, path))
        menu.addAction("Copy path", lambda: QApplication.clipboard().setText(path))
        menu.addAction("Delete", self.delete_selected)
        menu.exec(self.tree.viewport().mapToGlobal(point))

    def open_location(self, path):
        if os.path.isdir(path):
            QDesktopServices.openUrl(QUrl.fromLocalFile(path))
        else:
            folder = os.path.dirname(path)
            if os.path.exists(path):
                try:
                    os.startfile(folder)
                except Exception:
                    QDesktopServices.openUrl(QUrl.fromLocalFile(folder))

    def rename_item(self, index, path):
        old = os.path.basename(path)
        new, ok = QInputDialog.getText(self, "Rename", "New name:", text=old)
        if not ok or not new or new == old:
            return
        newpath = os.path.join(os.path.dirname(path), new)
        try:
            os.rename(path, newpath)
            # refresh model by resetting root
            root = self.tree.rootIndex()
            rootpath = self.model.filePath(root)
            self.model.setRootPath("")
            self.model.setRootPath(rootpath)
            self.tree.setRootIndex(self.model.index(rootpath))
            if self.settings.get("compute_folder_sizes"):
                if os.path.isdir(rootpath):
                    self.start_folder_size_scan(rootpath)
        except Exception as e:
            QMessageBox.critical(self, "Rename failed", str(e))

    def delete_selected(self):
        sels = self.tree.selectionModel().selectedRows()
        if not sels:
            return
        paths = [self.model.filePath(ix) for ix in sels]
        ans = QMessageBox.question(self, "Delete", f"Delete {len(paths)} items? This cannot be undone.")
        if ans != QMessageBox.Yes:
            return
        errors = []
        for p in paths:
            try:
                if os.path.isdir(p):
                    shutil.rmtree(p)
                else:
                    os.remove(p)
            except Exception as e:
                errors.append(str(e))
        if errors:
            QMessageBox.critical(self, "Delete errors", "\n".join(errors))
        root = self.tree.rootIndex()
        rootpath = self.model.filePath(root)
        self.model.setRootPath("")
        self.model.setRootPath(rootpath)
        self.tree.setRootIndex(self.model.index(rootpath))
        if self.settings.get("compute_folder_sizes"):
            if os.path.isdir(rootpath):
                self.start_folder_size_scan(rootpath)

    # ---------- copy / cut / paste ----------
    def copy_selected(self):
        sels = self.tree.selectionModel().selectedRows()
        if not sels:
            return
        self.clipboard_files = [self.model.filePath(ix) for ix in sels]
        self.clipboard_mode = 'copy'
        QApplication.clipboard().setText('\n'.join(self.clipboard_files))
        self.status_label.setText(f"Copied {len(self.clipboard_files)} items")

    def cut_selected(self):
        sels = self.tree.selectionModel().selectedRows()
        if not sels:
            return
        self.clipboard_files = [self.model.filePath(ix) for ix in sels]
        self.clipboard_mode = 'cut'
        QApplication.clipboard().setText('\n'.join(self.clipboard_files))
        self.status_label.setText(f"Cut {len(self.clipboard_files)} items")

    def paste_to_current(self):
        sels = self.tree.selectionModel().selectedRows()
        dest = None
        if sels:
            ix = sels[0]
            p = self.model.filePath(ix)
            if os.path.isdir(p):
                dest = p
            else:
                dest = os.path.dirname(p)
        else:
            root = self.tree.rootIndex()
            if root.isValid():
                dest = self.model.filePath(root)
            else:
                dest = os.path.expanduser("~")
        if not dest:
            return
        self.paste_to_path(dest)

    def paste_to_path(self, dest_path):
        if not self.clipboard_files:
            return
        errors = []
        for src in self.clipboard_files:
            if not os.path.exists(src):
                errors.append(f"Source missing: {src}")
                continue
            name = os.path.basename(src)
            target = os.path.join(dest_path, name)
            if os.path.exists(target):
                choice = self.handle_conflict(target)
                if choice == 'skip':
                    continue
                elif choice == 'suffix':
                    target = self.make_suffix_name(dest_path, name)
                elif choice == 'rename':
                    new = QInputDialog.getText(self, "Rename on paste", "New name:", text=f"copy_{name}")
                    if not new[1]:
                        continue
                    target = os.path.join(dest_path, new[0])
                elif choice == 'replace':
                    try:
                        if os.path.isdir(target):
                            shutil.rmtree(target)
                        else:
                            os.remove(target)
                    except Exception as e:
                        errors.append(str(e))
                        continue
                else:
                    continue
            try:
                if os.path.isdir(src):
                    if os.path.exists(target):
                        target = self.make_suffix_name(dest_path, name)
                    if self.clipboard_mode == 'cut':
                        shutil.move(src, target)
                    else:
                        shutil.copytree(src, target)
                else:
                    if self.clipboard_mode == 'cut':
                        shutil.move(src, target)
                    else:
                        shutil.copy2(src, target)
            except Exception as e:
                errors.append(str(e))
        if self.clipboard_mode == 'cut':
            self.clipboard_files = []
            self.clipboard_mode = None
            QApplication.clipboard().clear()
        root = self.tree.rootIndex()
        rootpath = self.model.filePath(root)
        self.model.setRootPath("")
        self.model.setRootPath(rootpath)
        self.tree.setRootIndex(self.model.index(rootpath))
        if self.settings.get("compute_folder_sizes"):
            if os.path.isdir(rootpath):
                self.start_folder_size_scan(rootpath)
        if errors:
            QMessageBox.critical(self, "Paste errors", "\n".join(errors))

    def handle_conflict(self, existing_path):
        name = os.path.basename(existing_path)
        msg = QMessageBox(self)
        msg.setWindowTitle("Conflict")
        msg.setText(f"Target already exists: {name}\nWhat do you want to do?")
        suffix = msg.addButton("Add suffix (file_1)", QMessageBox.AcceptRole)
        rename = msg.addButton("Rename", QMessageBox.AcceptRole)
        replace = msg.addButton("Replace", QMessageBox.DestructiveRole)
        skip = msg.addButton("Skip", QMessageBox.RejectRole)
        msg.exec()
        clicked = msg.clickedButton()
        if clicked == suffix:
            return 'suffix'
        if clicked == rename:
            return 'rename'
        if clicked == replace:
            return 'replace'
        return 'skip'

    def make_suffix_name(self, dest_path, name):
        base, ext = os.path.splitext(name)
        i = 1
        while True:
            new = f"{base}_{i}{ext}"
            target = os.path.join(dest_path, new)
            if not os.path.exists(target):
                return target
            i += 1

    # ---------- open / rename helpers ----------
    def open_index(self, index):
        p = self.model.filePath(index)
        try:
            QDesktopServices.openUrl(QUrl.fromLocalFile(p))
        except Exception as e:
            QMessageBox.critical(self, "Open failed", str(e))

    # ---------- search ----------
    def find_all_indexes(self, needle):
        matches = []
        root = self.tree.rootIndex()
        if not root.isValid():
            return matches
        root_path = self.model.filePath(root)

        try:
            for r, dirs, files in os.walk(root_path):
                for name in dirs + files:
                    if needle.lower() in name.lower():
                        full = os.path.join(r, name)
                        try:
                            # Ensure all parents of this path are loaded into the model
                            parts = os.path.normpath(full).split(os.sep)
                            for i in range(1, len(parts)):
                                subpath = os.sep.join(parts[:i])
                                if os.path.isdir(subpath):
                                    self.model.index(subpath)
                            idx = self.model.index(full)
                            if idx.isValid():
                                matches.append(idx)
                        except Exception:
                            pass
        except Exception:
            pass

        return matches


    def expand_to_index(self, idx):
        ancestors = []
        parent = idx.parent()
        root = self.tree.rootIndex()
        while parent.isValid() and parent != root:
            ancestors.append(parent)
            parent = parent.parent()
        for a in reversed(ancestors):
            try:
                self.tree.expand(a)
                QCoreApplication.processEvents()  # yield for model to load children
            except Exception:
                pass
        if idx.parent().isValid():
            try:
                self.tree.expand(idx.parent())
                QCoreApplication.processEvents()
            except Exception:
                pass


    def find_next(self):
        needle = self.search_edit.text().strip()
        if not needle:
            return

        # if we already have matches from a previous search
        if hasattr(self, "matches") and self.matches:
            self.match_pos = (self.match_pos + 1) % len(self.matches)
            idx = self.matches[self.match_pos]
            self.expand_to_index(idx)
            self.tree.scrollTo(idx)
            self.tree.setCurrentIndex(idx)
            self.status_label.setText(f"Showing {self.match_pos + 1}/{len(self.matches)} matches")
            return

        # --- otherwise start a new threaded search ---
        root_idx = self.tree.rootIndex()
        if not root_idx.isValid():
            QMessageBox.warning(self, "No root", "Please select a folder to search in.")
            return

        root_path = self.model.filePath(root_idx)
        if not os.path.isdir(root_path):
            return

        # Stop old search if running
        try:
            if hasattr(self, "search_thread") and self.search_thread.isRunning():
                self.search_thread.stop()
                self.search_thread.wait(100)
        except Exception:
            pass

        self.status_label.setText("Searching...")
        self.progress.setMaximum(0)
        self.progress.setValue(0)
        self.match_label.setText("")
        self.matches = []
        self.match_pos = -1

        self.search_thread = SearchWorker(root_path, needle)
        self.search_thread.progress.connect(
            lambda c, p: self.status_label.setText(f"Searching... checked {c} items in {os.path.basename(p) or p}")
        )
        self.search_thread.found.connect(self._on_search_found)
        self.search_thread.done.connect(self._on_search_done)
        self.search_thread.start()

    def find_prev(self):
        needle = self.search_edit.text().strip()
        if not needle:
            return
        if not hasattr(self, 'matches') or not self.matches:
            self.matches = self.find_all_indexes(needle)
            self.match_pos = 0
        if not self.matches:
            return
        self.match_pos = (self.match_pos - 1) % len(self.matches)
        idx = self.matches[self.match_pos]
        self.expand_to_index(idx)
        self.tree.scrollTo(idx)
        self.tree.setCurrentIndex(idx)


    @Slot(str)
    def _on_search_found(self, path):
        try:
            idx = self.model.index(path)
            if idx.isValid():
                self.matches.append(idx)
                self.match_label.setText(f"{len(self.matches)} matches")
        except Exception:
            pass

    @Slot(list)
    def _on_search_done(self, results):
        self.progress.setMaximum(100)
        self.progress.setValue(100)
        self.status_label.setText(f"Search complete — {len(results)} results.")
        if results:
            self.matches = [self.model.index(r) for r in results if self.model.index(r).isValid()]
            self.match_pos = 0
            first_idx = self.matches[0]
            self.expand_to_index(first_idx)
            self.tree.scrollTo(first_idx)
            self.tree.setCurrentIndex(first_idx)


    # ---------- Expand All ----------
    def expand_all_async(self):
        root_idx = self.tree.rootIndex()
        if not root_idx.isValid():
            QMessageBox.warning(self, "No root", "Please select a folder first.")
            return
        root_path = self.model.filePath(root_idx)
        if not os.path.isdir(root_path):
            QMessageBox.warning(self, "Invalid root", "The selected root path is invalid.")
            return

        # If currently expanding, toggle stop
        if self._expanding:
            # stop requested
            self.status_label.setText("Stopping expansion...")
            if self.expand_thread and self.expand_thread.isRunning():
                self.expand_thread.stop()
            # clear queue
            self._expand_queue.clear()
            self._expand_attempts.clear()
            self._expanding = False
            self.expand_all_btn.setText("Expand All")
            self.expand_all_btn.setEnabled(True)
            self.collapse_all_btn.setEnabled(True)
            return

        # start expansion
        self._expand_queue.clear()
        self._expand_attempts.clear()
        self._expanding = True
        self.expand_all_btn.setText("Stop")
        self.expand_all_btn.setEnabled(True)
        self.collapse_all_btn.setEnabled(False)
        self.status_label.setText("Preparing expansion...")
        self.progress.setMaximum(0)

        # thread will emit batches of paths
        self.expand_thread = ExpandAllThread(root_path, batch_size=1000)
        self.expand_thread.progress.connect(self.on_expand_batch_received)
        self.expand_thread.done.connect(self.on_expand_done_thread)
        self.expand_thread.start()

        # start processing loop (timer-driven)
        QTimer.singleShot(50, self._process_expand_queue_step)

    @Slot(list)
    def on_expand_batch_received(self, batch):
        # extend the process queue with new incoming paths
        for p in batch:
            self._expand_queue.append(p)
        # update status
        self.status_label.setText(f"Queued folders: {len(self._expand_queue)}")
        QCoreApplication.processEvents()

    def _process_expand_queue_step(self):
        """
        Process a small chunk of queued folders on the GUI thread, then re-schedule itself.
        This yields to the event loop to keep UI responsive.
        """
        if not self._expanding:
            return  # expansion stopped
        # process up to N items per tick
        MAX_PER_TICK = 40
        processed = 0
        requeue = []
        while self._expand_queue and processed < MAX_PER_TICK:
            folder = self._expand_queue.popleft()
            processed += 1
            try:
                idx = self.model.index(folder)
                if idx.isValid():
                    # valid index -> expand it
                    self.tree.expand(idx)
                    # reset attempts if it was previously retried
                    if folder in self._expand_attempts:
                        del self._expand_attempts[folder]
                else:
                    # not ready yet; requeue for later (but limit attempts)
                    self._expand_attempts[folder] += 1
                    if self._expand_attempts[folder] <= 8:
                        requeue.append(folder)
                    # else skip after too many attempts
            except Exception:
                pass

        # re-add requeued items to the end
        for item in requeue:
            self._expand_queue.append(item)

        # update status
        qlen = len(self._expand_queue)
        self.status_label.setText(f"Expanding... queue {qlen} (processed {processed})")
        QCoreApplication.processEvents()

        # if still items pending or thread still producing, schedule next tick
        if self._expand_queue or (self.expand_thread and self.expand_thread.isRunning()):
            QTimer.singleShot(50, self._process_expand_queue_step)
        else:
            # nothing pending and thread finished -> finalize
            self._finalize_expansion()

    def _finalize_expansion(self):
        # mark finished and restore UI
        self._expanding = False
        self.status_label.setText("Expand All complete.")
        self.progress.setMaximum(100)
        self.progress.setValue(0)
        self.expand_all_btn.setText("Expand All")
        self.expand_all_btn.setEnabled(True)
        self.collapse_all_btn.setEnabled(True)
        # final repaint to ensure everything is visible
        self.tree.viewport().update()

    def on_expand_done_thread(self):
        # thread finished producing lists; we rely on the queue processor to finish remaining items
        # update status
        self.status_label.setText(f"Expansion batches produced — finishing queue ({len(self._expand_queue)} left)")
        QCoreApplication.processEvents()

    @Slot()
    def on_expand_done(self):
        # kept for compatibility (unused)
        pass

    # ---------- Collapse All ----------
    def collapse_all(self):
        try:
            self.tree.collapseAll()
            self.status_label.setText("Collapsed all")
            QCoreApplication.processEvents()
        except Exception:
            pass

    # ---------- Folder size scanning ----------
    def start_folder_size_scan(self, root_path):
        if not root_path or not os.path.isdir(root_path):
            self.status_label.setText("Cannot compute sizes: invalid root")
            return

        try:
            if self.size_thread and self.size_thread.isRunning():
                self.size_thread.stop()
                self.size_thread.wait(500)
        except Exception:
            pass

        self.folder_sizes.clear()
        self.tree.viewport().update()
        precount = self.settings.get("precount_for_progress", False)
        self.status_label.setText("Preparing folder size calculation...")
        self.progress.setMaximum(0 if not precount else 100)
        self.progress.setValue(0)

        try:
            self.size_thread = FolderSizeWorker(root_path, precount=precount)
            self.size_thread.folder_done.connect(self.on_folder_size_done)
            self.size_thread.progress.connect(self.on_size_progress)
            self.size_thread.done.connect(self.on_size_done)
            self.size_thread.start()
        except Exception as e:
            print("start_folder_size_scan error:", e)
            self.status_label.setText("Failed to start size calculation")

    @Slot(str, object)   # accept object instead of int
    def on_folder_size_done(self, folder_path, size_bytes):
        try:
            self.folder_sizes[folder_path] = int(size_bytes)  # ensure stored as Python int
            self.tree.viewport().update()
        except Exception as e:
            print("on_folder_size_done error:", e)

    @Slot(int, int, str)
    def on_size_progress(self, current, total, folder):
        try:
            if total and total > 0:
                pct = int((current / total) * 100)
                self.progress.setMaximum(100)
                self.progress.setValue(pct)
                self.status_label.setText(f"Calculating folder sizes... {current}/{total}: {os.path.basename(folder) or folder}")
            else:
                self.progress.setMaximum(0)
                self.status_label.setText("Calculating folder sizes...")
            QCoreApplication.processEvents()
        except Exception as e:
            print("on_size_progress error:", e)

    @Slot()
    def on_size_done(self):
        try:
            self.status_label.setText("Folder size calculation complete — refreshing view")
            self.progress.setMaximum(100)
            self.progress.setValue(0)
            self.tree.viewport().update()
        except Exception as e:
            print("on_size_done error:", e)

    # ---------- cleanup ----------
    def closeEvent(self, event):
        try:
            if self.expand_thread and self.expand_thread.isRunning():
                self.expand_thread.stop()
                self.expand_thread.wait(2000)
        except Exception:
            pass
        try:
            if self.size_thread and self.size_thread.isRunning():
                self.size_thread.stop()
                self.size_thread.wait(2000)
        except Exception:
            pass
        super().closeEvent(event)


# ---------- Settings Dialog ----------
class SettingsDialog(QDialog):
    def __init__(self, settings, parent=None, root_index_valid=False):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.settings = dict(settings)
        self.root_index_valid = root_index_valid
        self._build()

    def _build(self):
        v = QVBoxLayout(self)
        self.highlight_list = QListWidget()

        if self.root_index_valid and self.parent() and hasattr(self.parent(), "model"):
            try:
                root_idx = self.parent().tree.rootIndex()
                if root_idx.isValid():
                    root_path = self.parent().model.filePath(root_idx)
                    exts = set()
                    try:
                        for root, dirs, files in os.walk(root_path):
                            for f in files:
                                try:
                                    _, e = os.path.splitext(f.lower())
                                    if e:
                                        exts.add(e)
                                except Exception:
                                    pass
                    except Exception:
                        exts = set()
                    for e in sorted(exts):
                        color = self.settings.get("highlight_map", {}).get(e, "#FFFFFF")
                        it = QListWidgetItem(f"{e} -> {color}")
                        it.setData(Qt.UserRole, (e, color))
                        self.highlight_list.addItem(it)
                else:
                    msg = QListWidgetItem("No folder selected in main window — open a folder to auto-detect extensions")
                    msg.setFlags(Qt.NoItemFlags)
                    self.highlight_list.addItem(msg)
            except Exception:
                msg = QListWidgetItem("Could not scan folder for extensions")
                msg.setFlags(Qt.NoItemFlags)
                self.highlight_list.addItem(msg)
        else:
            msg = QListWidgetItem("No folder selected — open a folder to auto-detect extensions")
            msg.setFlags(Qt.NoItemFlags)
            self.highlight_list.addItem(msg)

        v.addWidget(QLabel("Highlight extensions (select then pick Color)"))
        v.addWidget(self.highlight_list)

        btn_row = QHBoxLayout()
        self.add_ext_btn = QPushButton("Add")
        self.color_btn = QPushButton("Color")
        self.remove_btn = QPushButton("Remove")
        btn_row.addWidget(self.add_ext_btn); btn_row.addWidget(self.color_btn); btn_row.addWidget(self.remove_btn)
        v.addLayout(btn_row)

        self.precount_cb = QCheckBox("Pre-count for progress (slow)")
        self.precount_cb.setChecked(self.settings.get('precount_for_progress', False))
        v.addWidget(self.precount_cb)
        self.compute_size_cb = QCheckBox("Compute folder sizes (slow)")
        self.compute_size_cb.setChecked(self.settings.get('compute_folder_sizes', False))
        v.addWidget(self.compute_size_cb)

        ok_row = QHBoxLayout()
        self.ok_btn = QPushButton("Save")
        self.cancel_btn = QPushButton("Cancel")
        ok_row.addStretch(); ok_row.addWidget(self.ok_btn); ok_row.addWidget(self.cancel_btn)
        v.addLayout(ok_row)

        self.add_ext_btn.clicked.connect(self.on_add_ext)
        self.color_btn.clicked.connect(self.on_color)
        self.remove_btn.clicked.connect(self.on_remove)
        self.ok_btn.clicked.connect(self.accept)
        self.cancel_btn.clicked.connect(self.reject)

    def on_add_ext(self):
        ext, ok = QInputDialog.getText(self, "Add extension", "Extension (e.g. .json):")
        if not ok or not ext:
            return
        ext = ext.strip().lower()
        if not ext.startswith('.'):
            ext = '.' + ext
        col = QColorDialog.getColor()
        if not col.isValid():
            return
        it = QListWidgetItem(f"{ext} -> {col.name()}")
        it.setData(Qt.UserRole, (ext, col.name()))
        self.highlight_list.addItem(it)

    def on_color(self):
        sel = self.highlight_list.currentItem()
        if not sel:
            return
        try:
            ext, current = sel.data(Qt.UserRole)
        except Exception:
            return
        col = QColorDialog.getColor()
        if not col.isValid():
            return
        sel.setText(f"{ext} -> {col.name()}")
        sel.setData(Qt.UserRole, (ext, col.name()))

    def on_remove(self):
        sel = self.highlight_list.currentRow()
        if sel >= 0:
            self.highlight_list.takeItem(sel)

    def get_settings(self):
        hl = {}
        for i in range(self.highlight_list.count()):
            try:
                ext, col = self.highlight_list.item(i).data(Qt.UserRole)
                if ext:
                    hl[ext] = col
            except Exception:
                pass
        self.settings['highlight_map'] = hl
        self.settings['precount_for_progress'] = self.precount_cb.isChecked()
        self.settings['compute_folder_sizes'] = self.compute_size_cb.isChecked()
        return self.settings


# ---------- run ----------
def main():
    app = QApplication(sys.argv)
    if os.path.exists(APP_ICON):
        app.setWindowIcon(QIcon(APP_ICON))
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
