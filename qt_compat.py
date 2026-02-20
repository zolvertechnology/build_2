"""
PyQt5 / PyQt6 compatibility shim.
Import everything Qt-related from here so the rest of the code is version-agnostic.
"""

PYQT: int = 0

try:
    from PyQt6.QtCore import (
        Qt, QThread, pyqtSignal, QUrl, QObject, QTimer,
        QAbstractTableModel, QModelIndex,
    )
    from PyQt6.QtGui import QFont, QAction, QIcon
    from PyQt6.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QGridLayout, QLabel, QPushButton, QComboBox, QLineEdit, QTextEdit,
        QFileDialog, QMessageBox, QTableWidget, QTableWidgetItem,
        QHeaderView, QTabWidget, QGroupBox, QSpinBox, QDoubleSpinBox,
        QCheckBox, QProgressBar, QSplitter, QFrame, QSizePolicy,
        QStatusBar, QDialog, QTextBrowser, QAbstractItemView, QTableView,
        QScrollBar,
    )
    PYQT = 6

    _AlignCenter      = Qt.AlignmentFlag.AlignCenter
    _AlignRight       = Qt.AlignmentFlag.AlignRight
    _AlignVCenter     = Qt.AlignmentFlag.AlignVCenter
    _Vertical         = Qt.Orientation.Vertical
    _Horizontal       = Qt.Orientation.Horizontal
    _Stretch          = QHeaderView.ResizeMode.Stretch
    _NoEdit           = QTableWidget.EditTrigger.NoEditTriggers
    _Warning          = QMessageBox.Icon.Warning
    _Ok               = QMessageBox.StandardButton.Ok
    _Yes              = QMessageBox.StandardButton.Yes
    _No               = QMessageBox.StandardButton.No
    _RichText         = Qt.TextFormat.RichText
    _TextBrowser      = Qt.TextInteractionFlag.TextBrowserInteraction
    _DisplayRole      = Qt.ItemDataRole.DisplayRole
    _MultiSelection   = QAbstractItemView.SelectionMode.MultiSelection
    _ResetRole        = QMessageBox.ButtonRole.ResetRole

    try:
        from PyQt6.QtWebEngineWidgets import QWebEngineView
        HAS_WEBENGINE = True
    except ImportError:
        HAS_WEBENGINE = False

except ImportError:
    from PyQt5.QtCore import (
        Qt, QThread, pyqtSignal, QUrl, QObject, QTimer,
        QAbstractTableModel, QModelIndex,
    )
    from PyQt5.QtGui import QFont, QIcon
    from PyQt5.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QGridLayout, QLabel, QPushButton, QComboBox, QLineEdit, QTextEdit,
        QFileDialog, QMessageBox, QTableWidget, QTableWidgetItem,
        QHeaderView, QTabWidget, QGroupBox, QSpinBox, QDoubleSpinBox,
        QCheckBox, QProgressBar, QSplitter, QFrame, QSizePolicy,
        QAction, QStatusBar, QDialog, QTextBrowser, QAbstractItemView,
        QTableView, QScrollBar,
    )
    PYQT = 5

    _AlignCenter      = Qt.AlignCenter
    _AlignRight       = Qt.AlignRight
    _AlignVCenter     = Qt.AlignVCenter
    _Vertical         = Qt.Vertical
    _Horizontal       = Qt.Horizontal
    _Stretch          = QHeaderView.Stretch
    _NoEdit           = QTableWidget.NoEditTriggers
    _Warning          = QMessageBox.Warning
    _Ok               = QMessageBox.Ok
    _Yes              = QMessageBox.Yes
    _No               = QMessageBox.No
    _RichText         = Qt.RichText
    _TextBrowser      = Qt.TextBrowserInteraction
    _DisplayRole      = Qt.DisplayRole
    _MultiSelection   = QAbstractItemView.MultiSelection
    _ResetRole        = QMessageBox.ResetRole

    try:
        from PyQt5.QtWebEngineWidgets import QWebEngineView
        HAS_WEBENGINE = True
    except ImportError:
        HAS_WEBENGINE = False


# ── Optional: matplotlib ──────────────────────────────────────────────────────
HAS_MATPLOTLIB = False
FigureCanvas = None
Figure = None

try:
    import matplotlib
    matplotlib.use("QtAgg" if PYQT == 6 else "Qt5Agg")
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas  # type: ignore
    from matplotlib.figure import Figure
    HAS_MATPLOTLIB = True
except Exception:
    try:
        import matplotlib
        matplotlib.use("Qt5Agg")
        from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas  # type: ignore
        from matplotlib.figure import Figure
        HAS_MATPLOTLIB = True
    except Exception:
        pass


def exec_app(app):
    """Cross-version QApplication.exec()."""
    return app.exec() if PYQT == 6 else app.exec_()


def exec_dialog(dlg):
    """Cross-version QDialog.exec()."""
    return dlg.exec() if PYQT == 6 else dlg.exec_()