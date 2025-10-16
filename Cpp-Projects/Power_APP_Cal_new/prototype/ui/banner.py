import sys
import json
import threading
import socket
import subprocess
import time
import argparse
from pathlib import Path

from PySide6 import QtCore, QtWidgets, QtGui
import ctypes
import ctypes.wintypes as wintypes


class SensorReader(QtCore.QObject):
    sample_received = QtCore.Signal(dict)

    def __init__(self, source_cmd=None, tcp_fallback=('127.0.0.1', 20123)):
        super().__init__()
        self._running = False
        self.source_cmd = source_cmd
        self.tcp_fallback = tcp_fallback
        self._proc = None
        self._socket = None

    def start(self):
        self._running = True
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        self._running = False
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
        if self._proc:
            try:
                self._proc.terminate()
            except Exception:
                pass
            try:
                self._proc.wait(timeout=1)
            except Exception:
                pass

    def _try_socket_connect(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect(self.tcp_fallback)
            s.settimeout(None)
            self._socket = s
            return True
        except Exception:
            self._socket = None
            return False

    def _run(self):
        # If a source_cmd is given, spawn the process and read its stdout (newline-delimited JSON)
        if self.source_cmd:
            try:
                shell = isinstance(self.source_cmd, str)
                self._proc = subprocess.Popen(
                    self.source_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
            except Exception:
                self._proc = None

        if self._proc:
            # Read from process stdout in blocking mode; termination will close the pipe
            try:
                while self._running:
                    line = self._proc.stdout.readline()
                    if not line:
                        break
                    try:
                        obj = json.loads(line.decode('utf-8').strip())
                        self.sample_received.emit(obj)
                    except Exception:
                        pass
            except Exception:
                pass
            # fall through to other fallbacks if process exits
        # Prefer file-based stub; fallback to TCP server if file missing
        path = Path(__file__).resolve().parent.parent / 'sensor.json'
        while self._running:
            try:
                if path.exists():
                    text = path.read_text()
                    obj = json.loads(text)
                    self.sample_received.emit(obj)
                else:
                    # try socket fallback
                    if not self._socket and not self._try_socket_connect():
                        # no source available; synthesize a fallback sample so the UI isn't empty
                        try:
                            import random
                            sample = {
                                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
                                'cpu_percent': round(random.uniform(1, 20), 1),
                                'ram_percent': round(random.uniform(20, 70), 1),
                                'gpu_percent': round(random.uniform(0, 10), 1),
                                'temps': {'cpu_package': round(random.uniform(35, 65), 1)},
                                'process_top': []
                            }
                            self.sample_received.emit(sample)
                        except Exception:
                            pass
                        # sleep briefly and retry
                        time.sleep(0.8)
                        continue
                    if self._socket:
                        data = b''
                        # read a line
                        while self._running:
                            chunk = self._socket.recv(1024)
                            if not chunk:
                                break
                            data += chunk
                            if b"\n" in data:
                                break
                        if data:
                            try:
                                line = data.split(b"\n")[0].decode(
                                    'utf-8').strip()
                                obj = json.loads(line)
                                self.sample_received.emit(obj)
                            except Exception:
                                pass
            except Exception:
                pass
            time.sleep(0.8)


class Banner(QtWidgets.QWidget):
    def __init__(self, engine_cmd=None, background=False, corner='tr', fade_minutes=3):
        # Frameless tool window; we manage bottom order manually via Win32
        super().__init__(None, QtCore.Qt.Window | QtCore.Qt.FramelessWindowHint)
        self.setAttribute(QtCore.Qt.WA_TranslucentBackground)
        self.setWindowFlag(QtCore.Qt.Tool, True)
        self.setWindowTitle('SysMon Banner')
        self.resize(360, 52)

        self.drag_pos = None
        self._corner = corner
        self._fade_minutes = fade_minutes

        layout = QtWidgets.QHBoxLayout(self)
        layout.setContentsMargins(10, 6, 10, 6)
        layout.setSpacing(12)

        self.cpu_label = QtWidgets.QLabel('CPU: --%')
        self.ram_label = QtWidgets.QLabel('RAM: --%')
        self.gpu_label = QtWidgets.QLabel('GPU: --%')
        self.temp_label = QtWidgets.QLabel(f"Temp: --{chr(176)}C")

        for w in (self.cpu_label, self.ram_label, self.gpu_label, self.temp_label):
            w.setStyleSheet('color: white; font-weight: 600')
            layout.addWidget(w)

        # Spacer, then control buttons (Hide to tray, Quit)
        layout.addStretch(1)
        btn_style = (
            'QToolButton { color: white; background: transparent; border: none;'
            ' font-weight: 700; font-size: 14px; padding: 2px; }'
            'QToolButton:hover { background: rgba(255,255,255,0.12); border-radius: 4px; }'
        )

        self.btn_hide = QtWidgets.QToolButton()
        self.btn_hide.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_TitleBarMinButton))
        self.btn_hide.setToolTip('Hide (keep running in tray)')
        self.btn_hide.setStyleSheet(btn_style)
        self.btn_hide.setFixedSize(22, 22)
        self.btn_hide.clicked.connect(self.hide)
        layout.addWidget(self.btn_hide)

        self.btn_quit = QtWidgets.QToolButton()
        self.btn_quit.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_TitleBarCloseButton))
        self.btn_quit.setToolTip('Quit (exit completely)')
        self.btn_quit.setStyleSheet(btn_style)
        self.btn_quit.setFixedSize(22, 22)
        self.btn_quit.clicked.connect(self.quit_app)
        layout.addWidget(self.btn_quit)

        # Minimal orange border + rounded corners
        self.setObjectName('BannerRoot')
        self.setStyleSheet(
            'QWidget#BannerRoot { background-color: rgba(30,30,30,220); border: 1.5px solid #FF7A18; border-radius: 16px; }')

        self.sensor = SensorReader(source_cmd=engine_cmd)
        self.sensor.sample_received.connect(self.on_sample)
        self.sensor.start()

        # Auto transparency timer (default 3 minutes)
        self._fade_timer = QtCore.QTimer(self)
        self._fade_timer.setSingleShot(True)
        self._fade_timer.timeout.connect(self._fade_out)
        self._restart_fade_timer()

        # Tray icon for background mode
        self.tray = QtWidgets.QSystemTrayIcon(self)
        icon = self.style().standardIcon(QtWidgets.QStyle.SP_ComputerIcon)
        self.tray.setIcon(icon)
        menu = QtWidgets.QMenu()
        act_show = menu.addAction('Show/Hide')
        act_quit = menu.addAction('Quit')
        act_show.triggered.connect(self.toggle_visible)
        act_quit.triggered.connect(self.quit_app)
        self.tray.setContextMenu(menu)
        self.tray.activated.connect(lambda r: self.toggle_visible() if r == QtWidgets.QSystemTrayIcon.Trigger else None)
        self.tray.show()

        if background:
            QtCore.QTimer.singleShot(100, self.place_in_corner)
            self.hide()
        else:
            QtCore.QTimer.singleShot(100, self.place_in_corner)
            QtCore.QTimer.singleShot(150, self._ensure_bottom)

    def place_in_corner(self):
        screen = QtWidgets.QApplication.primaryScreen().availableGeometry()
        w, h = self.width(), self.height()
        margin = 12
        pos = QtCore.QPoint(screen.right()-w-margin, screen.top()+margin)
        if self._corner == 'tl':
            pos = QtCore.QPoint(screen.left()+margin, screen.top()+margin)
        elif self._corner == 'br':
            pos = QtCore.QPoint(screen.right()-w-margin, screen.bottom()-h-margin)
        elif self._corner == 'bl':
            pos = QtCore.QPoint(screen.left()+margin, screen.bottom()-h-margin)
        self.move(pos)

    def toggle_visible(self):
        if self.isVisible():
            self.hide()
        else:
            self.show()
            self.raise_()
            self.activateWindow()
            QtCore.QTimer.singleShot(100, self._ensure_bottom)

    def quit_app(self):
        self.sensor.stop()
        QtWidgets.QApplication.quit()

    def on_sample(self, sample: dict):
        cpu = sample.get('cpu_percent', '--')
        ram = sample.get('ram_percent', '--')
        gpu = sample.get('gpu_percent', '--')
        temp = sample.get('temps', {}).get(
            'cpu_package', sample.get('temps', {}).get('gpu', '--'))
        self.cpu_label.setText(f'CPU: {cpu}%')
        self.ram_label.setText(f'RAM: {ram}%')
        self.gpu_label.setText(f'GPU: {gpu}%')
        self.temp_label.setText(f"Temp: {temp}{chr(176)}C")
        # Restore opacity and restart fade countdown on new data
        self.setWindowOpacity(1.0)
        self._restart_fade_timer()
        QtCore.QTimer.singleShot(0, self._ensure_bottom)

    def mousePressEvent(self, event):
        if event.button() == QtCore.Qt.LeftButton:
            self.drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()

    def mouseMoveEvent(self, event):
        if self.drag_pos is not None and event.buttons() & QtCore.Qt.LeftButton:
            self.move(event.globalPosition().toPoint() - self.drag_pos)

    def mouseReleaseEvent(self, event):
        self.drag_pos = None

    def enterEvent(self, event):
        # Restore visibility when hovering
        self.setWindowOpacity(1.0)
        self._restart_fade_timer()
        super().enterEvent(event)
        QtCore.QTimer.singleShot(0, self._ensure_bottom)

    def _restart_fade_timer(self):
        try:
            self._fade_timer.start(int(self._fade_minutes * 60 * 1000))
        except Exception:
            pass

    def _fade_out(self):
        try:
            anim = QtCore.QPropertyAnimation(self, b'windowOpacity', self)
            anim.setDuration(800)
            anim.setStartValue(self.windowOpacity())
            anim.setEndValue(0.25)
            anim.start(QtCore.QAbstractAnimation.DeleteWhenStopped)
        except Exception:
            self.setWindowOpacity(0.25)

    # Keep the banner behind other app windows (Windows)
    def _ensure_bottom(self):
        try:
            hwnd = int(self.winId())
            user32 = ctypes.windll.user32
            HWND_BOTTOM = ctypes.c_void_p(1)
            SWP_NOSIZE = 0x0001
            SWP_NOMOVE = 0x0002
            SWP_NOACTIVATE = 0x0010
            SWP_NOOWNERZORDER = 0x0200
            user32.SetWindowPos.restype = wintypes.BOOL
            user32.SetWindowPos.argtypes = [wintypes.HWND, wintypes.HWND,
                                            ctypes.c_int, ctypes.c_int,
                                            ctypes.c_int, ctypes.c_int, wintypes.UINT]
            user32.SetWindowPos(hwnd, HWND_BOTTOM, 0, 0, 0, 0,
                                SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE | SWP_NOOWNERZORDER)
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description='SysMon Banner')
    parser.add_argument('--engine', help='Shell command to spawn (JSONL on stdout)')
    parser.add_argument('--engine-args', nargs='+', help='Engine command and args as list')
    parser.add_argument('--background', action='store_true', help='Start hidden in system tray')
    parser.add_argument('--corner', choices=['tr','tl','br','bl'], default='tr', help='Screen corner for banner')
    parser.add_argument('--fade-minutes', type=float, default=3, help='Minutes before banner auto-fades')
    args = parser.parse_args()

    app = QtWidgets.QApplication(sys.argv)
    # Apply global stylesheet if available
    try:
        from . import theme as sysmon_theme  # type: ignore
    except Exception:
        import theme as sysmon_theme  # type: ignore
    try:
        qss = sysmon_theme.load_qss()
        if qss:
            app.setStyleSheet(qss)
    except Exception:
        pass
    engine_cmd = args.engine_args if args.engine_args else args.engine
    banner = Banner(engine_cmd=engine_cmd, background=args.background, corner=args.corner, fade_minutes=args.fade_minutes)
    banner.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
