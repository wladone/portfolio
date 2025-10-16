import sys
import argparse
import json
from collections import deque
from pathlib import Path

from PySide6 import QtCore, QtWidgets, QtGui
try:
    from PySide6.QtCharts import QChart, QChartView, QLineSeries, QValueAxis  # type: ignore
except Exception:
    QChart = QChartView = QLineSeries = QValueAxis = None  # type: ignore

try:
    import psutil  # type: ignore
except Exception:
    psutil = None

# Reuse the SensorReader and theme helpers
try:
    from .banner import SensorReader  # type: ignore
    from . import theme as sysmon_theme  # type: ignore
except Exception:
    # Fallback when executed as a script (not as a package)
    import os
    import sys as _sys
    _sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from banner import SensorReader  # type: ignore
    import theme as sysmon_theme  # type: ignore


class DesktopWindow(QtWidgets.QMainWindow):
    def __init__(self, engine_cmd=None, background=False):
        super().__init__()
        self.setWindowTitle('SysMon Desktop')
        self.resize(960, 540)

        # Central stats panel
        central = QtWidgets.QWidget()
        grid = QtWidgets.QGridLayout(central)
        grid.setContentsMargins(12, 12, 12, 12)
        grid.setHorizontalSpacing(16)
        grid.setVerticalSpacing(10)

        self.cpu = QtWidgets.QLabel('CPU: --%')
        self.ram = QtWidgets.QLabel('RAM: --%')
        self.gpu = QtWidgets.QLabel('GPU: --%')
        self.temp = QtWidgets.QLabel(f"Temp: --{chr(176)}C")
        self.status = QtWidgets.QLabel('Status: waiting for data...')

        for w in (self.cpu, self.ram, self.gpu, self.temp, self.status):
            w.setStyleSheet('font-size: 18px;')

        grid.addWidget(self.cpu, 0, 0)
        grid.addWidget(self.ram, 0, 1)
        grid.addWidget(self.gpu, 1, 0)
        grid.addWidget(self.temp, 1, 1)
        grid.addWidget(self.status, 2, 0, 1, 2)

        # Right side: latest JSON payload for debugging/extra info
        self.details = QtWidgets.QTextEdit()
        self.details.setReadOnly(True)
        self.details.setMinimumWidth(320)

        container = QtWidgets.QWidget()
        h = QtWidgets.QHBoxLayout(container)
        h.setContentsMargins(0, 0, 0, 0)
        h.addWidget(central, 1)
        # Middle panel: charts (if available)
        self.chart_view = None
        if QChart is not None:
            self.cpu_series = QLineSeries(name='CPU')
            self.ram_series = QLineSeries(name='RAM')
            chart = QChart()
            chart.addSeries(self.cpu_series)
            chart.addSeries(self.ram_series)
            self.axis_x = QValueAxis()
            self.axis_y = QValueAxis()
            self.axis_x.setRange(0, 59)
            self.axis_x.setTitleText('Samples')
            self.axis_y.setRange(0, 100)
            self.axis_y.setTitleText('%')
            chart.addAxis(self.axis_x, QtCore.Qt.AlignBottom)
            chart.addAxis(self.axis_y, QtCore.Qt.AlignLeft)
            self.cpu_series.attachAxis(self.axis_x)
            self.cpu_series.attachAxis(self.axis_y)
            self.ram_series.attachAxis(self.axis_x)
            self.ram_series.attachAxis(self.axis_y)
            # Apply theme to chart
            try:
                theme = sysmon_theme.load_theme_json()
                bg = QtGui.QColor(theme['base']['surface'])
                grid_c = QtGui.QColor('#222832')
                txt = QtGui.QColor(theme['base'].get('text', '#F5F7FA'))
                chart.setBackgroundBrush(QtGui.QBrush(bg))
                cpu_pen = QtGui.QPen(QtGui.QColor(theme['series']['cpu']))
                cpu_pen.setWidthF(2.5)
                cpu_pen.setCapStyle(QtCore.Qt.RoundCap)
                cpu_pen.setJoinStyle(QtCore.Qt.RoundJoin)
                self.cpu_series.setPen(cpu_pen)
                ram_pen = QtGui.QPen(QtGui.QColor(theme['series']['ram']))
                ram_pen.setWidthF(2.5)
                ram_pen.setCapStyle(QtCore.Qt.RoundCap)
                ram_pen.setJoinStyle(QtCore.Qt.RoundJoin)
                self.ram_series.setPen(ram_pen)
                grid_pen = QtGui.QPen(grid_c)
                self.axis_x.setGridLinePen(grid_pen)
                self.axis_y.setGridLinePen(grid_pen)
                self.axis_x.setLabelsColor(txt)
                self.axis_y.setLabelsColor(txt)
                chart.legend().setLabelColor(txt)
            except Exception:
                pass
            self.chart_view = QChartView(chart)
            self.chart_view.setMinimumWidth(380)
            h.addWidget(self.chart_view, 1)
        h.addWidget(self.details, 1)
        self.setCentralWidget(container)

        # History for charts
        self.hist_cpu = deque(maxlen=60)
        self.hist_ram = deque(maxlen=60)

        # Bottom: top processes table
        self.table = QtWidgets.QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(['PID', 'Name', 'CPU%', 'RAM%'])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea, self._wrap_in_dock('Top Processes', self.table))

        # Sensor reader
        self.sensor = SensorReader(source_cmd=engine_cmd)
        self.sensor.sample_received.connect(self.on_sample)
        self.sensor.start()

        # Status bar
        self.statusBar().showMessage('Running')
        # Tray icon and actions for background mode
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
            self.hide()

    def _wrap_in_dock(self, title: str, widget: QtWidgets.QWidget) -> QtWidgets.QDockWidget:
        dock = QtWidgets.QDockWidget(title, self)
        dock.setWidget(widget)
        dock.setAllowedAreas(QtCore.Qt.BottomDockWidgetArea | QtCore.Qt.TopDockWidgetArea)
        return dock

    @QtCore.Slot(dict)
    def on_sample(self, sample: dict):
        cpu = sample.get('cpu_percent', '--')
        ram = sample.get('ram_percent', '--')
        gpu = sample.get('gpu_percent', '--')
        temp = sample.get('temps', {}).get('cpu_package', sample.get('temps', {}).get('gpu', '--'))

        self.cpu.setText(f'CPU: {cpu}%')
        self.ram.setText(f'RAM: {ram}%')
        self.gpu.setText(f'GPU: {gpu}%')
        try:
            self.temp.setText(f"Temp: {temp}{chr(176)}C")
        except Exception:
            self.temp.setText(f'Temp: {temp} °C')
        self.status.setText('Status: receiving data')
        try:
            self.details.setPlainText(json.dumps(sample, indent=2))
        except Exception:
            pass

        # Update charts with latest CPU/RAM history
        try:
            c = float(cpu)
            r = float(ram)
            self.hist_cpu.append(c)
            self.hist_ram.append(r)
            if self.chart_view is not None:
                self.cpu_series.replace([QtCore.QPointF(i, v) for i, v in enumerate(self.hist_cpu)])
                self.ram_series.replace([QtCore.QPointF(i, v) for i, v in enumerate(self.hist_ram)])
                n = max(len(self.hist_cpu), len(self.hist_ram))
                self.axis_x.setRange(max(0, n-60), max(59, n-1))
        except Exception:
            pass

        # Update top processes table from sample or local psutil fallback
        rows = []
        try:
            if isinstance(sample.get('process_top'), list) and sample['process_top']:
                for p in sample['process_top'][:10]:
                    rows.append((p.get('pid', ''), p.get('name', ''), p.get('cpu', ''), p.get('ram', '')))
            elif psutil is not None:
                procs = []
                for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
                    info = proc.info
                    procs.append((info['pid'], info.get('name') or '', info.get('cpu_percent') or 0.0, info.get('memory_percent') or 0.0))
                procs.sort(key=lambda x: (x[2], x[3]), reverse=True)
                rows = procs[:10]
        except Exception:
            rows = []
        if rows:
            self.table.setRowCount(len(rows))
            for i, (pid, name, cpu_p, mem_p) in enumerate(rows):
                self.table.setItem(i, 0, QtWidgets.QTableWidgetItem(str(pid)))
                self.table.setItem(i, 1, QtWidgets.QTableWidgetItem(str(name)))
                self.table.setItem(i, 2, QtWidgets.QTableWidgetItem(f"{cpu_p:.1f}" if isinstance(cpu_p, (int, float)) else str(cpu_p)))
                self.table.setItem(i, 3, QtWidgets.QTableWidgetItem(f"{mem_p:.1f}" if isinstance(mem_p, (int, float)) else str(mem_p)))

    def toggle_visible(self):
        if self.isVisible():
            self.hide()
        else:
            self.show()
            self.raise_()
            self.activateWindow()

    def quit_app(self):
        self.sensor.stop()
        QtWidgets.QApplication.quit()

    def closeEvent(self, event):
        # Close to tray
        event.ignore()
        self.hide()


def main():
    parser = argparse.ArgumentParser(description='SysMon Desktop App')
    parser.add_argument('--engine', help='Shell command to spawn (JSONL on stdout)')
    parser.add_argument('--engine-args', nargs='+', help='Engine command and args as list')
    parser.add_argument('--background', action='store_true', help='Start hidden in system tray')
    args = parser.parse_args()

    app = QtWidgets.QApplication(sys.argv)
    # Global stylesheet (brand colors, tables, etc.)
    try:
        qss = sysmon_theme.load_qss()
        if qss:
            app.setStyleSheet(qss)
    except Exception:
        pass

    engine_cmd = args.engine_args if args.engine_args else args.engine
    win = DesktopWindow(engine_cmd=engine_cmd, background=args.background)
    win.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()

