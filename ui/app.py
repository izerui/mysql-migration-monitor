#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主应用类
负责整合所有UI组件和业务逻辑，提供完整的监控应用界面
"""

import asyncio
import signal
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from textual.app import App, ComposeResult
from textual.timer import Timer
from textual.widgets import Header, Footer
from textual.widgets import Header, Footer

from config_models import MySQLConfig, GlobConfig
from data_access.table_service import TableInfo
from services.monitor_service import MonitorService
from services.stats_service import StatsService
from ui.widgets import StatsWidget, TableDisplayWidget


class MonitorApp(App[None]):
    """监控应用主类 - 重构版本"""

    CSS = """
    Screen {
        background: $surface;
    }

    .stats {
        height: 8;
        border: solid $primary;
        margin: 1;
        padding: 1;
        background: $surface;
    }

    .data-table {
        height: 1fr;
        border: solid $primary;
        margin: 1;
        background: $surface;
    }

    .data-table > DataTable {
        background: $surface;
        scrollbar-background: $surface;
        scrollbar-color: $primary;
        scrollbar-corner-color: $surface;
    }

    DataTable > .datatable--cursor {
        background: $accent 50%;
    }

    DataTable > .datatable--hover {
        background: $primary 20%;
    }
    """

    BINDINGS = [
        ("q", "quit", "退出"),
        ("r", "refresh", "手动刷新"),
        ("space", "toggle_pause", "暂停/继续"),
        ("s", "sort_toggle", "切换排序"),
        ("f", "filter_toggle", "切换过滤"),
        ("ctrl+c", "quit", "退出"),
    ]

    def __init__(self, config_file: str = "config.ini", override_databases: Optional[List[str]] = None):
        super().__init__()
        self.config_file = config_file
        self.override_databases = override_databases

        # 配置和数据服务
        self.source: MySQLConfig
        self.target: MySQLConfig
        self.global_config: GlobConfig
        self.monitor_service: MonitorService

        # UI状态
        self.tables: List[TableInfo] = []
        self.start_time = datetime.now()
        self.is_paused = False
        self.sort_by = "schema_table"
        self.filter_mode = "all"

        # 排序和过滤选项
        self.sort_options = ["schema_table", "data_diff", "target_rows", "source_rows"]
        self.filter_options = ["all", "consistent", "inconsistent", "error"]
        self.current_sort_index = 0
        self.current_filter_index = 0

        # 监控任务
        self.monitor_task: Optional[asyncio.Task] = None
        self.refresh_timer: Optional[Timer] = None

        # 防抖机制
        self._last_update_time = 0
        self._update_debounce_ms = 100  # 100ms防抖
        self._pending_update = False

    def compose(self) -> ComposeResult:
        """构建UI组件"""
        yield Header()
        yield StatsWidget(classes="stats")
        yield TableDisplayWidget(classes="data-table")
        yield Footer()

    def on_mount(self):
        """应用挂载时的初始化"""
        # 设置信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # 启动初始化和监控
        asyncio.create_task(self.start_monitoring())

        # 设置定时刷新UI
        self.refresh_timer = self.set_interval(1.0, self.update_display)

    async def start_monitoring(self):
        """启动监控"""
        # 加载配置
        if not await self.load_config():
            self.exit()
            return

        # 初始化监控服务
        assert self.source is not None
        assert self.target is not None
        assert self.global_config is not None
        self.monitor_service = MonitorService(self.source, self.target, self.global_config)

        # 初始化表数据
        if not await self.monitor_service.initialize():
            self.exit()
            return

        self.tables = self.monitor_service.tables

        # 启动后台监控任务
        self.monitor_task = asyncio.create_task(
            self.monitor_service.start_monitoring(self.monitor_service.schema_tables)
        )

        # 等待监控任务完成（正常情况下不会完成，除非被取消）
        try:
            await self.monitor_task
        except asyncio.CancelledError:
            pass

    async def load_config(self) -> bool:
        """加载配置文件"""
        config_path = Path(self.config_file)
        if not config_path.exists():
            print(f"❌ 配置文件不存在: {config_path}")
            return False

        try:
            from configparser import ConfigParser

            config = ConfigParser()
            config.read(config_path, encoding='utf-8')

            # 读取全局数据库配置
            if self.override_databases:
                databases_list = self.override_databases
            else:
                global_section = config['global']
                databases_list = [db.strip() for db in global_section['databases'].split(',')]

            # 源数据库 MySQL 配置
            mysql_source_section = config['source']
            self.source = MySQLConfig(
                host=mysql_source_section['host'],
                port=int(mysql_source_section['port']),
                username=mysql_source_section['username'],
                password=mysql_source_section['password']
            )

            # 目标数据库 MySQL 配置
            mysql_target_section = config['target']
            self.target = MySQLConfig(
                host=mysql_target_section['host'],
                port=int(mysql_target_section['port']),
                username=mysql_target_section['username'],
                password=mysql_target_section['password']
            )

            # 全局配置
            global_section = config['global']
            self.global_config = GlobConfig(
                databases=databases_list,
                refresh_interval=int(global_section.get('refresh_interval', 3))
            )
            return True

        except Exception as e:
            print(f"❌ 配置加载失败: {str(e)}")
            return False

    def update_display(self):
        """更新显示内容 - 带防抖机制"""
        if not self.monitor_service:
            return

        # 防抖检查
        current_time = asyncio.get_event_loop().time() * 1000
        if current_time - self._last_update_time < self._update_debounce_ms:
            if not self._pending_update:
                self._pending_update = True
                asyncio.create_task(self._delayed_update_display())
            return

        self._last_update_time = current_time
        self._pending_update = False

        # 执行实际更新
        self._perform_update_display()

    async def _delayed_update_display(self):
        """延迟更新显示"""
        await asyncio.sleep(self._update_debounce_ms / 1000)
        if self._pending_update:
            self._perform_update_display()
            self._pending_update = False

    def _perform_update_display(self):
        """执行实际显示更新"""
        if not self.monitor_service:
            return

        # 更新表格列表
        self.monitor_service.update_tables_list()
        self.tables = self.monitor_service.tables

        # 更新统计信息
        stats_widget = self.query_one(StatsWidget)
        stats_widget.parent_app = self
        stats_widget.update_stats(
            self.tables,
            self.monitor_service.target_iteration,
            self.monitor_service.source_iteration,
            self.monitor_service.start_time,
            self.monitor_service.is_paused,
            self.sort_by,
            self.filter_mode
        )

        # 更新数据表格
        table_widget = self.query_one(TableDisplayWidget)
        table_widget.update_table_data(self.tables, self.sort_by, self.filter_mode)

    async def action_quit(self):
        """退出应用"""
        await self._stop_and_exit()

    async def _stop_and_exit(self):
        """停止监控并退出"""
        if self.monitor_service:
            await self.monitor_service.stop_monitoring()

        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()

        if self.refresh_timer:
            self.refresh_timer.stop()

        self.exit()

    def action_refresh(self):
        """手动刷新"""
        if self.monitor_service:
            self.monitor_service.manual_refresh()

    def action_toggle_pause(self):
        """切换暂停/继续"""
        if self.monitor_service:
            self.monitor_service.toggle_pause()

    def action_sort_toggle(self):
        """切换排序方式"""
        self.current_sort_index = (self.current_sort_index + 1) % len(self.sort_options)
        self.sort_by = self.sort_options[self.current_sort_index]
        self.update_display()

    def action_filter_toggle(self):
        """切换过滤方式"""
        self.current_filter_index = (self.current_filter_index + 1) % len(self.filter_options)
        self.filter_mode = self.filter_options[self.current_filter_index]
        self.update_display()

    def _signal_handler(self, signum, frame):
        """信号处理器"""
        asyncio.create_task(self._stop_and_exit())

    def get_relative_time(self, last_updated: datetime) -> str:
        """获取相对时间描述"""
        return StatsService.get_relative_time(last_updated)

    def update_progress_data(self):
        """更新进度数据 - 兼容性方法"""
        # 这个方法现在在 stats_service 中处理
        pass

    def calculate_migration_speed(self, tables: List[TableInfo]) -> float:
        """计算迁移速度 - 兼容性方法"""
        return StatsService.calculate_migration_speed(tables)

    def estimate_remaining_time(self, total_source_rows: int, total_target_rows: int, speed: float) -> str:
        """估算剩余时间 - 兼容性方法"""
        return StatsService.estimate_remaining_time(total_source_rows, total_target_rows, speed)
