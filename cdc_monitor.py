#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL vs MySQL 数据一致性监控工具 - Textual版本
使用Textual框架提供现代化的TUI界面，支持DataTable滚动查看
实时监控两个MySQL数据库之间的数据迁移状态，支持多数据库对比和表名一一对应映射。
"""

import argparse
import asyncio
import re
import signal
import sys
from configparser import ConfigParser
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import aiomysql
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Container, Vertical
from textual.timer import Timer
from textual.widgets import DataTable, Footer, Header, Static


@dataclass
class DatabaseConfig:
    """数据库配置"""
    host: str
    port: int
    database: str
    username: str
    password: str


@dataclass
class MySQLConfig(DatabaseConfig):
    """MySQL配置"""
    databases: List[str] = field(default_factory=list)
    ignored_prefixes: List[str] = field(default_factory=list)


@dataclass
class TableInfo:
    """表信息"""
    schema_name: str
    target_table_name: str  # 目标MySQL中的表名（内部使用）
    source_rows: int = 0
    target_rows: int = 0
    previous_source_rows: int = 0
    previous_target_rows: int = 0
    last_updated: datetime = field(default_factory=datetime.now)
    source_last_updated: datetime = field(default_factory=datetime.now)
    target_last_updated: datetime = field(default_factory=datetime.now)
    is_first_query: bool = True
    source_updating: bool = False
    target_updating: bool = False
    source_is_estimated: bool = False
    target_is_estimated: bool = False

    @property
    def change(self) -> int:
        """记录数变化"""
        return 0 if self.is_first_query else self.target_rows - self.previous_target_rows

    @property
    def data_diff(self) -> int:
        """数据差异"""
        if self.target_rows == -1 or self.source_rows == -1:
            return 0  # 错误状态时差异为0，避免统计计算错误
        return self.target_rows - self.source_rows

    @property
    def is_consistent(self) -> bool:
        """检查数据是否一致"""
        if self.target_rows == 0 and self.source_rows == 0:
            return True
        return self.target_rows == self.source_rows

    def full_name(self) -> str:
        """完整表名"""
        return f"{self.schema_name}.{self.target_table_name}"


class SyncProperties:
    """表名映射规则 - 数据迁移专用，一一对应映射"""

    @staticmethod
    def get_target_table_name(source_table_name: str) -> str:
        """
        生成目标表名
        数据迁移场景下，源表和目标表一一对应，直接返回源表名作为目标表名
        """
        return source_table_name

    pass  # 类已简化，无需额外方法


class StatsWidget(Static):
    """统计信息组件"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def update_stats(self, tables: List[TableInfo], target_iteration: int, source_iteration: int, start_time: datetime,
                    is_paused: bool = False, sort_by: str = "schema_table", filter_mode: str = "all"):
        """更新统计数据"""
        # 过滤掉错误状态的表进行统计
        valid_tables = [t for t in tables if t.target_rows != -1 and t.source_rows != -1]
        error_tables = [t for t in tables if t.target_rows == -1 or t.source_rows == -1]

        total_target_rows = sum(t.target_rows for t in valid_tables)
        total_source_rows = sum(t.source_rows for t in valid_tables)
        total_diff = total_target_rows - total_source_rows
        total_changes = sum(t.change for t in valid_tables)
        changed_count = len([t for t in valid_tables if t.change != 0])

        # 一致性统计
        consistent_count = len([t for t in tables if t.is_consistent])
        inconsistent_count = len(tables) - consistent_count

        # 运行时长
        runtime = datetime.now() - start_time
        runtime_str = self._format_duration(runtime.total_seconds())

        # 构建显示文本
        text = Text()

        # 标题行
        text.append("🔍 MySQL vs MySQL 数据监控", style="bold blue")
        text.append(f" - 目标第{target_iteration}次/源第{source_iteration}次", style="dim")
        text.append(f" - 运行时长: {runtime_str}", style="cyan")

        # 状态和排序信息
        if is_paused:
            text.append(" - ", style="dim")
            text.append("⏸️ 已暂停", style="bold yellow")

        # 排序和过滤信息
        sort_display = {
            "schema_table": "Schema.表名",
            "data_diff": "数据差异",
            "target_rows": "目标记录数",
            "source_rows": "源记录数"
        }
        filter_display = {
            "all": "全部",
            "inconsistent": "不一致",
            "consistent": "一致",
            "error": "错误"
        }
        text.append(f" - 排序: {sort_display.get(sort_by, sort_by)}", style="dim")
        text.append(f" - 过滤: {filter_display.get(filter_mode, filter_mode)}", style="dim")
        text.append(f" - 总计: {len(tables)} 个表", style="dim")
        text.append("\n\n")

        # 数据量统计
        text.append("📈 数据统计: ", style="bold")
        text.append(f"目标总计: {total_target_rows:,} 行, ", style="white")
        text.append(f"源总计: {total_source_rows:,} 行, ", style="white")

        # 数据差异颜色语义化
        if total_diff < 0:
            text.append(f"数据差异: {total_diff:+,} 行", style="bold red")
        elif total_diff > 0:
            text.append(f"数据差异: {total_diff:+,} 行", style="bold green")
        else:
            text.append(f"数据差异: {total_diff:+,} 行", style="white")
        text.append("\n")

        # 变化和一致性统计
        if total_changes > 0:
            text.append(f"🔄 本轮变化: +{total_changes:,} 行", style="bold green")
        elif total_changes < 0:
            text.append(f"🔄 本轮变化: {total_changes:+,} 行", style="bold red")
        else:
            text.append(f"🔄 本轮变化: {total_changes:+,} 行", style="white")

        text.append(f" ({changed_count} 个表有变化), ", style="white")
        text.append(f"一致性: {consistent_count} 个一致", style="bold green")

        if inconsistent_count > 0:
            text.append(f", {inconsistent_count} 个不一致", style="bold red")
        if len(error_tables) > 0:
            text.append(f", {len(error_tables)} 个错误", style="bold red")

        text.append("\n")

        # 进度信息和迁移速度 - 带进度条和速度估算
        if total_source_rows > 0:
            completion_rate = min(total_target_rows / total_source_rows, 1.0)
            completion_percent = completion_rate * 100

            text.append("📊 迁移进度: ", style="bold cyan")

            # 创建进度条
            bar_width = 20
            filled_width = int(bar_width * completion_rate)
            empty_width = bar_width - filled_width

            # 进度条颜色根据完成率
            if completion_rate >= 0.95:
                bar_color = "bold green"
            elif completion_rate >= 0.8:
                bar_color = "bold yellow"
            else:
                bar_color = "bold red"

            # 显示进度条
            text.append("█" * filled_width, style=bar_color)
            text.append("░" * empty_width, style="dim")
            text.append(f" {completion_percent:.1f}%", style="bold white")
            text.append(f" ({total_target_rows:,}/{total_source_rows:,})", style="dim")

            if completion_rate >= 1.0:
                text.append(" - 已完成", style="bold green")
            else:
                remaining = total_source_rows - total_target_rows
                text.append(f" - 剩余: {remaining:,} 行", style="dim")

                # 计算迁移速度和预估时间
                if hasattr(self, 'parent_app') and self.parent_app:
                    speed = self.parent_app.calculate_migration_speed()
                    if speed > 0:
                        text.append(f" - 速度: {speed:.1f} 行/秒", style="bright_blue")
                        estimated_time = self.parent_app.estimate_remaining_time(total_source_rows, total_target_rows, speed)
                        text.append(f" - 预估: {estimated_time}", style="bright_blue")
                    else:
                        text.append(" - 速度: 计算中...", style="dim")

        self.update(text)

    def _format_duration(self, seconds: float) -> str:
        """格式化时长显示"""
        if seconds < 60:
            return f"{int(seconds)}秒"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}分{secs}秒"
        elif seconds < 86400:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}小时{minutes}分钟"
        else:
            days = int(seconds // 86400)
            hours = int((seconds % 86400) // 3600)
            return f"{days}天{hours}小时"


class MonitorApp(App[None]):
    """监控应用主类"""

    CSS = """
    Screen {
        background: $surface;
    }

    .stats {
        height: 10;
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
        self.source_config = None
        self.target_config = None
        self.monitor_config = {}
        self.tables: List[TableInfo] = []
        self.iteration = 0
        self.migration_props = SyncProperties()
        self.start_time = datetime.now()

        # 分离的更新计数器
        self.source_iteration = 0
        self.target_iteration = 0
        self.source_update_interval = 3
        self.first_source_update = True
        self.first_target_update = True
        self.source_updating = False

        # 停止标志，用于优雅退出
        self.stop_event = asyncio.Event()

        # 异步更新支持
        self.mysql_update_lock = asyncio.Lock()
        self.mysql_update_tasks = []
        self.target_update_lock = asyncio.Lock()
        self.target_update_tasks = []

        # 进度跟踪
        self.history_data = []
        self.max_history_points = 20

        # 定时器
        self.refresh_timer: Optional[Timer] = None

        # 界面控制属性
        self.is_paused = False
        self.sort_by = "schema_table"  # 可选: schema_table, data_diff, target_rows, source_rows
        self.filter_mode = "all"  # 可选: all, inconsistent, consistent, error

        # 信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def compose(self) -> ComposeResult:
        """构建UI组件"""
        yield Header()

        with Vertical():
            # 统计信息面板
            yield StatsWidget(classes="stats")

            # 数据表格容器
            with Container(classes="data-table"):
                yield DataTable(id="tables")

        yield Footer()

    def on_mount(self) -> None:
        """应用启动时的初始化"""
        # 设置数据表格
        table = self.query_one("#tables", DataTable)
        table.add_columns(
            "序号", "状态", "SCHEMA", "表名", "目标行数",
            "源行数", "差异", "变化量", "目标更新",
            "源更新"
        )

        # 启动监控任务
        self.call_later(self.start_monitoring)

    async def start_monitoring(self):
        """启动监控任务"""
        if not await self.load_config():
            self.exit(1)
            return

        # 初始化数据库连接测试
        target_conn = await self.connect_target_mysql(self.target_config.databases[0])
        if not target_conn:
            self.exit(1)
            return
        target_conn.close()

        # 初始化表结构
        target_tables = await self.initialize_tables_from_source_mysql()
        total_tables = sum(len(tables_dict) for tables_dict in target_tables.values())

        if total_tables == 0:
            self.exit(1)
            return

        # 第一次数据更新
        target_conn = await self.connect_target_mysql(self.target_config.databases[0])
        if target_conn:
            await self.get_target_mysql_rows_from_information_schema(target_conn, target_tables)
            target_conn.close()
            self.first_target_update = False

        self.source_iteration += 1
        await self.update_source_mysql_counts_async(target_tables, use_information_schema=True)
        self.first_source_update = False

        # 转换为列表格式
        self.tables = []
        for schema_name, tables_dict in target_tables.items():
            for table_info in tables_dict.values():
                self.tables.append(table_info)

        # 更新显示
        self.update_display()

        # 启动定时刷新
        refresh_interval = self.monitor_config.get('refresh_interval', 3)
        self.refresh_timer = self.set_interval(refresh_interval, self.refresh_data)

    def update_display(self):
        """更新显示内容"""
        # 更新统计信息
        stats_widget = self.query_one(StatsWidget)
        stats_widget.parent_app = self  # 传递app实例引用
        stats_widget.update_stats(
            self.tables,
            self.target_iteration,
            self.source_iteration,
            self.start_time,
            self.is_paused,
            self.sort_by,
            self.filter_mode
        )

        # 更新数据表格
        self._update_data_table()

    def _filter_tables(self, tables: List[TableInfo]) -> List[TableInfo]:
        """根据当前过滤模式过滤表格"""
        if self.filter_mode == "inconsistent":
            return [t for t in tables if not t.is_consistent]
        elif self.filter_mode == "consistent":
            return [t for t in tables if t.is_consistent]
        elif self.filter_mode == "error":
            return [t for t in tables if t.target_rows == -1 or t.source_rows == -1]
        else:  # all
            return tables

    def _sort_tables(self, tables: List[TableInfo]) -> List[TableInfo]:
        """根据当前排序方式对表格进行排序"""
        if self.sort_by == "data_diff":
            # 按数据差异排序，差异大的在前
            return sorted(tables, key=lambda t: abs(t.data_diff) if t.data_diff != 0 else -1, reverse=True)
        elif self.sort_by == "target_rows":
            # 按目标记录数排序，多的在前
            return sorted(tables, key=lambda t: t.target_rows if t.target_rows != -1 else -1, reverse=True)
        elif self.sort_by == "source_rows":
            # 按源记录数排序，多的在前
            return sorted(tables, key=lambda t: t.source_rows if t.source_rows != -1 else -1, reverse=True)
        else:  # schema_table
            # 按schema名和表名排序
            return sorted(tables, key=lambda t: (t.schema_name, t.target_table_name))

    def _update_data_table(self):
        """更新数据表格"""
        table = self.query_one("#tables", DataTable)

        # 先过滤再排序
        filtered_tables = self._filter_tables(self.tables)
        sorted_tables = self._sort_tables(filtered_tables)

        # 保存当前光标位置和滚动位置
        current_cursor = table.cursor_coordinate if table.row_count > 0 else None
        current_scroll_y = table.scroll_y if hasattr(table, 'scroll_y') else 0

        # 清空表格并重新填充
        table.clear()

        for i, t in enumerate(sorted_tables, 1):
            # 状态图标
            if t.target_rows == -1 or t.source_rows == -1:
                icon = "❌"
            elif t.is_consistent:
                icon = "✅"
            else:
                icon = "⚠️"

            # 数据差异文本和样式 - 零值与变化量保持一致
            if t.target_rows == -1 or t.source_rows == -1:
                diff_text = "[bold bright_red]ERROR[/]"  # 错误状态用亮红色
            else:
                # 根据差异大小和方向使用不同颜色
                if t.data_diff < 0:
                    diff_text = f"[bold orange3]{t.data_diff:+,}[/]"  # 负数用橙色（PG落后）
                elif t.data_diff > 0:
                    diff_text = f"[bold bright_green]{t.data_diff:+,}[/]"  # 正数用亮绿色（PG领先）
                else:
                    diff_text = "[dim white]0[/]"  # 零用暗白色（与变化量一致）

            # 变化量文本和样式 - 去掉无变化时的横线
            if t.target_rows == -1:
                change_text = "[bold bright_red]ERROR[/]"
            elif t.change > 0:
                change_text = f"[bold spring_green3]+{t.change:,} ⬆[/]"  # 增加用春绿色
            elif t.change < 0:
                change_text = f"[bold orange3]{t.change:,} ⬇[/]"  # 减少用橙色
            else:
                change_text = "[dim white]0[/]"  # 无变化只显示0，与数据差异保持一致

            # 源更新时间样式 - 与目标更新时间保持一致
            if t.source_updating:
                source_status = "[yellow3]更新中[/]"  # 使用更温和的深黄色
            else:
                source_relative_time = self.get_relative_time(t.source_last_updated)
                if "年前" in source_relative_time or "个月前" in source_relative_time:
                    source_status = f"[bold orange1]{source_relative_time}[/]"  # 很久没更新用橙色
                elif "天前" in source_relative_time:
                    source_status = f"[bold yellow3]{source_relative_time}[/]"  # 几天前用深黄色
                elif "小时前" in source_relative_time:
                    source_status = f"[bright cyan]{source_relative_time}[/]"  # 几小时前用亮青色
                else:
                    source_status = f"[dim bright_black]{source_relative_time}[/]"  # 最近更新用暗色（与目标一致）

            # 记录数显示和样式 - 区分估计值和精确值
            if t.target_rows == -1:
                target_rows_display = "[bold bright_red]ERROR[/]"
            elif t.target_is_estimated:
                target_rows_display = f"[italic bright_blue]~{t.target_rows:,}[/]"  # 估计值用斜体亮蓝色
            else:
                target_rows_display = f"[bold bright_cyan]{t.target_rows:,}[/]"  # 精确值用亮青色粗体

            if t.source_rows == -1:
                source_rows_display = "[bold bright_red]ERROR[/]"
            elif t.source_is_estimated:
                source_rows_display = f"[italic medium_purple1]~{t.source_rows:,}[/]"  # 估计值用斜体中紫色
            else:
                source_rows_display = f"[bold bright_magenta]{t.source_rows:,}[/]"  # 精确值用亮洋红色粗体

            # Schema名称和表名样式 - 使用更清晰的颜色
            schema_display = f"[bold medium_purple3]{t.schema_name[:12] + '...' if len(t.schema_name) > 15 else t.schema_name}[/]"  # Schema用中紫色
            table_display = f"[bold dodger_blue2]{t.target_table_name[:35] + '...' if len(t.target_table_name) > 38 else t.target_table_name}[/]"  # 表名用道奇蓝色

            # 目标更新时间样式 - 区分更新状态，使用更温和的颜色
            if t.target_updating:
                target_time_display = "[yellow3]更新中[/]"  # 使用更温和的深黄色
            else:
                target_relative_time = self.get_relative_time(t.last_updated)
                if "年前" in target_relative_time or "个月前" in target_relative_time:
                    target_time_display = f"[bold orange1]{target_relative_time}[/]"  # 很久没更新用橙色
                elif "天前" in target_relative_time:
                    target_time_display = f"[bold yellow3]{target_relative_time}[/]"  # 几天前用深黄色
                elif "小时前" in target_relative_time:
                    target_time_display = f"[bright cyan]{target_relative_time}[/]"  # 几小时前用亮青色
                else:
                    target_time_display = f"[dim bright_black]{target_relative_time}[/]"  # 最近更新用暗色

            # 源更新时间样式 - 使用原来MySQL更新时间的颜色方案
            if t.source_updating:
                source_time_display = "[yellow3]更新中[/]"  # 使用更温和的深黄色
            else:
                source_relative_time = self.get_relative_time(t.source_last_updated)
                if "年前" in source_relative_time or "个月前" in source_relative_time:
                    source_time_display = f"[bold orange1]{source_relative_time}[/]"  # 很久没更新用橙色
                elif "天前" in source_relative_time:
                    source_time_display = f"[bold yellow3]{source_relative_time}[/]"  # 几天前用深黄色
                elif "小时前" in source_relative_time:
                    source_time_display = f"[bright cyan]{source_relative_time}[/]"  # 几小时前用亮青色
                else:
                    source_time_display = f"[dim bright_black]{source_relative_time}[/]"  # 最近更新用暗色



            # 添加行到表格
            table.add_row(
                str(i),
                icon,
                schema_display,
                table_display,
                target_rows_display,
                source_rows_display,
                diff_text,
                change_text,
                target_time_display,
                source_time_display
            )

        # 尝试恢复光标位置和滚动位置
        if current_cursor is not None and table.row_count > 0:
            try:
                # 恢复光标位置
                new_row = min(current_cursor.row, table.row_count - 1)
                table.move_cursor(row=new_row)

                # 多种方式尝试恢复滚动位置
                self.call_after_refresh(self._restore_scroll_position, table, current_scroll_y)

            except Exception:
                pass  # 如果恢复失败，保持默认位置

    def _restore_scroll_position(self, table: DataTable, scroll_y: int):
        """恢复滚动位置的辅助方法"""
        try:
            # 尝试多种方式恢复滚动位置
            if hasattr(table, 'scroll_y'):
                table.scroll_y = scroll_y
            if hasattr(table, 'scroll_to'):
                table.scroll_to(y=scroll_y, animate=False)
            if hasattr(table, 'scroll_offset'):
                table.scroll_offset = table.scroll_offset.replace(y=scroll_y)
        except Exception:
            pass  # 静默失败，不影响正常功能

    async def refresh_data(self):
        """定时刷新数据"""
        if self.stop_event.is_set() or self.is_paused:
            return

        # 重新构建target_tables结构用于更新
        target_tables = {}
        for table_info in self.tables:
            schema_name = table_info.schema_name
            if schema_name not in target_tables:
                target_tables[schema_name] = {}
            target_tables[schema_name][table_info.target_table_name] = table_info

        # 更新目标MySQL记录数
        self.target_iteration += 1
        await self.update_target_mysql_counts_async(target_tables)

        # 按间隔更新源MySQL记录数
        if self.target_iteration % self.source_update_interval == 0:
            self.source_iteration += 1
            print(f"📊 触发源表更新: target_iteration={self.target_iteration}, source_iteration={self.source_iteration}")
            await self.update_source_mysql_counts_async(target_tables, use_information_schema=False)
        else:
            print(f"⏭️ 跳过源表更新: target_iteration={self.target_iteration}, 将在第{self.source_update_interval - (self.target_iteration % self.source_update_interval)}次刷新时更新")

        # 更新进度跟踪数据
        self.update_progress_data(self.tables)

        # 更新显示
        self.update_display()

    def action_quit(self) -> None:
        """退出应用"""
        self.stop_event.set()
        if self.refresh_timer:
            self.refresh_timer.stop()
        self.exit()

    def action_refresh(self) -> None:
        """手动刷新"""
        self.call_later(self.refresh_data)

    def action_toggle_pause(self) -> None:
        """暂停/继续监控"""
        self.is_paused = not self.is_paused
        self.update_display()

    def action_sort_toggle(self) -> None:
        """切换排序方式"""
        sort_options = ["schema_table", "data_diff", "target_rows", "source_rows"]
        current_index = sort_options.index(self.sort_by)
        self.sort_by = sort_options[(current_index + 1) % len(sort_options)]
        self.update_display()

    def action_filter_toggle(self) -> None:
        """切换过滤方式"""
        filter_options = ["all", "inconsistent", "consistent", "error"]
        current_index = filter_options.index(self.filter_mode)
        self.filter_mode = filter_options[(current_index + 1) % len(filter_options)]
        self.update_display()

    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.stop_event.set()
        self.exit()

    def get_relative_time(self, target_time: datetime) -> str:
        """获取相对时间显示"""
        now = datetime.now()
        diff = now - target_time

        # 计算总秒数
        total_seconds = int(diff.total_seconds())

        if total_seconds < 0:
            return "刚刚"
        elif total_seconds < 60:
            return f"{total_seconds}秒前"
        elif total_seconds < 3600:  # 小于1小时
            minutes = total_seconds // 60
            return f"{minutes}分钟前"
        elif total_seconds < 86400:  # 小于1天
            hours = total_seconds // 3600
            return f"{hours}小时前"
        elif total_seconds < 2592000:  # 小于30天
            days = total_seconds // 86400
            return f"{days}天前"
        elif total_seconds < 31536000:  # 小于1年
            months = total_seconds // 2592000
            return f"{months}个月前"
        else:
            years = total_seconds // 31536000
            return f"{years}年前"

    def update_progress_data(self, tables: List[TableInfo]):
        """更新进度数据，计算总数和变化量"""
        current_time = datetime.now()

        # 过滤掉错误状态的表进行统计
        valid_tables = [t for t in tables if t.target_rows != -1 and t.source_rows != -1]

        total_target_rows = sum(t.target_rows for t in valid_tables)
        total_source_rows = sum(t.source_rows for t in valid_tables)
        total_target_change = sum(t.change for t in valid_tables)

        # 添加到历史数据
        self.history_data.append((current_time, total_target_rows, total_source_rows, total_target_change))

        # 保持历史数据在指定范围内
        if len(self.history_data) > self.max_history_points:
            self.history_data.pop(0)

    def calculate_migration_speed(self) -> float:
        """计算迁移速度（记录/秒）"""
        if len(self.history_data) < 2:
            return 0.0

        # 使用最近的数据点计算速度
        recent_data = self.history_data[-min(10, len(self.history_data)):]

        if len(recent_data) < 2:
            return 0.0

        # 计算时间跨度和总变化量
        time_span = (recent_data[-1][0] - recent_data[0][0]).total_seconds()
        if time_span <= 0:
            return 0.0

        # 计算目标MySQL总变化量（所有数据点的变化量之和）
        total_change = sum(data[3] for data in recent_data if data[3] > 0)  # 只计算正向变化

        return total_change / time_span if time_span > 0 else 0.0

    def estimate_remaining_time(self, source_total: int, target_total: int, speed: float) -> str:
        """估算剩余迁移时间"""
        if speed <= 0 or source_total <= 0:
            return "无法估算"

        # 计算还需要同步的记录数
        remaining_records = source_total - target_total
        if remaining_records <= 0:
            return "已完成"

        remaining_seconds = remaining_records / speed

        if remaining_seconds < 60:
            return f"{int(remaining_seconds)}秒"
        elif remaining_seconds < 3600:
            minutes = int(remaining_seconds // 60)
            seconds = int(remaining_seconds % 60)
            return f"{minutes}分{seconds}秒"
        elif remaining_seconds < 86400:
            hours = int(remaining_seconds // 3600)
            minutes = int((remaining_seconds % 3600) // 60)
            return f"{hours}小时{minutes}分钟"
        else:
            days = int(remaining_seconds // 86400)
            hours = int((remaining_seconds % 86400) // 3600)
            return f"{days}天{hours}小时"

    async def load_config(self) -> bool:
        """加载配置文件"""
        config_path = Path(self.config_file)
        if not config_path.exists():
            return False

        try:
            config = ConfigParser()
            config.read(config_path, encoding='utf-8')

            # 源MySQL配置
            mysql_source_section = config['mysql']
            if self.override_databases:
                databases_list = self.override_databases
            else:
                databases_list = mysql_source_section['databases'].split(',')

            self.source_config = MySQLConfig(
                host=mysql_source_section['host'],
                port=int(mysql_source_section['port']),
                database="",
                username=mysql_source_section['username'],
                password=mysql_source_section['password'],
                databases=databases_list,
                ignored_prefixes=mysql_source_section.get('ignored_table_prefixes', '').split(',')
            )

            # 目标MySQL配置
            mysql_target_section = config['mysql_target']
            self.target_config = MySQLConfig(
                host=mysql_target_section['host'],
                port=int(mysql_target_section['port']),
                database="",
                username=mysql_target_section['username'],
                password=mysql_target_section['password'],
                databases=[db.strip() for db in mysql_target_section['databases'].split(',')],
                ignored_prefixes=mysql_target_section.get('ignored_table_prefixes', '').split(',')
            )

            # 监控配置
            monitor_section = config['monitor']
            self.monitor_config = {
                'refresh_interval': int(monitor_section.get('refresh_interval', 3)),
                'mysql_update_interval': int(monitor_section.get('mysql_update_interval', 3)),
            }

            self.mysql_update_interval = self.monitor_config['mysql_update_interval']
            return True

        except Exception as e:
            return False

    async def connect_target_mysql(self, database: str):
        """连接目标MySQL"""
        try:
            conn = await aiomysql.connect(
                host=self.target_config.host,
                port=self.target_config.port,
                db=database,
                user=self.target_config.username,
                password=self.target_config.password,
                connect_timeout=5,
                charset='utf8mb4'
            )
            return conn
        except Exception as e:
            return None

    async def connect_source_mysql(self, database: str):
        """连接源MySQL"""
        try:
            conn = await aiomysql.connect(
                host=self.source_config.host,
                port=self.source_config.port,
                db=database,
                user=self.source_config.username,
                password=self.source_config.password,
                connect_timeout=5,
                charset='utf8mb4'
            )
            return conn
        except Exception as e:
            return None

    async def initialize_tables_from_source_mysql(self):
        """从源MySQL初始化表结构"""
        schema_tables = {}

        for schema_name in self.source_config.databases:
            schema_name = schema_name.strip()
            if not schema_name:
                continue

            source_conn = await self.connect_source_mysql(schema_name)
            if not source_conn:
                continue

            try:
                async with source_conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = %s
                          AND TABLE_TYPE = 'BASE TABLE'
                    """, (schema_name,))

                    source_table_names = []
                    rows = await cursor.fetchall()
                    for row in rows:
                        table_name = row[0]
                        if not any(table_name.startswith(prefix.strip())
                                 for prefix in self.source_config.ignored_prefixes if prefix.strip()):
                            source_table_names.append(table_name)

                # 按目标表名分组
                target_tables = {}
                for source_table_name in source_table_names:
                    target_table_name = self.migration_props.get_target_table_name(source_table_name)

                    if target_table_name not in target_tables:
                        current_time = datetime.now()
                        target_tables[target_table_name] = TableInfo(
                            schema_name=schema_name,
                            target_table_name=target_table_name,
                            source_rows=0,
                            target_rows=0,
                            source_last_updated=current_time - timedelta(days=365),
                            target_last_updated=current_time - timedelta(days=365),
                            last_updated=current_time
                        )


                if target_tables:
                    schema_tables[schema_name] = target_tables

            finally:
                source_conn.close()

        return schema_tables



    async def _update_single_schema_source_mysql(self, schema_name: str, tables_dict: Dict[str, TableInfo],
                                          use_information_schema: bool = False) -> bool:
        """更新单个schema的MySQL记录数（异步版本，支持中断）"""
        current_time = datetime.now()

        # 检查是否收到停止信号
        if self.stop_event.is_set():
            return False

        try:
            mysql_conn = await self.connect_source_mysql(schema_name)
            if not mysql_conn:
                print(f"❌ 无法连接到源MySQL数据库: {schema_name}")
                return False
            print(f"✅ 成功连接到源MySQL数据库: {schema_name}")

            try:
                if use_information_schema:
                    # 检查停止标志
                    if self.stop_event.is_set():
                        return False

                    # 第一次运行使用information_schema快速获取估计值
                    async with mysql_conn.cursor() as cursor:
                        await cursor.execute("""
                                             SELECT table_name, table_rows
                                             FROM information_schema.tables
                                             WHERE table_schema = %s
                                               AND table_type = 'BASE TABLE'
                                             ORDER BY table_rows DESC
                                             """, (schema_name,))

                        # 建立表名到行数的映射
                        table_rows_map = {}
                        rows = await cursor.fetchall()
                        for row in rows:
                            table_name, table_rows = row
                            table_rows_map[table_name] = table_rows or 0  # 处理NULL值

                    # 更新TableInfo中的MySQL行数
                    for table_info in tables_dict.values():
                        # 检查停止标志
                        if self.stop_event.is_set():
                            return False

                        async with self.mysql_update_lock:
                            if table_info.source_updating:
                                print(f"⏳ 表 {table_info.full_name} 正在更新中，跳过...")
                                continue  # 如果正在更新中，跳过

                            table_info.source_updating = True
                            table_info.source_rows = 0  # 重置
                            print(f"🔄 开始更新源表 {table_info.full_name} 的记录数...")

                            # 获取源表的估计行数
                            if table_name in table_rows_map:
                                table_info.source_rows = table_rows_map[table_name]

                            table_info.source_last_updated = current_time
                            table_info.source_updating = False
                            table_info.source_is_estimated = True  # 标记为估计值
                            print(f"✅ 完成更新源表 {table_info.full_name}: {table_info.source_rows} 条记录")
                else:
                    # 常规更新使用精确的COUNT查询 - 优化显示逻辑
                    # 首先标记所有表为更新中状态
                    async with self.mysql_update_lock:
                        for table_info in tables_dict.values():
                            if not table_info.source_updating:
                                table_info.source_updating = True

                    # 然后逐个处理表
                    for table_info in tables_dict.values():
                        # 检查停止标志
                        if self.stop_event.is_set():
                            # 恢复所有表的状态
                            async with self.mysql_update_lock:
                                for ti in tables_dict.values():
                                    ti.source_updating = False
                            return False

                        # 在锁外执行查询以避免长时间锁定
                        temp_mysql_rows = 0

                        # 更新源表的记录数
                        # 检查停止标志
                        if self.stop_event.is_set():
                            async with self.mysql_update_lock:
                                for ti in tables_dict.values():
                                    ti.source_updating = False
                            return False

                            try:
                                async with mysql_conn.cursor() as cursor:
                                    # 先尝试使用主键索引进行count查询
                                    try:
                                        await cursor.execute(
                                            f"SELECT COUNT(*) FROM `{mysql_table_name}` USE INDEX (PRIMARY)")
                                        result = await cursor.fetchone()
                                        mysql_rows = result[0]
                                    except Exception:
                                        # 如果使用索引失败（可能没有主键索引），使用普通查询
                                        await cursor.execute(f"SELECT COUNT(*) FROM `{mysql_table_name}`")
                                        result = await cursor.fetchone()
                                        mysql_rows = result[0]
                                temp_mysql_rows += mysql_rows
                            except Exception as e:
                                # 表可能不存在或无权限，跳过
                                continue

                        # 查询完成后更新结果
                        async with self.mysql_update_lock:
                            table_info.source_rows = temp_mysql_rows
                            table_info.source_last_updated = current_time
                            table_info.source_updating = False
                            table_info.source_is_estimated = False  # 标记为精确值
                            print(f"✅ 完成精确更新源表 {table_info.full_name}: {table_info.source_rows} 条记录")

                return True
            finally:
                mysql_conn.close()

        except Exception as e:
            # 出现异常时，标记所有表的source_updating为False
            async with self.mysql_update_lock:
                for table_info in tables_dict.values():
                    if table_info.source_updating:
                        table_info.target_updating = False
            return False

    async def update_source_mysql_counts_async(self, target_tables: Dict[str, Dict[str, TableInfo]],
                                        use_information_schema: bool = False):
        """异步更新源MySQL记录数（不阻塞主线程）"""
        # 清理已完成的任务
        self.mysql_update_tasks = [f for f in self.mysql_update_tasks if not f.done()]

        # 为每个schema提交异步更新任务
        for schema_name, tables_dict in target_tables.items():
            # 检查该schema是否已经有正在进行的更新任务
            schema_updating = False
            async with self.mysql_update_lock:
                for table_info in tables_dict.values():
                    if table_info.source_updating:
                        schema_updating = True
                        break

            if not schema_updating:
                print(f"🚀 提交源表更新任务: schema={schema_name}, 表数量={len(tables_dict)}")
                future = asyncio.create_task(
                    self._update_single_schema_source_mysql(schema_name, tables_dict, use_information_schema))
                self.mysql_update_tasks.append(future)

    async def update_source_mysql_counts(self, conn, target_tables: Dict[str, Dict[str, TableInfo]],
                                  use_information_schema: bool = False):
        """更新源MySQL记录数（同步版本，用于兼容性）"""
        for schema_name, tables_dict in target_tables.items():
            await self._update_single_schema_source_mysql(schema_name, tables_dict, use_information_schema)

    async def get_target_mysql_rows_from_information_schema(self, conn, target_tables: Dict[str, Dict[str, TableInfo]]):
        """第一次运行时使用information_schema快速获取目标MySQL表行数估计值"""
        current_time = datetime.now()
        self.target_updating = True

        try:
            for schema_name, tables_dict in target_tables.items():
                try:
                    # 一次性获取该schema下所有表的统计信息
                    async with conn.cursor() as cursor:
                        await cursor.execute("""
                            SELECT TABLE_NAME, TABLE_ROWS
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_SCHEMA = %s
                        """, (schema_name,))

                        rows = await cursor.fetchall()
                        target_stats_map = {}
                        for row in rows:
                            table_name, estimated_rows = row[0], row[1]
                            target_stats_map[table_name] = max(0, estimated_rows or 0)  # 确保非负数

                    # 更新TableInfo
                    for target_table_name, table_info in tables_dict.items():
                        if target_table_name in target_stats_map:
                            new_count = target_stats_map[target_table_name]
                        else:
                            # 如果统计信息中没有，可能是新表或无数据，使用精确查询
                            try:
                                async with conn.cursor() as cursor:
                                    await cursor.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{target_table_name}`")
                                    result = await cursor.fetchone()
                                    new_count = result[0] if result else 0
                            except:
                                new_count = -1  # 查询失败标记为-1

                        if not table_info.is_first_query:
                            table_info.previous_target_rows = table_info.target_rows
                        else:
                            table_info.previous_target_rows = new_count
                            table_info.is_first_query = False

                        table_info.target_rows = new_count
                        table_info.last_updated = current_time
                        table_info.target_is_estimated = True  # 标记为估计值

                except Exception as e:
                    # 如果information_schema查询失败，回退到逐表精确查询
                    await self.update_target_mysql_counts(conn, {schema_name: tables_dict})
        finally:
            self.target_updating = False

    async def _update_single_schema_target_mysql(self, schema_name: str, tables_dict: Dict[str, TableInfo]) -> bool:
        """更新单个schema的目标MySQL记录数（异步版本，支持中断）"""
        current_time = datetime.now()

        # 检查是否收到停止信号
        if self.stop_event.is_set():
            return False

        try:
            conn = await self.connect_source_mysql(schema_name)
            if not conn:
                return False

            try:
                # 常规更新使用精确的COUNT查询 - 优化显示逻辑
                # 首先标记所有表为更新中状态
                async with self.target_update_lock:
                    for table_info in tables_dict.values():
                        if not table_info.target_updating:
                            table_info.target_updating = True

                # 然后逐个处理表
                for target_table_name, table_info in tables_dict.items():
                    # 检查停止标志
                    if self.stop_event.is_set():
                        # 恢复所有表的状态
                        async with self.target_update_lock:
                            for ti in tables_dict.values():
                                ti.target_updating = False
                        return False

                    # 在锁外执行查询以避免长时间锁定
                    try:
                        # 直接获取记录数
                        async with conn.cursor() as cursor:
                            await cursor.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{target_table_name}`")
                            result = await cursor.fetchone()
                            new_count = result[0] if result else 0

                        # 查询完成后更新结果
                        async with self.target_update_lock:
                            if not table_info.is_first_query:
                                table_info.previous_target_rows = table_info.target_rows
                            else:
                                table_info.previous_target_rows = new_count
                                table_info.is_first_query = False

                            table_info.target_rows = new_count
                            table_info.target_last_updated = current_time
                            table_info.target_updating = False
                            table_info.target_is_estimated = False  # 标记为精确值

                    except Exception as e:
                        # 出现异常时标记为错误状态
                        async with self.target_update_lock:
                            if not table_info.is_first_query:
                                table_info.previous_target_rows = table_info.target_rows
                            else:
                                table_info.previous_target_rows = -1
                                table_info.is_first_query = False

                            table_info.target_rows = -1  # -1表示查询失败
                            table_info.target_last_updated = current_time
                            table_info.target_updating = False
                            table_info.target_is_estimated = False  # 错误状态不是估计值

                return True
            finally:
                conn.close()

        except Exception as e:
            # 出现异常时，标记所有表的target_updating为False
            async with self.target_update_lock:
                for table_info in tables_dict.values():
                    if table_info.target_updating:
                        table_info.target_updating = False
            return False

    async def update_target_mysql_counts_exact(self, conn, target_tables: Dict[str, Dict[str, TableInfo]]):
        """异步更新目标MySQL记录数（不阻塞主线程）"""
        # 清理已完成的任务
        self.target_update_tasks = [f for f in self.target_update_tasks if not f.done()]

        # 检查是否已经有正在进行的更新任务
        if self.target_updating:
            return

        # 为每个schema提交异步更新任务
        for schema_name, tables_dict in target_tables.items():
            # 检查该schema是否已经有正在进行的更新任务
            schema_updating = False
            for table_info in tables_dict.values():
                if table_info.target_updating:
                    schema_updating = True
                    break

            if not schema_updating:
                future = asyncio.create_task(self._update_single_schema_target_mysql(schema_name, tables_dict))
                self.target_update_tasks.append(future)

    async def update_target_mysql_counts_async(self, target_tables: Dict[str, Dict[str, TableInfo]]):
        """异步更新目标MySQL记录数（不阻塞主线程）"""
        await self.update_target_mysql_counts_exact(None, target_tables)

    async def update_target_mysql_counts(self, conn, target_tables: Dict[str, Dict[str, TableInfo]]):
        """更新目标MySQL记录数（同步版本，用于兼容性）"""
        current_time = datetime.now()
        self.target_updating = True
        try:
            await self._update_target_mysql_counts_exact(conn, target_tables)
        finally:
            self.target_updating = False

    async def _update_target_mysql_counts_exact(self, conn, target_tables: Dict[str, Dict[str, TableInfo]]):
        """使用精确COUNT查询更新目标MySQL记录数"""
        current_time = datetime.now()
        for schema_name, tables_dict in target_tables.items():
            for target_table_name, table_info in tables_dict.items():
                try:
                    # 直接获取记录数
                    async with conn.cursor() as cursor:
                        await cursor.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{target_table_name}`")
                        result = await cursor.fetchone()
                        new_count = result[0] if result else 0

                    if not table_info.is_first_query:
                        table_info.previous_target_rows = table_info.target_rows
                    else:
                        table_info.previous_target_rows = new_count
                        table_info.is_first_query = False

                    table_info.target_rows = new_count
                    table_info.last_updated = current_time
                    table_info.target_is_estimated = False  # 标记为精确值

                except Exception as e:
                    # 出现异常时标记为错误状态，记录数设为-1表示错误
                    if not table_info.is_first_query:
                        table_info.previous_target_rows = table_info.target_rows
                    else:
                        table_info.previous_target_rows = -1
                        table_info.is_first_query = False

                    table_info.target_rows = -1  # -1表示查询失败
                    table_info.last_updated = current_time
                    table_info.target_is_estimated = False  # 错误状态不是估计值





def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="MySQL vs MySQL 数据一致性监控工具 (Textual版本)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python3 cdc_monitor.py                          # 使用配置文件中的数据库列表
  python3 cdc_monitor.py --databases db1,db2     # 监控指定的数据库
  python3 cdc_monitor.py -d test_db               # 只监控test_db数据库
  python3 cdc_monitor.py --config my_config.ini  # 使用指定的配置文件

快捷键:
  q/Ctrl+C : 退出程序
  r        : 手动刷新数据
  space    : 暂停/继续监控
  s        : 切换排序方式 (Schema.表名 → 数据差异 → PG记录数 → MySQL记录数)
  f        : 切换过滤方式 (全部 → 不一致 → 一致 → 错误)
  方向键   : 移动光标浏览表格
  Page Up/Down : 快速翻页
        """
    )

    parser.add_argument(
        '--databases', '-d',
        type=str,
        help='指定要监控的MySQL数据库列表（逗号分隔），覆盖配置文件中的databases配置'
    )

    parser.add_argument(
        '--config', '-c',
        type=str,
        default="config.ini",
        help='指定配置文件路径（默认: config.ini）'
    )

    args = parser.parse_args()

    # 检查配置文件是否存在
    config_file = args.config
    if not Path(config_file).exists():
        print(f"❌ 配置文件不存在: {config_file}")
        print("请确保config.ini文件存在并配置正确")
        sys.exit(1)

    # 处理数据库列表参数
    override_databases = None
    if args.databases:
        override_databases = [db.strip() for db in args.databases.split(',') if db.strip()]
        if not override_databases:
            print("❌ 指定的数据库列表为空")
            sys.exit(1)

    app = MonitorApp(config_file, override_databases)
    app.run()


if __name__ == "__main__":
    main()
