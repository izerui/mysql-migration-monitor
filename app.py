#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MySQL vs MySQL 数据一致性监控工具 - Textual版本（支持表映射关系）
使用Textual框架提供现代化的TUI界面，支持DataTable滚动查看
实时监控两个MySQL数据库之间的数据同步状态，支持表映射关系和多源表合并到目标表。
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
    target_table_name: str  # 目标MySQL中的表名
    target_rows: int = 0
    source_rows: int = 0
    previous_target_rows: int = 0
    previous_source_rows: int = 0
    source_tables: List[str] = field(default_factory=list)  # 源表列表
    last_updated: datetime = field(default_factory=datetime.now)
    source_last_updated: datetime = field(default_factory=datetime.now)
    target_last_updated: datetime = field(default_factory=datetime.now)
    is_first_query: bool = True
    source_updating: bool = False
    target_updating: bool = False
    target_is_estimated: bool = False
    source_is_estimated: bool = False
    pause_auto_refresh: bool = False  # 是否暂停自动刷新



    @property
    def data_diff(self) -> int:
        """数据差异"""
        if self.target_rows == -1 or self.source_rows == -1:
            return 0  # 错误状态时差异为0，避免统计计算错误
        return self.target_rows - self.source_rows

    @property
    def is_consistent(self) -> bool:
        """数据是否一致"""
        return self.target_rows == self.source_rows

    @property
    def full_name(self) -> str:
        """完整表名"""
        return f"{self.schema_name}.{self.target_table_name}"


class SyncProperties:
    """表名映射规则（与Java版本保持一致）"""

    @staticmethod
    def get_target_table_name(source_table_name: str) -> str:
        """
        生成目标表名
        应用表名映射规则：table_runtime、table_uuid、table_数字 统一映射到 table
        """
        if not source_table_name or not source_table_name.strip():
            return source_table_name

        # 检查是否包含下划线
        if '_' not in source_table_name:
            return source_table_name  # 没有下划线，直接返回

        # 1. 检查 runtime 后缀
        if source_table_name.endswith('_runtime'):
            return source_table_name[:-8]  # 移除 "_runtime"

        # 2. 检查 9位数字后缀
        last_underscore_index = source_table_name.rfind('_')
        if last_underscore_index > 0:
            suffix = source_table_name[last_underscore_index + 1:]
            if SyncProperties._is_numeric_suffix(suffix):
                return source_table_name[:last_underscore_index]

        # 2a. 检查 9位数字_年度 格式
        # 例如: order_bom_item_333367878_2018
        if re.match(r'.*_\d{9}_\d{4}$', source_table_name):
            return re.sub(r'_\d{9}_\d{4}$', '', source_table_name)

        # 3. 检查各种UUID格式后缀
        extracted_base_name = SyncProperties._extract_table_name_from_uuid(source_table_name)
        if extracted_base_name != source_table_name:
            return extracted_base_name

        # 不符合映射规则，保持原样
        return source_table_name

    @staticmethod
    def _is_numeric_suffix(s: str) -> bool:
        """检查字符串是否为9位纯数字"""
        if not s or not s.strip():
            return False
        return re.match(r'^\d{9}$', s) is not None

    @staticmethod
    def _extract_table_name_from_uuid(table_name: str) -> str:
        """
        从包含UUID的表名中提取基础表名
        支持多种UUID格式：
        1. order_bom_0e9b60a4_d6ed_473d_a326_9e8c8f744ec2 -> order_bom
        2. users_a1b2c3d4-e5f6-7890-abcd-ef1234567890 -> users
        3. products_a1b2c3d4e5f67890abcdef1234567890 -> products
        """
        if not table_name or '_' not in table_name:
            return table_name

        # 模式1: 下划线分隔的UUID格式 (8_4_4_4_12)
        # 例如: order_bom_0e9b60a4_d6ed_473d_a326_9e8c8f744ec2
        pattern1 = r'_[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12}$'
        if re.search(pattern1, table_name):
            return re.sub(pattern1, '', table_name)

        # 模式2: 连字符分隔的UUID格式 (8-4-4-4-12)
        # 例如: users_a1b2c3d4-e5f6-7890-abcd-ef1234567890
        pattern2 = r'_[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        if re.search(pattern2, table_name):
            return re.sub(pattern2, '', table_name)

        # 模式3: 下划线分隔的UUID格式后跟年度 (8_4_4_4_12_年度)
        # 例如: order_bom_item_05355967_c503_4a2d_9dd1_2dd7a9ffa15e_2030
        pattern3 = r'_[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12}_\d{4}$'
        if re.search(pattern3, table_name):
            return re.sub(pattern3, '', table_name)

        # 模式4: 混合格式 - 移除所有分隔符后检查是否为32位十六进制
        parts = table_name.split('_')
        if len(parts) >= 2:
            # 从后往前组合，找到可能的UUID开始位置
            for i in range(len(parts) - 1, 0, -1):
                possible_uuid_parts = parts[i:]
                possible_uuid = '_'.join(possible_uuid_parts)
                clean_uuid = re.sub(r'[-_]', '', possible_uuid)

                if len(clean_uuid) == 32 and re.match(r'^[0-9a-fA-F]{32}$', clean_uuid):
                    # 找到了UUID，返回基础表名
                    return '_'.join(parts[:i])
                elif len(clean_uuid) > 32:
                    break  # 太长了，不可能是UUID

        return table_name  # 没有找到UUID模式，返回原表名


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

        # 一致性统计
        text.append(f"一致性: {consistent_count} 个一致", style="bold green")

        if inconsistent_count > 0:
            text.append(f", {inconsistent_count} 个不一致", style="bold red")
        if len(error_tables) > 0:
            text.append(f", {len(error_tables)} 个错误", style="bold red")

        text.append("\n")

        # 进度信息和同步速度 - 带进度条和速度估算
        if total_source_rows > 0:
            completion_rate = min(total_target_rows / total_source_rows, 1.0)
            completion_percent = completion_rate * 100

            text.append("📊 同步进度: ", style="bold cyan")

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
        self.sync_props = SyncProperties()
        self.start_time = datetime.now()

        # 分离的更新计数器
        self.target_iteration = 0
        self.source_iteration = 0
        self.source_update_interval = 5
        self.first_source_update = True
        self.first_target_update = True
        self.target_updating = False
        self.source_updating = False

        # 停止标志，用于优雅退出
        self.stop_event = asyncio.Event()

        # 异步更新支持
        self.source_update_lock = asyncio.Lock()
        self.source_update_tasks = []
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

        # 用于增量更新的数据缓存
        self._last_tables_hash = None
        self._last_display_data = {}

        # 信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def compose(self) -> ComposeResult:
        """构建UI组件"""
        yield Header()

        with Vertical():
            # 统计信息面板
            stats_widget = StatsWidget(classes="stats")
            stats_widget.parent_app = self  # 传递app实例引用
            yield stats_widget

            # 数据表格容器
            with Container(classes="data-table"):
                yield DataTable(id="tables")

        yield Footer()

    def on_mount(self) -> None:
        """应用启动时的初始化"""
        # 设置数据表格
        table = self.query_one("#tables", DataTable)
        table.add_columns(
            "序号", "SCHEMA", "目标表名", "状态", "目标行数",
            "源汇总数", "数据差异", "目标更新", "源更新", "源表数量"
        )

        # 启动监控任务
        self.call_later(self.start_monitoring)

    async def start_monitoring(self):
        """启动监控任务"""
        if not await self.load_config():
            self.exit(1)
            return

        # 初始化数据库连接测试
        target_conn = await self.connect_target(self.monitor_config['databases'][0])
        if not target_conn:
            self.exit(1)
            return
        if target_conn:
            target_conn.close()

        # 初始化表结构（以目标数据库为准）
        target_tables = await self.initialize_tables_from_target()
        total_tables = sum(len(tables_dict) for tables_dict in target_tables.values())

        if total_tables == 0:
            self.exit(1)
            return

        # 第一次数据更新
        target_conn = await self.connect_target(self.monitor_config['databases'][0])
        if target_conn:
            await self.get_target_rows_from_information_schema(target_conn, target_tables)
            if target_conn is not None and hasattr(target_conn, 'closed') and not target_conn.closed:
                try:
                    await target_conn.close()
                except Exception as e:
                    print(f"关闭连接时出错: {e}")
            self.first_target_update = False

        # 首次获取源表估算值
        source_conn = await self.connect_source(self.monitor_config['databases'][0])
        if source_conn:
            await self.get_source_rows_from_information_schema(source_conn, target_tables)
            if source_conn is not None and hasattr(source_conn, 'closed') and not source_conn.closed:
                try:
                    await source_conn.close()
                except Exception as e:
                    print(f"关闭源连接时出错: {e}")
            self.first_source_update = False

        # 转换为列表格式
        self.tables = []
        for schema_name, tables_dict in target_tables.items():
            for table_info in tables_dict.values():
                self.tables.append(table_info)

        # 更新显示
        self.update_display()

        # 启动定时刷新
        refresh_interval = self.monitor_config.get('refresh_interval', 2)
        self.refresh_timer = self.set_interval(refresh_interval, self.refresh_data)

    def update_display(self):
        """更新显示内容"""
        # 更新统计信息
        stats_widget = self.query_one(StatsWidget)
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
        """更新数据表格 - 使用优化的重建策略避免滚动位置丢失"""
        table = self.query_one("#tables", DataTable)

        # 先过滤再排序
        filtered_tables = self._filter_tables(self.tables)
        sorted_tables = self._sort_tables(filtered_tables)

        # 检查是否有实际变化，如果没有则跳过更新
        current_hash = self._get_tables_hash(sorted_tables)
        if hasattr(self, '_last_tables_hash') and self._last_tables_hash == current_hash:
            return  # 数据没有变化，跳过更新

        # 保存当前滚动位置
        current_scroll_y = table.scroll_y if hasattr(table, 'scroll_y') else 0

        # 使用批量更新减少闪烁
        with self.app.batch_update():
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

                # 数据差异文本和样式
                if t.target_rows == -1 or t.source_rows == -1:
                    diff_text = "[bold bright_red]ERROR[/]"
                else:
                    if t.data_diff < 0:
                        diff_text = f"[bold orange3]{t.data_diff:+,}[/]"
                    elif t.data_diff > 0:
                        diff_text = f"[bold bright_green]{t.data_diff:+,}[/]"
                    else:
                        diff_text = "[dim white]0[/]"



                # 目标更新时间样式
                if t.target_updating:
                    target_time_display = "[yellow3]更新中[/]"
                else:
                    target_relative_time = self.get_relative_time(t.target_last_updated)
                    if "年前" in target_relative_time or "个月前" in target_relative_time:
                        target_time_display = f"[bold orange1]{target_relative_time}[/]"
                    elif "天前" in target_relative_time:
                        target_time_display = f"[bold yellow3]{target_relative_time}[/]"
                    elif "小时前" in target_relative_time:
                        target_time_display = f"[bright_cyan]{target_relative_time}[/]"
                    else:
                        target_time_display = f"[dim bright_black]{target_relative_time}[/]"

                # 源更新时间样式
                if t.source_updating:
                    source_time_display = "[yellow3]更新中[/]"
                else:
                    source_relative_time = self.get_relative_time(t.source_last_updated)
                    if "年前" in source_relative_time or "个月前" in source_relative_time:
                        source_time_display = f"[bold orange1]{source_relative_time}[/]"
                    elif "天前" in source_relative_time:
                        source_time_display = f"[bold yellow3]{source_relative_time}[/]"
                    elif "小时前" in source_relative_time:
                        source_time_display = f"[bright_cyan]{source_relative_time}[/]"
                    else:
                        source_time_display = f"[dim bright_black]{source_relative_time}[/]"

                # 记录数显示和样式
                if t.target_rows == -1:
                    target_rows_display = "[bold bright_red]ERROR[/]"
                elif t.target_is_estimated:
                    target_rows_display = f"[italic bright_blue]~{t.target_rows:,}[/]"
                else:
                    target_rows_display = f"[bold bright_cyan]{t.target_rows:,}[/]"

                if t.source_rows == -1:
                    source_rows_display = "[bold bright_red]ERROR[/]"
                elif t.source_is_estimated:
                    source_rows_display = f"[italic green3]~{t.source_rows:,}[/]"
                else:
                    source_rows_display = f"[bold bright_green]{t.source_rows:,}[/]"

                # Schema名称和表名样式
                schema_display = f"[bold medium_purple3]{t.schema_name[:12] + '...' if len(t.schema_name) > 15 else t.schema_name}[/]"
                table_display = f"[bold dodger_blue2]{t.target_table_name[:35] + '...' if len(t.target_table_name) > 38 else t.target_table_name}[/]"

                # 源表数量样式
                source_count = len(t.source_tables)
                if source_count >= 5:
                    source_count_display = f"[bold orange1]{source_count}[/]"
                elif source_count >= 3:
                    source_count_display = f"[bold yellow3]{source_count}[/]"
                elif source_count >= 2:
                    source_count_display = f"[bright_cyan]{source_count}[/]"
                else:
                    source_count_display = f"[dim bright_white]{source_count}[/]"

                # 添加行到表格
                table.add_row(
                    str(i),
                    schema_display,
                    table_display,
                    icon,
                    target_rows_display,
                    source_rows_display,
                    diff_text,
                    target_time_display,
                    source_time_display,
                    source_count_display
                )

        # 恢复滚动位置
        if current_scroll_y > 0 and hasattr(table, 'scroll_y'):
            try:
                max_scroll = table.max_scroll_y if hasattr(table, 'max_scroll_y') else current_scroll_y
                table.scroll_y = min(current_scroll_y, max_scroll)
            except Exception:
                pass  # 如果恢复失败，保持默认位置

        # 保存当前哈希值
        self._last_tables_hash = current_hash

    def _get_tables_hash(self, tables: List[TableInfo]) -> str:
        """获取表格数据的哈希值用于变化检测"""
        import hashlib
        data_str = ""
        for t in tables:
            data_str += f"{t.schema_name}:{t.target_table_name}:{t.target_rows}:{t.source_rows}:{t.data_diff}:{len(t.source_tables)}:"
        return hashlib.md5(data_str.encode()).hexdigest()





    def _rebuild_data_table(self, sorted_tables: List[TableInfo]):
        """重建数据表格（仅在必要时调用）"""
        table = self.query_one("#tables", DataTable)

        # 保存当前滚动位置
        current_scroll_y = table.scroll_y if hasattr(table, 'scroll_y') else 0

        # 使用批量更新减少闪烁
        with self.app.batch_update():
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

                # 数据差异文本和样式
                if t.target_rows == -1 or t.source_rows == -1:
                    diff_text = "[bold bright_red]ERROR[/]"
                else:
                    if t.data_diff < 0:
                        diff_text = f"[bold orange3]{t.data_diff:+,}[/]"
                    elif t.data_diff > 0:
                        diff_text = f"[bold bright_green]{t.data_diff:+,}[/]"
                    else:
                        diff_text = "[dim white]0[/]"



                # 目标更新时间样式
                if t.target_updating:
                    target_time_display = "[yellow3]更新中[/]"
                else:
                    target_relative_time = self.get_relative_time(t.target_last_updated)
                    if "年前" in target_relative_time or "个月前" in target_relative_time:
                        target_time_display = f"[bold orange1]{target_relative_time}[/]"
                    elif "天前" in target_relative_time:
                        target_time_display = f"[bold yellow3]{target_relative_time}[/]"
                    elif "小时前" in target_relative_time:
                        target_time_display = f"[bright_cyan]{target_relative_time}[/]"
                    else:
                        target_time_display = f"[dim bright_black]{target_relative_time}[/]"

                # 源更新时间样式
                if t.source_updating:
                    source_time_display = "[yellow3]更新中[/]"
                else:
                    source_relative_time = self.get_relative_time(t.source_last_updated)
                    if "年前" in source_relative_time or "个月前" in source_relative_time:
                        source_time_display = f"[bold orange1]{source_relative_time}[/]"
                    elif "天前" in source_relative_time:
                        source_time_display = f"[bold yellow3]{source_relative_time}[/]"
                    elif "小时前" in source_relative_time:
                        source_time_display = f"[bright_cyan]{source_relative_time}[/]"
                    else:
                        source_time_display = f"[dim bright_black]{source_relative_time}[/]"

                # 记录数显示和样式
                if t.target_rows == -1:
                    target_rows_display = "[bold bright_red]ERROR[/]"
                elif t.target_is_estimated:
                    target_rows_display = f"[italic bright_blue]~{t.target_rows:,}[/]"
                else:
                    target_rows_display = f"[bold bright_cyan]{t.target_rows:,}[/]"

                if t.source_rows == -1:
                    source_rows_display = "[bold bright_red]ERROR[/]"
                elif t.source_is_estimated:
                    source_rows_display = f"[italic green3]~{t.source_rows:,}[/]"
                else:
                    source_rows_display = f"[bold bright_green]{t.source_rows:,}[/]"

                # Schema名称和表名样式
                schema_display = f"[bold medium_purple3]{t.schema_name[:12] + '...' if len(t.schema_name) > 15 else t.schema_name}[/]"
                table_display = f"[bold dodger_blue2]{t.target_table_name[:35] + '...' if len(t.target_table_name) > 38 else t.target_table_name}[/]"

                # 源表数量样式
                source_count = len(t.source_tables)
                if source_count >= 5:
                    source_count_display = f"[bold orange1]{source_count}[/]"
                elif source_count >= 3:
                    source_count_display = f"[bold yellow3]{source_count}[/]"
                elif source_count >= 2:
                    source_count_display = f"[bright_cyan]{source_count}[/]"
                else:
                    source_count_display = f"[dim bright_white]{source_count}[/]"

                # 添加行到表格
                table.add_row(
                    str(i),
                    schema_display,
                    table_display,
                    icon,
                    target_rows_display,
                    source_rows_display,
                    diff_text,
                    target_time_display,
                    source_time_display,
                    source_count_display
                )

        # 恢复滚动位置
        if current_scroll_y > 0 and hasattr(table, 'scroll_y'):
            try:
                max_scroll = table.max_scroll_y if hasattr(table, 'max_scroll_y') else current_scroll_y
                table.scroll_y = min(current_scroll_y, max_scroll)
            except Exception:
                pass  # 如果恢复失败，保持默认位置

    async def refresh_data(self):
        """定时刷新数据"""
        if self.stop_event.is_set() or self.is_paused:
            return

        # 重新构建target_tables结构用于更新
        # 数据不一致的表始终更新，数据一致的表根据pause_auto_refresh决定
        target_tables = {}
        skipped_count = 0
        for table_info in self.tables:
            # 如果数据不一致，始终更新（忽略pause_auto_refresh）
            # 如果数据一致且暂停自动刷新，则跳过
            if table_info.is_consistent and table_info.pause_auto_refresh:
                skipped_count += 1
                continue
            schema_name = table_info.schema_name
            if schema_name not in target_tables:
                target_tables[schema_name] = {}
            target_tables[schema_name][table_info.target_table_name] = table_info

        # 如果没有需要更新的表，直接返回
        if not target_tables:
            self.log(f"所有表都已暂停自动刷新或数据一致，跳过更新 (共跳过 {skipped_count} 个表)")
            return
        else:
            self.log(f"自动刷新 {len(target_tables)} 个schema的表，跳过 {skipped_count} 个已暂停的表")

        # 更新目标MySQL记录数
        self.target_iteration += 1
        await self.update_target_counts_async(target_tables)

        # 按间隔更新源MySQL记录数
        if self.target_iteration % self.source_update_interval == 0:
            self.source_iteration += 1
            await self.update_source_counts_async(target_tables, use_information_schema=False)

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
        # 重置所有表的暂停状态
        for table_info in self.tables:
            table_info.pause_auto_refresh = False
        self.log("手动刷新，重置所有表的暂停状态")

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
        """更新进度数据，计算总数"""
        current_time = datetime.now()

        # 过滤掉错误状态的表进行统计
        valid_tables = [t for t in tables if t.target_rows != -1 and t.source_rows != -1]

        total_target_rows = sum(t.target_rows for t in valid_tables)
        total_source_rows = sum(t.source_rows for t in valid_tables)

        # 添加到历史数据
        self.history_data.append((current_time, total_target_rows, total_source_rows, 0))

        # 保持历史数据在指定范围内
        if len(self.history_data) > self.max_history_points:
            self.history_data.pop(0)





    async def load_config(self) -> bool:
        """加载配置文件"""
        config_path = Path(self.config_file)
        if not config_path.exists():
            return False

        try:
            config = ConfigParser()
            config.read(config_path, encoding='utf-8')

            # 源数据库 MySQL 配置
            source_section = config['source']
            self.source_config = MySQLConfig(
                host=source_section['host'],
                port=int(source_section['port']),
                database="",
                username=source_section['username'],
                password=source_section['password']
            )

            # 目标数据库 MySQL 配置
            target_section = config['target']
            self.target_config = MySQLConfig(
                host=target_section['host'],
                port=int(target_section['port']),
                database="",
                username=target_section['username'],
                password=target_section['password']
            )

            # 监控配置
            monitor_section = config['monitor']
            if self.override_databases:
                databases_list = self.override_databases
            else:
                databases_list = [db.strip() for db in monitor_section['databases'].split(',')]

            self.monitor_config = {
                'databases': databases_list,
                'refresh_interval': int(monitor_section.get('refresh_interval', 2)),
                'source_update_interval': int(monitor_section.get('source_update_interval', 5)),
                'ignored_table_prefixes': monitor_section.get('ignored_table_prefixes', '').split(',')
            }

            self.source_update_interval = self.monitor_config['source_update_interval']
            return True

        except Exception as e:
            return False

    async def connect_source(self, database: str):
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

    async def connect_target(self, database: str):
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

    async def initialize_tables_from_target(self):
        """从目标MySQL初始化表结构，以目标数据库的表为准"""
        schema_tables = {}

        for schema_name in self.monitor_config['databases']:
            schema_name = schema_name.strip()
            if not schema_name:
                continue

            # 先获取目标数据库的表结构
            target_conn = await self.connect_target(schema_name)
            if not target_conn:
                continue

            try:
                async with target_conn.cursor() as cursor:
                    await cursor.execute("""
                                         SELECT TABLE_NAME
                                         FROM INFORMATION_SCHEMA.TABLES
                                         WHERE TABLE_SCHEMA = %s
                                           AND TABLE_TYPE = 'BASE TABLE'
                                         """, (schema_name,))

                    target_table_names = []
                    rows = await cursor.fetchall()
                    for row in rows:
                        table_name = row[0]
                        if not any(table_name.startswith(prefix.strip())
                                   for prefix in self.monitor_config['ignored_table_prefixes'] if prefix.strip()):
                            target_table_names.append(table_name)

                # 获取源数据库的表结构用于匹配
                source_conn = await self.connect_source(schema_name)
                source_table_names = []
                if source_conn:
                    try:
                        async with source_conn.cursor() as cursor:
                            await cursor.execute("""
                                                 SELECT TABLE_NAME
                                                 FROM INFORMATION_SCHEMA.TABLES
                                                 WHERE TABLE_SCHEMA = %s
                                                   AND TABLE_TYPE = 'BASE TABLE'
                                                 """, (schema_name,))
                            rows = await cursor.fetchall()
                            for row in rows:
                                table_name = row[0]
                                if not any(table_name.startswith(prefix.strip())
                                           for prefix in self.monitor_config['ignored_table_prefixes'] if prefix.strip()):
                                    source_table_names.append(table_name)
                    finally:
                        source_conn.close()

                # 创建目标表信息
                target_tables = {}
                for target_table_name in target_table_names:
                    current_time = datetime.now()
                    target_tables[target_table_name] = TableInfo(
                        schema_name=schema_name,
                        target_table_name=target_table_name,
                        target_rows=0,
                        source_rows=0,
                        source_last_updated=current_time - timedelta(days=365),
                        target_last_updated=current_time - timedelta(days=365),
                        last_updated=current_time
                    )

                    # 反向映射逻辑：收集所有映射到该目标表的源表
                    # 清空源表列表，准备重新收集
                    target_tables[target_table_name].source_tables = []

                    # 1. 直接匹配：如果目标表名在源表中存在，添加为源表
                    if target_table_name in source_table_names:
                        target_tables[target_table_name].source_tables.append(target_table_name)

                    # 2. 转换规则匹配：收集所有映射到该目标表的源表
                    for source_table in source_table_names:
                        mapped_target = self.sync_props.get_target_table_name(source_table)
                        if mapped_target == target_table_name:
                            # 避免重复添加
                            if source_table not in target_tables[target_table_name].source_tables:
                                target_tables[target_table_name].source_tables.append(source_table)

                    # 3. 如果没有找到任何源表，使用目标表名作为源表名（默认情况）
                    if not target_tables[target_table_name].source_tables:
                        target_tables[target_table_name].source_tables.append(target_table_name)

                    # 调试日志：显示映射关系
                    if len(target_tables[target_table_name].source_tables) > 1:
                        self.log(f"表映射: {target_table_name} <- {target_tables[target_table_name].source_tables}")

                if target_tables:
                    schema_tables[schema_name] = target_tables

            finally:
                target_conn.close()

        return schema_tables

    async def get_source_rows_from_information_schema(self, conn, target_tables: Dict[str, Dict[str, TableInfo]]):
        """第一次运行时使用information_schema快速获取源MySQL表行数估计值"""
        current_time = datetime.now()

        try:
            for schema_name, tables_dict in target_tables.items():
                try:
                    # 检查连接是否有效
                    if conn is None or not hasattr(conn, 'closed') or conn.closed:
                        return

                    # 收集所有需要查询的源表
                    all_source_tables = set()
                    for table_info in tables_dict.values():
                        all_source_tables.update(table_info.source_tables)

                    if not all_source_tables:
                        continue

                    # 一次性获取所有源表的统计信息
                    async with conn.cursor() as cursor:
                        placeholders = ','.join(['%s'] * len(all_source_tables))
                        await cursor.execute(f"""
                            SELECT table_name, table_rows
                            FROM information_schema.tables
                            WHERE table_schema = %s
                            AND table_name IN ({placeholders})
                        """, (schema_name, *all_source_tables))

                        # 建立表名到估计行数的映射
                        source_stats_map = {}
                        rows = await cursor.fetchall()
                        for row in rows:
                            table_name, table_rows = row[0], row[1]
                            source_stats_map[table_name] = max(0, table_rows or 0)

                    # 更新每个目标表的源行数（估算值）
                    for target_table_name, table_info in tables_dict.items():
                        total_source_rows = 0
                        for source_table_name in table_info.source_tables:
                            if source_table_name in source_stats_map:
                                total_source_rows += source_stats_map[source_table_name]

                        if not table_info.is_first_query:
                            table_info.previous_source_rows = table_info.source_rows
                        else:
                            table_info.previous_source_rows = total_source_rows
                            table_info.is_first_query = False

                        table_info.source_rows = total_source_rows
                        table_info.source_last_updated = current_time
                        table_info.source_is_estimated = True  # 首次使用估算值
                        # 首次估算值不暂停自动刷新，等待精确值

                except Exception as e:
                    # 如果information_schema查询失败，回退到逐表精确查询
                    if conn is not None and hasattr(conn, 'closed') and not conn.closed:
                        await self.update_source_counts(conn, {schema_name: tables_dict}, use_information_schema=True)
        except Exception as e:
            print(f"get_source_rows_from_information_schema 异常: {e}")

    async def get_target_rows_from_information_schema(self, conn, target_tables: Dict[str, Dict[str, TableInfo]]):
        """第一次运行时使用information_schema快速获取目标MySQL表行数估计值"""
        current_time = datetime.now()
        self.target_updating = True

        try:
            for schema_name, tables_dict in target_tables.items():
                try:
                    # 检查连接是否有效
                    if conn is None or not hasattr(conn, 'closed') or conn.closed:
                        return

                    # 一次性获取该schema下所有表的统计信息
                    async with conn.cursor() as cursor:
                        await cursor.execute("""
                                             SELECT TABLE_NAME, TABLE_ROWS
                                             FROM INFORMATION_SCHEMA.TABLES
                                             WHERE TABLE_SCHEMA = %s
                                               AND TABLE_TYPE = 'BASE TABLE'
                                             """, (schema_name,))

                        # 建立表名到估计行数的映射
                        target_stats_map = {}
                        rows = await cursor.fetchall()
                        for row in rows:
                            table_name, table_rows = row[0], row[1]
                            target_stats_map[table_name] = max(0, table_rows or 0)  # 处理NULL值

                    # 更新TableInfo中的目标行数
                    for target_table_name, table_info in tables_dict.items():
                        if target_table_name in target_stats_map:
                            new_count = target_stats_map[target_table_name]
                        else:
                            # 如果统计信息中没有，可能是新表或无数据，使用精确查询
                            try:
                                # 再次检查连接状态
                                if conn is None or not hasattr(conn, 'closed') or conn.closed:
                                    continue
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
                        table_info.target_last_updated = current_time
                        table_info.target_is_estimated = True  # 首次使用估算值
                        table_info.target_updating = False  # 重置更新状态

                except Exception as e:
                    # 估算获取失败就失败，不回退到精确查询
                    pass  # 保持当前状态，让表格显示为错误状态
        except Exception as e:
            # 捕获方法级别的异常，防止连接对象被破坏
            print(f"get_target_rows_from_information_schema 异常: {e}")
        finally:
            self.target_updating = False

    async def _update_single_schema_source(self, schema_name: str, tables_dict: Dict[str, TableInfo],
                                           use_information_schema: bool = False) -> bool:
        """更新单个schema的源MySQL记录数（异步版本，支持中断）"""
        current_time = datetime.now()

        # 检查是否收到停止信号
        if self.stop_event.is_set():
            return False

        try:
            source_conn = await self.connect_source(schema_name)
            if not source_conn:
                return False

            try:
                if use_information_schema:
                    # 检查停止标志
                    if self.stop_event.is_set():
                        return False

                    # 首先标记所有表为更新中状态
                    async with self.source_update_lock:
                        for table_info in tables_dict.values():
                            if not table_info.source_updating:
                                table_info.source_updating = True
                                table_info.source_rows = 0  # 重置
                                self.log(f"源表 {table_info.target_table_name} 开始更新")

                    # 立即更新显示以确保能看到"更新中"状态
                    self.call_from_thread(self.update_display)

                    # 使用批量查询获取所有源表的估计行数
                    if tables_dict:
                        # 构建所有需要查询的源表
                        all_source_tables = []
                        table_source_map = {}  # 记录每个目标表对应的源表

                        for target_table_name, table_info in tables_dict.items():
                            table_source_map[target_table_name] = table_info.source_tables
                            all_source_tables.extend(table_info.source_tables)

                        # 去重
                        unique_source_tables = list(set(all_source_tables))

                        if unique_source_tables:
                            try:
                                async with source_conn.cursor() as cursor:
                                    # 构建IN查询批量获取所有源表的行数
                                    placeholders = ','.join(['%s'] * len(unique_source_tables))
                                    await cursor.execute(f"""
                                        SELECT table_name, table_rows
                                        FROM information_schema.tables
                                        WHERE table_schema = %s
                                        AND table_name IN ({placeholders})
                                    """, (schema_name, *unique_source_tables))

                                    # 建立表名到行数的映射
                                    source_rows_map = {}
                                    rows = await cursor.fetchall()
                                    for row in rows:
                                        table_name, table_rows = row[0], row[1]
                                        source_rows_map[table_name] = max(0, table_rows or 0)

                                    # 更新每个目标表的源行数
                                    for target_table_name, table_info in tables_dict.items():
                                        if self.stop_event.is_set():
                                            async with self.source_update_lock:
                                                table_info.source_updating = False
                                            return False

                                        total_source_rows = 0
                                        for source_table_name in table_source_map[target_table_name]:
                                            if source_table_name in source_rows_map:
                                                total_source_rows += source_rows_map[source_table_name]
                                            else:
                                                # 表不存在或查询失败，尝试精确查询
                                                try:
                                                    async with source_conn.cursor() as cursor2:
                                                        await cursor2.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{source_table_name}`")
                                                        result = await cursor2.fetchone()
                                                        if result:
                                                            total_source_rows += result[0]
                                                except:
                                                    continue

                                        async with self.source_update_lock:
                                            table_info.source_rows = total_source_rows
                                            table_info.source_last_updated = current_time
                                            table_info.source_updating = False
                                            table_info.source_is_estimated = True  # 仅当使用information_schema.tables.table_rows时为估算值

                            except Exception as e:
                                # 批量查询失败，回退到逐个查询
                                for target_table_name, table_info in tables_dict.items():
                                    if self.stop_event.is_set():
                                        async with self.source_update_lock:
                                            table_info.source_updating = False
                                        return False

                                    total_source_rows = 0
                                    for source_table_name in table_info.source_tables:
                                        try:
                                            async with source_conn.cursor() as cursor:
                                                await cursor.execute("""
                                                    SELECT table_rows
                                                    FROM information_schema.tables
                                                    WHERE table_schema = %s
                                                    AND table_name = %s
                                                """, (schema_name, source_table_name))
                                                result = await cursor.fetchone()
                                                if result and result[0]:
                                                    total_source_rows += result[0]
                                        except:
                                            continue

                                    async with self.source_update_lock:
                                        table_info.source_rows = total_source_rows
                                        table_info.source_last_updated = current_time
                                        table_info.source_updating = False
                                        table_info.source_is_estimated = True  # 仅当使用information_schema.tables.table_rows时为估算值
                                        # 估算值不暂停自动刷新，等待精确值
                else:
                    # 常规更新使用精确的COUNT查询
                    # 首先标记所有表为更新中状态
                    async with self.source_update_lock:
                        for table_info in tables_dict.values():
                            if not table_info.source_updating:
                                table_info.source_updating = True

                    # 然后逐个处理表
                    for table_info in tables_dict.values():
                        # 检查停止标志
                        if self.stop_event.is_set():
                            # 恢复所有表的状态
                            async with self.source_update_lock:
                                for ti in tables_dict.values():
                                    ti.source_updating = False
                            return False

                        # 更新源记录数（使用批量查询优化）
                        temp_source_rows = 0

                        if table_info.source_tables:
                            # 构建批量查询SQL
                            source_tables = [f"'{table}'" for table in table_info.source_tables]
                            tables_str = ",".join(source_tables)

                            try:
                                async with source_conn.cursor() as cursor:
                                    # 使用UNION ALL批量查询所有源表
                                    if len(table_info.source_tables) == 1:
                                        # 单个源表直接查询
                                        await cursor.execute(
                                            f"SELECT COUNT(*) FROM `{schema_name}`.`{table_info.source_tables[0]}`"
                                        )
                                        result = await cursor.fetchone()
                                        if result:
                                            temp_source_rows = result[0]
                                    else:
                                        # 多个源表使用批量查询
                                        union_queries = []
                                        for source_table in table_info.source_tables:
                                            union_queries.append(
                                                f"SELECT COUNT(*) as cnt FROM `{schema_name}`.`{source_table}`"
                                            )

                                        batch_sql = " UNION ALL ".join(union_queries)
                                        await cursor.execute(batch_sql)

                                        # 汇总所有结果
                                        results = await cursor.fetchall()
                                        temp_source_rows = sum(row[0] for row in results)

                            except Exception as e:
                                # 批量查询失败，回退到逐个查询
                                self.log(f"批量查询失败，回退到逐个查询: {e}")
                                temp_source_rows = 0
                                for source_table_name in table_info.source_tables:
                                    if self.stop_event.is_set():
                                        async with self.source_update_lock:
                                            for ti in tables_dict.values():
                                                ti.source_updating = False
                                        return False

                                    try:
                                        async with source_conn.cursor() as cursor:
                                            await cursor.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{source_table_name}`")
                                            result = await cursor.fetchone()
                                            if result:
                                                temp_source_rows += result[0]
                                    except Exception as e2:
                                        continue

                        # 查询完成后更新结果
                        async with self.source_update_lock:
                            table_info.source_rows = temp_source_rows
                            table_info.source_last_updated = current_time
                            table_info.source_updating = False
                            table_info.source_is_estimated = False  # 标记为精确值
                            self.log(f"源表 {table_info.target_table_name} 更新完成，源表数量: {len(table_info.source_tables)}, 总记录数: {temp_source_rows}")

                            # 检查数据是否一致，如果一致则暂停自动刷新
                            if table_info.is_consistent:
                                table_info.pause_auto_refresh = True
                                self.log(f"表 {table_info.target_table_name} 数据一致，暂停自动刷新")

                return True
            finally:
                source_conn.close()

        except Exception as e:
            # 出现异常时，标记所有表的source_updating为False
            async with self.source_update_lock:
                for table_info in tables_dict.values():
                    if table_info.source_updating:
                        table_info.source_updating = False
            return False

    async def update_source_counts(self, target_tables: Dict[str, Dict[str, TableInfo]],
                                   use_information_schema: bool = False):
        """更新源MySQL记录数（同步版本，用于兼容性）"""
        for schema_name, tables_dict in target_tables.items():
            await self._update_single_schema_source(schema_name, tables_dict, use_information_schema)

    async def update_source_counts_async(self, target_tables: Dict[str, Dict[str, TableInfo]],
                                         use_information_schema: bool = False):
        """异步更新源MySQL记录数（不阻塞主线程）"""
        # 清理已完成的任务
        self.source_update_tasks = [f for f in self.source_update_tasks if not f.done()]

        # 为每个schema提交异步更新任务
        for schema_name, tables_dict in target_tables.items():
            # 检查该schema是否已经有正在进行的更新任务
            schema_updating = False
            async with self.source_update_lock:
                for table_info in tables_dict.values():
                    if table_info.source_updating:
                        schema_updating = True
                        break

            if not schema_updating:
                future = asyncio.create_task(
                    self._update_single_schema_source(schema_name, tables_dict, use_information_schema))
                self.source_update_tasks.append(future)

    async def _update_single_schema_target(self, schema_name: str, tables_dict: Dict[str, TableInfo],
                                           use_information_schema: bool = False) -> bool:
        """更新单个schema的目标MySQL记录数（异步版本，支持中断）"""
        current_time = datetime.now()

        # 检查是否收到停止信号
        if self.stop_event.is_set():
            return False

        try:
            target_conn = await self.connect_target(schema_name)
            if not target_conn:
                return False

            try:
                if use_information_schema:
                    # 检查停止标志
                    if self.stop_event.is_set():
                        return False

                    # 首先标记所有表为更新中状态
                    async with self.target_update_lock:
                        for table_info in tables_dict.values():
                            if not table_info.target_updating:
                                table_info.target_updating = True
                                table_info.target_rows = 0  # 重置
                                self.log(f"目标表 {table_info.target_table_name} 开始更新")

                    # 立即更新显示以确保能看到"更新中"状态
                    self.call_from_thread(self.update_display)

                    # 使用批量查询获取所有目标表的估计行数
                    if tables_dict:
                        try:
                            async with target_conn.cursor() as cursor:
                                # 一次性获取该schema下所有表的统计信息
                                await cursor.execute("""
                                                     SELECT TABLE_NAME, TABLE_ROWS
                                                     FROM INFORMATION_SCHEMA.TABLES
                                                     WHERE TABLE_SCHEMA = %s
                                                       AND TABLE_TYPE = 'BASE TABLE'
                                                     """, (schema_name,))

                                # 建立表名到估计行数的映射
                                target_stats_map = {}
                                rows = await cursor.fetchall()
                                for row in rows:
                                    table_name, table_rows = row[0], row[1]
                                    target_stats_map[table_name] = max(0, table_rows or 0)

                                # 更新每个目标表的行数
                                for target_table_name, table_info in tables_dict.items():
                                    if self.stop_event.is_set():
                                        async with self.target_update_lock:
                                            table_info.target_updating = False
                                        return False

                                    if target_table_name in target_stats_map:
                                        new_count = target_stats_map[target_table_name]
                                    else:
                                        # 如果统计信息中没有，可能是新表或无数据，使用精确查询
                                        try:
                                            async with target_conn.cursor() as cursor2:
                                                await cursor2.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{target_table_name}`")
                                                result = await cursor2.fetchone()
                                                new_count = result[0] if result else 0
                                        except:
                                            new_count = -1

                                    async with self.target_update_lock:
                                        if not table_info.is_first_query:
                                            table_info.previous_target_rows = table_info.target_rows
                                        else:
                                            table_info.previous_target_rows = new_count
                                            table_info.is_first_query = False

                                        table_info.target_rows = new_count
                                        table_info.target_last_updated = current_time
                                        table_info.target_updating = False
                                        table_info.target_is_estimated = True  # 标记为估算值
                                        self.log(f"目标表 {table_info.target_table_name} 更新完成，行数: {new_count}")

                        except Exception as e:
                            # 估算获取失败就失败，不回退
                            for target_table_name, table_info in tables_dict.items():
                                if self.stop_event.is_set():
                                    async with self.target_update_lock:
                                        table_info.target_updating = False
                                    return False

                                # 估算失败，设置为错误状态
                                async with self.target_update_lock:
                                    if not table_info.is_first_query:
                                        table_info.previous_target_rows = table_info.target_rows
                                    else:
                                        table_info.previous_target_rows = -1
                                        table_info.is_first_query = False

                                    table_info.target_rows = -1  # 标记为错误状态
                                    table_info.target_last_updated = current_time
                                    table_info.target_updating = False
                                    table_info.target_is_estimated = True  # 仍然是估算值，但失败了
                else:
                    # 常规更新使用精确的COUNT查询
                    # 首先标记所有表为更新中状态
                    async with self.target_update_lock:
                        for table_info in tables_dict.values():
                            if not table_info.target_updating:
                                table_info.target_updating = True
                                self.log(f"目标表 {table_info.target_table_name} 开始更新")

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
                            # 直接获取目标表的记录数
                            new_count = 0
                            async with target_conn.cursor() as cursor:
                                await cursor.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{target_table_name}`")
                                result = await cursor.fetchone()
                                if result:
                                    new_count = result[0]

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
                                self.log(f"目标表 {table_info.target_table_name} 更新完成，行数: {new_count}")

                                # 检查数据是否一致，如果一致则暂停自动刷新
                                if table_info.is_consistent:
                                    table_info.pause_auto_refresh = True
                                    self.log(f"表 {table_info.target_table_name} 数据一致，暂停自动刷新")

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
                                table_info.target_is_estimated = False  # 错误状态不是估计值

                return True
            finally:
                target_conn.close()

        except Exception as e:
            # 出现异常时，标记所有表的target_updating为False
            async with self.target_update_lock:
                for table_info in tables_dict.values():
                    if table_info.target_updating:
                        table_info.target_updating = False
            return False

    async def update_target_counts(self, conn, target_tables: Dict[str, Dict[str, TableInfo]]):
        """更新目标MySQL记录数（同步版本，用于兼容性）"""
        for schema_name, tables_dict in target_tables.items():
            await self._update_single_schema_target(schema_name, tables_dict, use_information_schema=False)

    async def update_target_counts_async(self, target_tables: Dict[str, Dict[str, TableInfo]]):
        """异步更新目标MySQL记录数（不阻塞主线程）"""
        # 清理已完成的任务
        self.target_update_tasks = [f for f in self.target_update_tasks if not f.done()]

        # 为每个schema提交异步更新任务
        for schema_name, tables_dict in target_tables.items():
            # 检查该schema是否已经有正在进行的更新任务
            schema_updating = False
            async with self.target_update_lock:
                for table_info in tables_dict.values():
                    if table_info.target_updating:
                        schema_updating = True
                        break

            if not schema_updating:
                future = asyncio.create_task(
                    self._update_single_schema_target(schema_name, tables_dict, use_information_schema=False))
                self.target_update_tasks.append(future)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="MySQL vs MySQL 数据一致性监控工具 (Textual版本，支持表映射关系)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python3 app.py                          # 使用配置文件中的数据库列表
  python3 app.py --databases db1,db2     # 监控指定的数据库
  python3 app.py -d test_db               # 只监控test_db数据库
  python3 app.py --config my_config.ini  # 使用指定的配置文件

快捷键:
  q/Ctrl+C : 退出程序
  r        : 手动刷新数据
  space    : 暂停/继续监控
  s        : 切换排序方式 (Schema.表名 → 数据差异 → 目标记录数 → 源记录数)
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
