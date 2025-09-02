#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UI组件层
包含所有自定义Textual组件，负责界面展示和用户交互
"""

from typing import Optional

from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Container
from textual.widgets import DataTable, Footer, Header, Static

from data_access.table_service import TableInfo
from services.stats_service import StatsService


class StatsWidget(Static):
    """统计信息组件"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parent_app: Optional[App] = None

    def update_stats(
        self,
        tables: list[TableInfo],
        target_iteration: int,
        source_iteration: int,
        start_time,
        is_paused: bool = False,
        sort_by: str = "schema_table",
        filter_mode: str = "all"
    ):
        """更新统计数据 - 与cdc_monitor.py保持一致"""
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
        from ui.app import MonitorApp
        if isinstance(self.parent_app, MonitorApp) and hasattr(self.parent_app, 'monitor_service'):
            runtime = self.parent_app.monitor_service.get_runtime_seconds()
            runtime_str = StatsService.format_duration(runtime)
        else:
            runtime_str = "未知"

        # 构建显示文本 - 与cdc_monitor.py完全一致
        text = Text()

        # 标题行 - 与cdc_monitor.py一致
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
                speed = StatsService.calculate_migration_speed(valid_tables)
                if speed > 0:
                    text.append(f" - 速度: {speed:.1f} 行/秒", style="bright_blue")
                    estimated_time = StatsService.estimate_remaining_time(
                        total_source_rows, total_target_rows, speed
                    )
                    text.append(f" - 预估: {estimated_time}", style="bright_blue")
                else:
                    text.append(" - 速度: 计算中...", style="dim")

        self.update(text)


class TableDisplayWidget(DataTable):
    """表格显示组件"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cursor_coordinate = (0, 0)
        self.scroll_y = 0
        self.id = "tables"

    def restore_scroll_position(self, cursor_coordinate, scroll_y):
        """恢复滚动位置"""
        self.cursor_coordinate = cursor_coordinate
        self.scroll_y = scroll_y

    def update_table_data(self, tables: list[TableInfo], sort_by: str = "schema_table", filter_mode: str = "all"):
        """更新表格数据"""
        # 先过滤再排序
        filtered_tables = StatsService.filter_tables(tables, filter_mode)
        sorted_tables = StatsService.sort_tables(filtered_tables, sort_by)

        # 保存当前光标位置和滚动位置
        from textual.coordinate import Coordinate
        current_cursor = self.cursor_coordinate if self.row_count > 0 else None
        current_scroll_y = self.scroll_y if hasattr(self, 'scroll_y') else 0

        # 检查是否需要重新创建列（只在第一次或列结构变化时）
        if not self.columns:
            self.add_columns("序号", "状态", "Schema.表名", "源记录数", "源更新时间", "目标记录数", "目标更新时间", "差异", "变化")

        # 批量更新模式 - 减少UI重绘
        self._update_in_batch(sorted_tables, current_cursor, current_scroll_y)

    def _update_in_batch(self, sorted_tables, current_cursor, current_scroll_y):
        """批量更新表格数据，减少重绘"""
        try:
            # 暂停渲染以优化性能
            self.call_later(self._perform_batch_update, sorted_tables, current_cursor, current_scroll_y)
        except Exception:
            # 回退到逐行更新
            self._perform_row_by_row_update(sorted_tables, current_cursor, current_scroll_y)

    def _perform_batch_update(self, sorted_tables, current_cursor, current_scroll_y):
        """执行批量更新"""
        # 获取当前行数
        current_rows = self.row_count

        # 批量更新现有行
        for i, t in enumerate(sorted_tables):
            if i < current_rows:
                self._update_existing_row(i, t)
            else:
                self._add_new_row(i, t)

        # 删除多余的行
        while self.row_count > len(sorted_tables):
            self.remove_row(self.row_count - 1)

        # 恢复滚动位置
        self._restore_scroll_position(current_cursor, current_scroll_y, len(sorted_tables))

    def _perform_row_by_row_update(self, sorted_tables, current_cursor, current_scroll_y):
        """逐行更新（回退方案）"""
        current_rows = self.row_count

        for i, t in enumerate(sorted_tables):
            # 与cdc_monitor.py保持一致的显示格式
            icon, diff_text, change_text, source_rows_display, source_status, target_rows_display, target_status = self._get_table_display_data(t)

            row_data = [
                Text(str(i + 1)),  # 序号
                Text(icon),  # 状态
                Text(f"{t.schema_name}.{t.target_table_name}"),  # Schema.表名
                Text.from_markup(source_rows_display),  # 源记录数
                Text.from_markup(source_status),  # 源更新时间
                Text.from_markup(target_rows_display),  # 目标记录数
                Text.from_markup(target_status),  # 目标更新时间
                Text.from_markup(diff_text),  # 差异
                Text.from_markup(change_text)  # 变化
            ]

            if i < current_rows:
                # 更新现有行
                for col_idx, cell_data in enumerate(row_data):
                    try:
                        self.update_cell_at(row=i, column=col_idx, value=cell_data)
                    except Exception:
                        self.remove_row(str(i))
                        self.add_row(*row_data, key=str(i))
            else:
                # 添加新行
                self.add_row(*row_data, key=str(i))

        # 删除多余的行
        while self.row_count > len(sorted_tables):
            self.remove_row(str(self.row_count - 1))

        self._restore_scroll_position(current_cursor, current_scroll_y, len(sorted_tables))

    def _get_table_display_data(self, table: TableInfo):
        """获取表格显示数据 - 与cdc_monitor.py保持一致"""
        # 状态图标
        if table.target_rows == -1 or table.source_rows == -1:
            icon = "❌"
        elif table.is_consistent:
            icon = "✅"
        else:
            icon = "⚠️"

        # 数据差异文本和样式
        if table.target_rows == -1 or table.source_rows == -1:
            diff_text = "[bold bright_red]ERROR[/]"
        else:
            if table.data_diff < 0:
                diff_text = f"[bold orange3]{table.data_diff:+,}[/]"
            elif table.data_diff > 0:
                diff_text = f"[bold bright_green]{table.data_diff:+,}[/]"
            else:
                diff_text = "[dim white]0[/]"

        # 变化量文本和样式
        if table.target_rows == -1:
            change_text = "[bold bright_red]ERROR[/]"
        elif table.change > 0:
            change_text = f"[bold spring_green3]+{table.change:,} ⬆[/]"
        elif table.change < 0:
            change_text = f"[bold orange3]{table.change:,} ⬇[/]"
        else:
            change_text = "[dim white]0[/]"

        # 记录数显示和样式
        if table.source_rows == -1:
            source_rows_display = "[bold bright_red]ERROR[/]"
        elif table.source_is_estimated:
            source_rows_display = f"[italic green3]~{table.source_rows:,}[/]"
        else:
            source_rows_display = f"[bold bright_green]{table.source_rows:,}[/]"

        if table.target_rows == -1:
            target_rows_display = "[bold bright_red]ERROR[/]"
        elif table.target_is_estimated:
            target_rows_display = f"[italic bright_blue]~{table.target_rows:,}[/]"
        else:
            target_rows_display = f"[bold bright_cyan]{table.target_rows:,}[/]"

        # 更新时间样式
        if table.source_updating:
            source_status = "[yellow3]更新中[/]"
        else:
            source_relative_time = StatsService.get_relative_time(table.source_last_updated)
            if "年前" in source_relative_time or "个月前" in source_relative_time:
                source_status = f"[bold orange1]{source_relative_time}[/]"
            elif "天前" in source_relative_time:
                source_status = f"[bold yellow3]{source_relative_time}[/]"
            elif "小时前" in source_relative_time:
                source_status = f"[bright cyan]{source_relative_time}[/]"
            else:
                source_status = f"[dim bright_black]{source_relative_time}[/]"

        if table.target_updating:
            target_status = "[yellow3]更新中[/]"
        else:
            target_relative_time = StatsService.get_relative_time(table.target_last_updated)
            if "年前" in target_relative_time or "个月前" in target_relative_time:
                target_status = f"[bold orange1]{target_relative_time}[/]"
            elif "天前" in target_relative_time:
                target_status = f"[bold yellow3]{target_relative_time}[/]"
            elif "小时前" in target_relative_time:
                target_status = f"[bright cyan]{target_relative_time}[/]"
            else:
                target_status = f"[dim bright_black]{target_relative_time}[/]"

        return icon, diff_text, change_text, source_rows_display, source_status, target_rows_display, target_status

    def _update_existing_row(self, row_idx, table):
        """更新单行数据"""
        icon, diff_text, change_text, source_rows_display, source_status, target_rows_display, target_status = self._get_table_display_data(table)

        updates = [
            (0, Text(str(row_idx + 1))),  # 序号
            (1, Text(icon)),  # 状态
            (2, Text(f"{table.schema_name}.{table.target_table_name}")),  # Schema.表名
            (3, Text.from_markup(source_rows_display)),  # 源记录数
            (4, Text.from_markup(source_status)),  # 源更新时间
            (5, Text.from_markup(target_rows_display)),  # 目标记录数
            (6, Text.from_markup(target_status)),  # 目标更新时间
            (7, Text.from_markup(diff_text)),  # 差异
            (8, Text.from_markup(change_text))  # 变化
        ]

        for col_idx, cell_data in updates:
            try:
                self.update_cell_at(row=row_idx, column=col_idx, value=cell_data)
            except Exception:
                pass

    def _add_new_row(self, row_idx, table):
        """添加新行"""
        icon, diff_text, change_text, source_rows_display, source_status, target_rows_display, target_status = self._get_table_display_data(table)

        row_data = [
            Text(str(row_idx + 1)),  # 序号
            Text(icon),  # 状态
            Text(f"{table.schema_name}.{table.target_table_name}"),  # Schema.表名
            Text.from_markup(source_rows_display),  # 源记录数
            Text.from_markup(source_status),  # 源更新时间
            Text.from_markup(target_rows_display),  # 目标记录数
            Text.from_markup(target_status),  # 目标更新时间
            Text.from_markup(diff_text),  # 差异
            Text.from_markup(change_text)  # 变化
        ]
        self.add_row(*row_data, key=str(row_idx))

    def _restore_scroll_position(self, current_cursor, current_scroll_y, table_count):
        """恢复滚动位置"""
        if current_cursor and current_cursor.row < table_count:
            try:
                from textual.coordinate import Coordinate
                new_row = min(current_cursor.row, table_count - 1)
                self.move_cursor(row=new_row)
                if hasattr(self, 'scroll_to') and current_scroll_y <= getattr(self, 'max_scroll_y', float('inf')):
                    self.scroll_to(y=current_scroll_y or 0)
            except Exception:
                pass


class MonitorLayout(Container):
    """监控布局容器"""

    def compose(self) -> ComposeResult:
        """布局组件"""
        yield Header()
        yield StatsWidget(classes="stats")
        yield TableDisplayWidget(classes="data-table")
        yield Footer()
