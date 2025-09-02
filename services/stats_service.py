#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统计服务层
负责计算各种统计信息和时间估算，为UI展示提供数据支持
"""

from datetime import datetime, timedelta
from typing import List, Optional

from data_access.table_service import TableInfo


class StatsService:
    """统计服务，负责计算统计信息和时间估算"""

    @staticmethod
    def get_relative_time(last_updated: datetime) -> str:
        """获取相对时间描述"""
        if not last_updated:
            return "未知时间"

        now = datetime.now()
        diff = now - last_updated

        if diff.total_seconds() < 60:
            return "刚刚"
        elif diff.total_seconds() < 3600:
            minutes = int(diff.total_seconds() // 60)
            return f"{minutes}分钟前"
        elif diff.total_seconds() < 86400:
            hours = int(diff.total_seconds() // 3600)
            return f"{hours}小时前"
        elif diff.total_seconds() < 2592000:  # 30天
            days = int(diff.total_seconds() // 86400)
            return f"{days}天前"
        elif diff.total_seconds() < 31536000:  # 1年
            months = int(diff.total_seconds() // 2592000)
            return f"{months}个月前"
        else:
            years = int(diff.total_seconds() // 31536000)
            return f"{years}年前"

    @staticmethod
    def format_duration(seconds: float) -> str:
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

    @staticmethod
    def calculate_migration_speed(
        tables: List[TableInfo],
        time_window_seconds: float = 60.0
    ) -> float:
        """计算迁移速度（行/秒）"""
        if not tables or time_window_seconds <= 0:
            return 0.0

        total_changes = sum(abs(t.change) for t in tables if t.change != 0)
        return total_changes / time_window_seconds

    @staticmethod
    def estimate_remaining_time(
        total_source_rows: int,
        total_target_rows: int,
        speed: float
    ) -> str:
        """估算剩余时间"""
        if speed <= 0:
            return "估算中..."

        remaining_diff = abs(total_source_rows - total_target_rows)
        if remaining_diff == 0:
            return "已完成"

        remaining_seconds = remaining_diff / speed

        if remaining_seconds < 60:
            return f"{int(remaining_seconds)}秒"
        elif remaining_seconds < 3600:
            minutes = int(remaining_seconds // 60)
            secs = int(remaining_seconds % 60)
            return f"{minutes}分{secs}秒"
        elif remaining_seconds < 86400:
            hours = int(remaining_seconds // 3600)
            minutes = int((remaining_seconds % 3600) // 60)
            return f"{hours}小时{minutes}分钟"
        else:
            days = int(remaining_seconds // 86400)
            hours = int((remaining_seconds % 86400) // 3600)
            return f"{days}天{hours}小时"

    @staticmethod
    def get_table_status_info(table: TableInfo) -> dict:
        """获取单个表的状态信息"""
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
            diff_style = "bright_red"
        else:
            if table.data_diff < 0:
                diff_text = f"[bold orange3]{table.data_diff:+,}[/]"
                diff_style = "orange3"
            elif table.data_diff > 0:
                diff_text = f"[bold bright_green]{table.data_diff:+,}[/]"
                diff_style = "bright_green"
            else:
                diff_text = "[dim white]0[/]"
                diff_style = "white"

        # 变化量文本和样式
        if table.target_rows == -1:
            change_text = "[bold bright_red]ERROR[/]"
            change_style = "bright_red"
        elif table.change > 0:
            change_text = f"[bold spring_green3]+{table.change:,} ⬆[/]"
            change_style = "spring_green3"
        elif table.change < 0:
            change_text = f"[bold orange3]{table.change:,} ⬇[/]"
            change_style = "orange3"
        else:
            change_text = "[dim white]0[/]"
            change_style = "white"

        # 源更新时间样式
        if table.source_updating:
            source_status = "[yellow3]更新中[/]"
            source_style = "yellow3"
        else:
            source_relative_time = StatsService.get_relative_time(table.source_last_updated)
            if "年前" in source_relative_time or "个月前" in source_relative_time:
                source_status = f"[bold orange1]{source_relative_time}[/]"
                source_style = "orange1"
            elif "天前" in source_relative_time:
                source_status = f"[bold yellow3]{source_relative_time}[/]"
                source_style = "yellow3"
            elif "小时前" in source_relative_time:
                source_status = f"[bright cyan]{source_relative_time}[/]"
                source_style = "cyan"
            else:
                source_status = f"[bright green]{source_relative_time}[/]"
                source_style = "green"

        # 目标更新时间样式
        if table.target_updating:
            target_status = "[yellow3]更新中[/]"
            target_style = "yellow3"
        else:
            target_relative_time = StatsService.get_relative_time(table.target_last_updated)
            if "年前" in target_relative_time or "个月前" in target_relative_time:
                target_status = f"[bold orange1]{target_relative_time}[/]"
                target_style = "orange1"
            elif "天前" in target_relative_time:
                target_status = f"[bold yellow3]{target_relative_time}[/]"
                target_style = "yellow3"
            elif "小时前" in target_relative_time:
                target_status = f"[bright cyan]{target_relative_time}[/]"
                target_style = "cyan"
            else:
                target_status = f"[bright green]{target_relative_time}[/]"
                target_style = "green"

        # 标记样式
        source_mark = "[yellow3]估计值[/]" if table.source_is_estimated else "[bright_green]精确值[/]"
        target_mark = "[yellow3]估计值[/]" if table.target_is_estimated else "[bright_green]精确值[/]"

        return {
            'icon': icon,
            'diff_text': diff_text,
            'diff_style': diff_style,
            'change_text': change_text,
            'change_style': change_style,
            'source_status': source_status,
            'source_style': source_style,
            'target_status': target_status,
            'target_style': target_style,
            'source_mark': source_mark,
            'target_mark': target_mark
        }

    @staticmethod
    def filter_tables(tables: List[TableInfo], filter_mode: str) -> List[TableInfo]:
        """根据过滤模式过滤表格"""
        if filter_mode == "inconsistent":
            return [t for t in tables if not t.is_consistent]
        elif filter_mode == "consistent":
            return [t for t in tables if t.is_consistent]
        elif filter_mode == "error":
            return [t for t in tables if t.target_rows == -1 or t.source_rows == -1]
        else:  # all
            return tables

    @staticmethod
    def sort_tables(tables: List[TableInfo], sort_by: str) -> List[TableInfo]:
        """根据排序方式对表格进行排序"""
        if sort_by == "data_diff":
            # 按数据差异排序，差异大的在前
            return sorted(tables, key=lambda t: abs(t.data_diff) if t.data_diff != 0 else -1, reverse=True)
        elif sort_by == "target_rows":
            # 按目标记录数排序，多的在前
            return sorted(tables, key=lambda t: t.target_rows if t.target_rows != -1 else -1, reverse=True)
        elif sort_by == "source_rows":
            # 按源记录数排序，多的在前
            return sorted(tables, key=lambda t: t.source_rows if t.source_rows != -1 else -1, reverse=True)
        else:  # schema_table
            # 按schema名和表名排序
            return sorted(tables, key=lambda t: (t.schema_name, t.target_table_name))

    @staticmethod
    def generate_summary_text(
        stats: dict,
        runtime_seconds: float,
        speed: Optional[float] = None,
        estimated_time: Optional[str] = None
    ) -> str:
        """生成汇总统计文本"""
        text_parts = []

        # 基础统计信息
        text_parts.append(f"总表数: {stats['total_tables']} ")
        text_parts.append(f"(有效: {stats['valid_tables']} 错误: {stats['error_count']})")

        if stats['total_tables'] > 0:
            consistency_rate = (stats['consistent_count'] / stats['total_tables']) * 100
            text_parts.append(f" - 一致率: {consistency_rate:.1f}%")

        text_parts.append(f" (一致: {stats['consistent_count']} 不一致: {stats['inconsistent_count']})")

        # 运行时长
        runtime_str = StatsService.format_duration(runtime_seconds)
        text_parts.append(f" | 运行时长: {runtime_str}")

        # 记录统计
        text_parts.append(f" | 源记录数: {stats['total_source_rows']:,} ")
        text_parts.append(f"目标记录数: {stats['total_target_rows']:,} ")
        text_parts.append(f"差异: {stats['total_diff']:+,}")

        if stats['changed_count'] > 0:
            text_parts.append(f" | 变化: {stats['changed_count']}表 {stats['total_changes']:+,}行")

        # 迁移速度信息
        if speed is not None and speed > 0:
            text_parts.append(f" | 速度: {speed:.1f}行/秒")
            if estimated_time:
                text_parts.append(f" | 预估: {estimated_time}")

        return ''.join(text_parts)
