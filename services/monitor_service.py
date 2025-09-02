#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
监控核心服务层
负责监控逻辑、状态管理和业务流程控制
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional

from config_models import MySQLConfig, GlobConfig
from data_access.table_service import TableDataService, TableInfo


class MonitorService:
    """监控服务，负责整体监控逻辑和状态管理"""

    def __init__(self, source_config: MySQLConfig, target_config: MySQLConfig, global_config: GlobConfig):
        self.source_config = source_config
        self.target_config = target_config
        self.global_config = global_config

        # 数据服务层
        self.table_data_service = TableDataService(source_config, target_config)

        # 状态管理
        self.tables: List[TableInfo] = []
        self.schema_tables: Dict[str, Dict[str, TableInfo]] = {}
        self.iteration = 0
        self.source_iteration = 0
        self.target_iteration = 0

        # 运行状态
        self.start_time = datetime.now()
        self.is_paused = False
        self.stop_event = asyncio.Event()
        self.source_update_interval = 3
        self.first_source_update = True
        self.first_target_update = True

    async def initialize(self) -> bool:
        """初始化监控服务"""
        try:
            # 初始化表结构
            self.schema_tables = await self.table_data_service.initialize_tables(
                self.global_config.databases
            )

            # 展平表格列表
            self.tables = []
            for schema_tables in self.schema_tables.values():
                self.tables.extend(schema_tables.values())

            print(f"✅ 初始化完成，共发现 {len(self.tables)} 个表")
            return True

        except Exception as e:
            print(f"❌ 初始化失败: {str(e)}")
            return False

    async def start_monitoring(self, target_tables: Dict[str, Dict[str, TableInfo]]):
        """启动监控循环"""
        self.first_source_update = True
        self.first_target_update = True

        while not self.stop_event.is_set():
            if self.is_paused:
                await asyncio.sleep(1)
                continue

            # 更新迭代计数
            self.iteration += 1

            try:
                # 异步更新源数据库记录数
                if self.iteration % 1 == 0:  # 每次迭代都更新源数据库
                    if self.first_source_update:
                        # 第一次使用估计值快速获取
                        await self.table_data_service.submit_async_source_updates(
                            target_tables, use_estimation=True
                        )
                        self.first_source_update = False
                    else:
                        # 后续使用精确查询
                        await self.table_data_service.submit_async_source_updates(
                            target_tables, use_estimation=False
                        )
                    self.source_iteration += 1

                # 异步更新目标数据库记录数（延迟更新）
                if self.iteration % self.source_update_interval == 0:
                    if self.first_target_update:
                        # 第一次使用估计值快速获取
                        await self.table_data_service.submit_async_target_updates(
                            target_tables, use_estimation=True
                        )
                        self.first_target_update = False
                    else:
                        # 后续使用精确查询
                        await self.table_data_service.submit_async_target_updates(
                            target_tables, use_estimation=False
                        )
                    self.target_iteration += 1

                # 等待下一个周期
                await asyncio.sleep(self.global_config.refresh_interval)

            except asyncio.CancelledError:
                print("🛑 监控任务被取消")
                break
            except Exception as e:
                print(f"❌ 监控异常: {str(e)}")
                await asyncio.sleep(1)  # 异常时等待1秒后继续

    def pause_monitoring(self):
        """暂停监控"""
        self.is_paused = True
        print("⏸️ 监控已暂停")

    def resume_monitoring(self):
        """恢复监控"""
        self.is_paused = False
        print("▶️ 监控已恢复")

    def toggle_pause(self):
        """切换暂停/恢复状态"""
        if self.is_paused:
            self.resume_monitoring()
        else:
            self.pause_monitoring()

    async def stop_monitoring(self):
        """停止监控"""
        print("🛑 正在停止监控...")
        self.stop_event.set()

        # 取消所有正在进行的更新任务
        await self.table_data_service.cancel_all_updates()

        print("✅ 监控已停止")

    def manual_refresh(self):
        """手动触发刷新"""
        self.first_source_update = True
        self.first_target_update = True
        print("🔄 手动刷新已触发")

    def update_tables_list(self):
        """更新展平的表格列表"""
        self.tables = []
        for schema_tables in self.schema_tables.values():
            self.tables.extend(schema_tables.values())

    def get_valid_tables(self) -> List[TableInfo]:
        """获取有效的表格列表（排除错误状态的表）"""
        return [t for t in self.tables if t.target_rows != -1 and t.source_rows != -1]

    def get_error_tables(self) -> List[TableInfo]:
        """获取错误状态的表格列表"""
        return [t for t in self.tables if t.target_rows == -1 or t.source_rows == -1]

    def get_runtime_seconds(self) -> float:
        """获取运行时长（秒）"""
        return (datetime.now() - self.start_time).total_seconds()

    def get_total_stats(self):
        """获取总统计信息"""
        valid_tables = self.get_valid_tables()
        error_tables = self.get_error_tables()

        total_target_rows = sum(t.target_rows for t in valid_tables)
        total_source_rows = sum(t.source_rows for t in valid_tables)
        total_diff = total_target_rows - total_source_rows
        total_changes = sum(t.change for t in valid_tables)
        changed_count = len([t for t in valid_tables if t.change != 0])

        # 一致性统计
        consistent_count = len([t for t in self.tables if t.is_consistent])
        inconsistent_count = len(self.tables) - consistent_count

        return {
            'total_target_rows': total_target_rows,
            'total_source_rows': total_source_rows,
            'total_diff': total_diff,
            'total_changes': total_changes,
            'changed_count': changed_count,
            'consistent_count': consistent_count,
            'inconsistent_count': inconsistent_count,
            'error_count': len(error_tables),
            'total_tables': len(self.tables),
            'valid_tables': len(valid_tables)
        }
