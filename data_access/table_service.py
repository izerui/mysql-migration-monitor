#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
表数据服务层
负责表数据的获取、更新和管理，协调多个MySQL数据源的数据同步
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from config_models import MySQLConfig
from .mysql_repository import MySQLRepository


@dataclass
class TableInfo:
    """表信息数据类"""
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


class TableDataService:
    """表数据服务，负责管理表数据的获取和更新"""

    def __init__(self, source_config: MySQLConfig, target_config: MySQLConfig):
        self.source_config = source_config
        self.target_config = target_config
        self.source_repository = MySQLRepository(source_config)
        self.target_repository = MySQLRepository(target_config)

        # 异步更新锁和任务管理
        self.update_lock = asyncio.Lock()
        self.update_tasks = []

    async def initialize_tables(self, schema_names: List[str]) -> Dict[str, Dict[str, TableInfo]]:
        """初始化表结构，从源MySQL获取表列表"""
        schema_tables = {}

        for schema_name in schema_names:
            schema_name = schema_name.strip()
            if not schema_name:
                continue

            source_conn = await self.source_repository.connect(schema_name)
            if not source_conn:
                continue

            try:
                # 获取源MySQL中的表列表
                source_table_names = await self.source_repository.get_tables_from_schema(
                    source_conn, schema_name
                )

                # 初始化表信息
                target_tables = {}
                for source_table_name in source_table_names:
                    target_table_name = source_table_name

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

    async def update_source_schema_tables(
        self,
        schema_name: str,
        tables_dict: Dict[str, TableInfo],
        use_estimation: bool = False
    ) -> bool:
        """更新单个schema的源MySQL记录数"""
        table_names = list(tables_dict.keys())
        if not table_names:
            return True

        # 标记开始更新
        async with self.update_lock:
            for table_info in tables_dict.values():
                table_info.source_updating = True

        try:
            # 获取记录数
            count_results = await self.source_repository.update_single_schema_table_counts(
                schema_name, table_names, use_estimation
            )

            current_time = datetime.now()

            # 更新表信息
            async with self.update_lock:
                for table_name, count in count_results.items():
                    if table_name in tables_dict:
                        table_info = tables_dict[table_name]

                        if not table_info.is_first_query:
                            table_info.previous_source_rows = table_info.source_rows
                        else:
                            table_info.previous_source_rows = count
                            table_info.is_first_query = False

                        table_info.source_rows = count
                        table_info.source_last_updated = current_time
                        table_info.source_is_estimated = use_estimation

            return True

        except Exception as e:
            print(f"❌ 更新源schema表记录数失败 {schema_name}: {str(e)}")
            return False

        finally:
            # 清理更新状态
            async with self.update_lock:
                for table_info in tables_dict.values():
                    table_info.source_updating = False

    async def update_target_schema_tables(
        self,
        schema_name: str,
        tables_dict: Dict[str, TableInfo],
        use_estimation: bool = False
    ) -> bool:
        """更新单个schema的目标MySQL记录数"""
        table_names = list(tables_dict.keys())
        if not table_names:
            return True

        # 标记开始更新
        async with self.update_lock:
            for table_info in tables_dict.values():
                table_info.target_updating = True

        try:
            # 获取记录数
            count_results = await self.target_repository.update_single_schema_table_counts(
                schema_name, table_names, use_estimation
            )

            current_time = datetime.now()

            # 更新表信息
            async with self.update_lock:
                for table_name, count in count_results.items():
                    if table_name in tables_dict:
                        table_info = tables_dict[table_name]

                        if not table_info.is_first_query:
                            table_info.previous_target_rows = table_info.target_rows
                        else:
                            table_info.previous_target_rows = count
                            table_info.is_first_query = False

                        table_info.target_rows = count
                        table_info.last_updated = current_time
                        table_info.target_is_estimated = use_estimation

            return True

        except Exception as e:
            print(f"❌ 更新目标schema表记录数失败 {schema_name}: {str(e)}")
            return False

        finally:
            # 清理更新状态
            async with self.update_lock:
                for table_info in tables_dict.values():
                    table_info.target_updating = False

    async def submit_async_source_updates(
        self,
        target_tables: Dict[str, Dict[str, TableInfo]],
        use_estimation: bool = False
    ):
        """异步提交源表更新任务"""
        # 清理已完成的任务
        self.update_tasks = [f for f in self.update_tasks if not f.done()]

        # 为每个schema提交异步更新任务
        for schema_name, tables_dict in target_tables.items():
            # 检查该schema是否已经有正在进行的更新任务
            schema_updating = False
            async with self.update_lock:
                for table_info in tables_dict.values():
                    if table_info.source_updating:
                        schema_updating = True
                        break

            if not schema_updating:
                print(f"🚀 提交源表更新任务: schema={schema_name}, 表数量={len(tables_dict)}")
                future = asyncio.create_task(
                    self.update_source_schema_tables(schema_name, tables_dict, use_estimation)
                )
                self.update_tasks.append(future)

    async def submit_async_target_updates(
        self,
        target_tables: Dict[str, Dict[str, TableInfo]],
        use_estimation: bool = False
    ):
        """异步提交目标表更新任务"""
        # 清理已完成的任务
        self.update_tasks = [f for f in self.update_tasks if not f.done()]

        # 为每个schema提交异步更新任务
        for schema_name, tables_dict in target_tables.items():
            # 检查该schema是否已经有正在进行的更新任务
            schema_updating = False
            async with self.update_lock:
                for table_info in tables_dict.values():
                    if table_info.target_updating:
                        schema_updating = True
                        break

            if not schema_updating:
                print(f"🚀 提交目标表更新任务: schema={schema_name}, 表数量={len(tables_dict)}")
                future = asyncio.create_task(
                    self.update_target_schema_tables(schema_name, tables_dict, use_estimation)
                )
                self.update_tasks.append(future)

    async def cancel_all_updates(self):
        """取消所有正在进行的更新任务"""
        for task in self.update_tasks:
            if not task.done():
                task.cancel()

        self.update_tasks.clear()

    def get_active_task_count(self) -> int:
        """获取活跃任务数量"""
        return len([f for f in self.update_tasks if not f.done()])
