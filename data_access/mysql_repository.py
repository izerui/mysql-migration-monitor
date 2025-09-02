#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL 数据访问层
封装所有MySQL数据库操作，包括连接管理、查询执行等
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import aiomysql

from config_models import MySQLConfig


class MySQLRepository:
    """MySQL数据访问仓库"""

    def __init__(self, config: MySQLConfig):
        self.config = config

    async def connect(self, database: str) -> Optional[aiomysql.Connection]:
        """连接MySQL数据库"""
        try:
            conn = await aiomysql.connect(
                host=self.config.host,
                port=self.config.port,
                db=database,
                user=self.config.username,
                password=self.config.password,
                connect_timeout=5,
                charset='utf8mb4'
            )
            return conn
        except Exception as e:
            print(f"❌ MySQL连接异常 ({self.config.host}:{self.config.port}): {str(e)}")
            return None

    async def get_table_rows_from_information_schema(
        self,
        conn: aiomysql.Connection,
        schema_name: str,
        table_name: str
    ) -> int:
        """从information_schema获取表的行数估计值"""
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    SELECT table_rows
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                """, (schema_name, table_name))
                result = await cursor.fetchone()
                return int(result[0]) if result and result[0] else 0
        except Exception:
            return 0

    async def get_table_rows_count(
        self,
        conn: aiomysql.Connection,
        schema_name: str,
        table_name: str
    ) -> int:
        """获取表的精确行数"""
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{table_name}`")
                result = await cursor.fetchone()
                return int(result[0]) if result else 0
        except Exception:
            return -1  # 返回-1表示查询失败

    async def get_tables_from_schema(
        self,
        conn: aiomysql.Connection,
        schema_name: str
    ) -> List[str]:
        """获取指定schema中的所有表名"""
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                """, (schema_name,))
                results = await cursor.fetchall()
                return [row[0] for row in results]
        except Exception:
            return []

    async def get_schema_stats_from_information_schema(
        self,
        conn: aiomysql.Connection,
        schema_name: str
    ) -> Dict[str, int]:
        """一次性获取schema下所有表的统计信息"""
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    SELECT TABLE_NAME, TABLE_ROWS
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s
                """, (schema_name,))

                rows = await cursor.fetchall()
                stats_map = {}
                for row in rows:
                    table_name, estimated_rows = row[0], row[1]
                    stats_map[table_name] = max(0, estimated_rows or 0)  # 确保非负数
                return stats_map
        except Exception:
            return {}

    async def update_single_schema_table_counts(
        self,
        schema_name: str,
        table_names: List[str],
        use_estimation: bool = False
    ) -> Dict[str, int]:
        """更新单个schema下所有表的记录数"""
        result = {}

        conn = await self.connect(schema_name)
        if not conn:
            return {table_name: -1 for table_name in table_names}

        try:
            if use_estimation:
                # 使用information_schema获取估计值
                stats_map = await self.get_schema_stats_from_information_schema(conn, schema_name)
                for table_name in table_names:
                    result[table_name] = stats_map.get(table_name, -1)
            else:
                # 使用精确查询获取每个表的记录数
                for table_name in table_names:
                    count = await self.get_table_rows_count(conn, schema_name, table_name)
                    result[table_name] = count
        finally:
            conn.close()

        return result

    async def test_connection(self, schema_name: str) -> bool:
        """测试数据库连接"""
        conn = await self.connect(schema_name)
        if conn:
            conn.close()
            return True
        return False
