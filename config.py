#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL 配置模型类
包含 MySQL 数据库配置和全局配置的数据模型
"""

import asyncio
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class MySQLConfig:
    """MySQL 数据库配置"""
    host: str
    port: int
    username: str
    password: str
    databases: List[str] = field(default_factory=list)

    async def connect(self, database: str):
        """连接MySQL数据库"""
        try:
            import aiomysql
            conn = await aiomysql.connect(
                host=self.host,
                port=self.port,
                db=database,
                user=self.username,
                password=self.password,
                connect_timeout=5,
                charset='utf8mb4'
            )
            return conn
        except Exception as e:
            print(f"❌ MySQL连接异常 ({self.host}:{self.port}): {str(e)}")
            return None

    async def get_table_rows_from_information_schema(self, conn, schema_name: str, table_name: str) -> int:
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

    async def get_table_rows_count(self, conn, table_name: str) -> int:
        """获取表的精确行数"""
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                result = await cursor.fetchone()
                return int(result[0]) if result else 0
        except Exception:
            return 0

    async def get_tables_from_schema(self, conn, schema_name: str) -> List[str]:
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


@dataclass
class GlobConfig:
    """全局配置"""
    databases: List[str] = field(default_factory=list)
    refresh_interval: int = 2
