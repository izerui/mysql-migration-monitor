#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试版本的MySQL迁移监控工具
添加详细的日志输出，帮助定位数据显示问题
"""

import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import aiomysql
import configparser
from config_models import MySQLConfig, GlobConfig
from data_access.mysql_repository import MySQLRepository
from data_access.table_service import TableInfo, TableDataService


# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug_monitor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DebugMySQLRepository(MySQLRepository):
    """调试版本的MySQL仓库，添加详细日志"""

    async def connect(self, database: str) -> Optional[aiomysql.Connection]:
        """连接MySQL数据库（带调试信息）"""
        logger.info(f"尝试连接数据库: {self.config.host}:{self.config.port}/{database}")
        conn = await super().connect(database)
        if conn:
            logger.info(f"✅ 成功连接数据库: {database}")
        else:
            logger.error(f"❌ 连接数据库失败: {database}")
        return conn

    async def get_table_rows_count(self, conn: aiomysql.Connection, schema_name: str, table_name: str) -> int:
        """获取表的精确行数（带调试信息）"""
        logger.debug(f"开始查询表行数: {schema_name}.{table_name}")
        try:
            result = await super().get_table_rows_count(conn, schema_name, table_name)
            logger.debug(f"查询结果: {schema_name}.{table_name} = {result} 行")
            return result
        except Exception as e:
            logger.error(f"查询表行数失败: {schema_name}.{table_name} - {str(e)}")
            return -1

    async def get_tables_from_schema(self, conn: aiomysql.Connection, schema_name: str) -> List[str]:
        """获取指定schema中的所有表名（带调试信息）"""
        logger.info(f"获取schema中的表列表: {schema_name}")
        tables = await super().get_tables_from_schema(conn, schema_name)
        logger.info(f"schema {schema_name} 中找到 {len(tables)} 个表: {tables[:10]}...")
        return tables


class DebugTableDataService(TableDataService):
    """调试版本的表数据服务"""

    async def initialize_tables(self, schema_names: List[str]) -> Dict[str, Dict[str, TableInfo]]:
        """初始化表结构（带调试信息）"""
        logger.info(f"开始初始化表结构，数据库列表: {schema_names}")
        result = await super().initialize_tables(schema_names)

        total_tables = sum(len(tables) for tables in result.values())
        logger.info(f"初始化完成，共发现 {total_tables} 个表")

        for schema_name, tables in result.items():
            logger.info(f"  {schema_name}: {len(tables)} 个表")
            for table_name, table_info in tables.items():
                logger.debug(f"    {table_name}: {table_info.full_name()}")

        return result

    async def update_source_schema_tables(self, schema_name: str, tables_dict: Dict[str, TableInfo], use_estimation: bool = False) -> bool:
        """更新源表记录数（带调试信息）"""
        logger.info(f"开始更新源表记录数: schema={schema_name}, 表数量={len(tables_dict)}, use_estimation={use_estimation}")
        result = await super().update_source_schema_tables(schema_name, tables_dict, use_estimation)
        logger.info(f"源表更新完成: schema={schema_name}, 结果={result}")

        # 记录每个表的记录数
        for table_name, table_info in tables_dict.items():
            logger.debug(f"  {table_name}: source_rows={table_info.source_rows}")

        return result

    async def update_target_schema_tables(self, schema_name: str, tables_dict: Dict[str, TableInfo], use_estimation: bool = False) -> bool:
        """更新目标表记录数（带调试信息）"""
        logger.info(f"开始更新目标表记录数: schema={schema_name}, 表数量={len(tables_dict)}, use_estimation={use_estimation}")
        result = await super().update_target_schema_tables(schema_name, tables_dict, use_estimation)
        logger.info(f"目标表更新完成: schema={schema_name}, 结果={result}")

        # 记录每个表的记录数
        for table_name, table_info in tables_dict.items():
            logger.debug(f"  {table_name}: target_rows={table_info.target_rows}")

        return result


class DebugMonitor:
    """调试版本的监控器"""

    def __init__(self, config_file: str = "config.ini"):
        self.config_file = config_file
        self.source_config: Optional[MySQLConfig] = None
        self.target_config: Optional[MySQLConfig] = None
        self.global_config: Optional[GlobConfig] = None
        self.table_data_service: Optional[DebugTableDataService] = None
        self.schema_tables: Dict[str, Dict[str, TableInfo]] = {}
        self.tables: List[TableInfo] = []

    async def load_config(self) -> bool:
        """加载配置（带调试信息）"""
        logger.info(f"开始加载配置文件: {self.config_file}")

        config_path = Path(self.config_file)
        if not config_path.exists():
            logger.error(f"配置文件不存在: {config_path}")
            return False

        try:
            config = configparser.ConfigParser()
            config.read(config_path, encoding='utf-8')

            # 读取全局配置
            databases = [db.strip() for db in config['global']['databases'].split(',')]
            refresh_interval = int(config['global'].get('refresh_interval', 3))

            self.global_config = GlobConfig(
                databases=databases,
                refresh_interval=refresh_interval
            )

            # 读取源数据库配置
            source_section = config['source']
            self.source_config = MySQLConfig(
                host=source_section['host'],
                port=int(source_section['port']),
                username=source_section['username'],
                password=source_section['password']
            )

            # 读取目标数据库配置
            target_section = config['target']
            self.target_config = MySQLConfig(
                host=target_section['host'],
                port=int(target_section['port']),
                username=target_section['username'],
                password=target_section['password']
            )

            logger.info("✅ 配置加载成功")
            logger.info(f"  数据库: {self.global_config.databases}")
            logger.info(f"  刷新间隔: {self.global_config.refresh_interval}秒")
            logger.info(f"  源数据库: {self.source_config.host}:{self.source_config.port}")
            logger.info(f"  目标数据库: {self.target_config.host}:{self.target_config.port}")

            return True

        except Exception as e:
            logger.error(f"配置加载失败: {str(e)}")
            return False

    async def initialize(self) -> bool:
        """初始化监控（带调试信息）"""
        logger.info("开始初始化监控...")

        if not await self.load_config():
            return False

        # 创建调试版本的表数据服务
        self.table_data_service = DebugTableDataService(self.source_config, self.target_config)

        # 初始化表结构
        self.schema_tables = await self.table_data_service.initialize_tables(self.global_config.databases)

        # 展平表格列表
        self.tables = []
        for schema_tables in self.schema_tables.values():
            self.tables.extend(schema_tables.values())

        logger.info(f"初始化完成，共 {len(self.tables)} 个表")
        return True

    async def run_single_update(self) -> None:
        """运行单次更新（用于调试）"""
        logger.info("开始单次更新循环...")

        for schema_name, tables_dict in self.schema_tables.items():
            logger.info(f"处理schema: {schema_name}")

            # 更新源表
            logger.info(f"更新源表记录数...")
            await self.table_data_service.update_source_schema_tables(
                schema_name, tables_dict, use_estimation=False
            )

            # 更新目标表
            logger.info(f"更新目标表记录数...")
            await self.table_data_service.update_target_schema_tables(
                schema_name, tables_dict, use_estimation=False
            )

            # 显示结果
            logger.info(f"更新完成，结果:")
            for table_name, table_info in tables_dict.items():
                logger.info(f"  {table_name}: "
                          f"源={table_info.source_rows}, "
                          f"目标={table_info.target_rows}, "
                          f"差异={table_info.data_diff}, "
                          f"一致={table_info.is_consistent}")

    def print_summary(self) -> None:
        """打印汇总信息"""
        if not self.tables:
            logger.warning("没有表数据可显示")
            return

        print("\n" + "="*80)
        print("📊 调试汇总报告")
        print("="*80)

        valid_tables = [t for t in self.tables if t.target_rows != -1 and t.source_rows != -1]
        error_tables = [t for t in self.tables if t.target_rows == -1 or t.source_rows == -1]

        print(f"总表数: {len(self.tables)}")
        print(f"有效表: {len(valid_tables)}")
        print(f"错误表: {len(error_tables)}")

        if error_tables:
            print("\n❌ 错误表列表:")
            for table in error_tables:
                print(f"  {table.full_name()}: "
                      f"源={table.source_rows}, 目标={table.target_rows}")

        if valid_tables:
            print("\n✅ 有效表统计:")
            total_source = sum(t.source_rows for t in valid_tables)
            total_target = sum(t.target_rows for t in valid_tables)
            total_diff = total_target - total_source

            print(f"  源总记录数: {total_source:,}")
            print(f"  目标总记录数: {total_target:,}")
            print(f"  总差异: {total_diff:,}")

            consistent = [t for t in valid_tables if t.is_consistent]
            inconsistent = [t for t in valid_tables if not t.is_consistent]

            print(f"  一致表: {len(consistent)}")
            print(f"  不一致表: {len(inconsistent)}")

            if inconsistent:
                print("\n⚠️ 不一致的表:")
                for table in sorted(inconsistent, key=lambda t: abs(t.data_diff), reverse=True)[:10]:
                    print(f"  {table.full_name()}: 差异={table.data_diff:,}")


async def main():
    """主函数"""
    print("🔧 MySQL Migration Monitor 调试工具")
    print("=" * 50)

    monitor = DebugMonitor()

    try:
        # 初始化
        if not await monitor.initialize():
            print("❌ 初始化失败")
            return

        # 运行单次更新
        await monitor.run_single_update()

        # 打印汇总
        monitor.print_summary()

        print("\n✅ 调试完成，请查看 debug_monitor.log 获取详细日志")

    except Exception as e:
        logger.error(f"调试过程中发生错误: {str(e)}", exc_info=True)
        print(f"❌ 调试失败: {str(e)}")
    finally:
        if monitor.table_data_service:
            await monitor.table_data_service.cancel_all_updates()


if __name__ == "__main__":
    asyncio.run(main())
