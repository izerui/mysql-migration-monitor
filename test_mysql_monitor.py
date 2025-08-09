#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL vs MySQL 监控测试脚本
用于验证MySQL到MySQL数据监控功能是否正常
"""

import asyncio
import sys
import os
from pathlib import Path

# 添加当前目录到Python路径
sys.path.insert(0, str(Path(__file__).parent))

from cdc_monitor import MonitorApp

async def test_mysql_connections():
    """测试MySQL连接"""
    print("🔍 开始测试MySQL连接...")

    # 创建监控应用实例
    app = MonitorApp()

    # 加载配置
    if not await app.load_config():
        print("❌ 配置文件加载失败")
        return False

    print("✅ 配置文件加载成功")

    # 测试源MySQL连接
    print("📡 测试源MySQL连接...")
    try:
        source_conn = await app.connect_source_mysql(app.source_config.databases[0])
        if source_conn:
            await source_conn.close()
            print("✅ 源MySQL连接成功")
        else:
            print("❌ 源MySQL连接失败")
            return False
    except Exception as e:
        print(f"❌ 源MySQL连接异常: {e}")
        return False

    # 测试目标MySQL连接
    print("📡 测试目标MySQL连接...")
    try:
        target_conn = await app.connect_target_mysql(app.target_config.databases[0])
        if target_conn:
            await target_conn.close()
            print("✅ 目标MySQL连接成功")
        else:
            print("❌ 目标MySQL连接失败")
            return False
    except Exception as e:
        print(f"❌ 目标MySQL连接异常: {e}")
        return False

    return True

async def test_table_initialization():
    """测试表初始化"""
    print("📊 开始测试表初始化...")

    app = MonitorApp()

    if not await app.load_config():
        print("❌ 配置文件加载失败")
        return False

    try:
        target_tables = await app.initialize_tables_from_source_mysql()
        total_tables = sum(len(tables_dict) for tables_dict in target_tables.values())

        if total_tables == 0:
            print("⚠️  没有找到需要监控的表")
            return False

        print(f"✅ 表初始化成功，共发现 {total_tables} 个表")

        # 打印前5个表的信息
        count = 0
        for schema_name, tables_dict in target_tables.items():
            for table_name, table_info in tables_dict.items():
                if count >= 5:
                    break
                print(f"   📋 {schema_name}.{table_name} -> 源表: {len(table_info.source_tables)}个")
                count += 1

        return True

    except Exception as e:
        print(f"❌ 表初始化失败: {e}")
        return False

async def run_full_test():
    """运行完整测试"""
    print("🚀 MySQL vs MySQL 监控测试开始\n")

    # 测试连接
    if not await test_mysql_connections():
        return False

    print()

    # 测试表初始化
    if not await test_table_initialization():
        return False

    print("\n✅ 所有测试通过！可以启动监控程序。")
    print("\n📖 启动命令:")
    print("   python3 cdc_monitor.py")
    print("\n🎯 监控特定数据库:")
    print("   python3 cdc_monitor.py --databases your_database_name")

    return True

if __name__ == "__main__":
    try:
        asyncio.run(run_full_test())
    except KeyboardInterrupt:
        print("\n\n⏹️  测试被用户中断")
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
