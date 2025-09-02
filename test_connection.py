#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库连接测试脚本
用于验证源数据库和目标数据库的连接和查询是否正常
"""

import asyncio
import sys
from datetime import datetime

import aiomysql
import configparser
from pathlib import Path


async def test_mysql_connection(config, database_name, connection_type="源"):
    """测试MySQL连接和查询"""
    print(f"\n🔍 测试{connection_type}数据库连接...")
    print(f"主机: {config['host']}:{config['port']}")
    print(f"数据库: {database_name}")
    print(f"用户: {config['username']}")

    try:
        # 建立连接
        conn = await aiomysql.connect(
            host=config['host'],
            port=int(config['port']),
            db=database_name,
            user=config['username'],
            password=config['password'],
            connect_timeout=5,
            charset='utf8mb4'
        )

        print(f"✅ {connection_type}数据库连接成功")

        # 获取表列表
        async with conn.cursor() as cursor:
            await cursor.execute("""
                SELECT table_name, table_rows
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (database_name,))

            tables = await cursor.fetchall()

            if not tables:
                print(f"⚠️ 数据库 {database_name} 中没有找到任何表")
                return False

            print(f"📊 发现 {len(tables)} 个表:")

            # 显示前10个表
            for i, (table_name, estimated_rows) in enumerate(tables[:10]):
                print(f"   {i+1}. {table_name} (估计行数: {estimated_rows or 0:,})")

            if len(tables) > 10:
                print(f"   ... 还有 {len(tables) - 10} 个表")

            # 测试精确查询
            print(f"\n🔍 测试精确行数查询...")
            for table_name, _ in tables[:3]:  # 测试前3个表
                try:
                    await cursor.execute(f"SELECT COUNT(*) FROM `{database_name}`.`{table_name}`")
                    exact_count = await cursor.fetchone()
                    print(f"   {table_name}: {exact_count[0]:,} 行")
                except Exception as e:
                    print(f"   {table_name}: 查询失败 - {str(e)}")

        conn.close()
        return True

    except Exception as e:
        print(f"❌ {connection_type}数据库连接失败: {str(e)}")
        return False


async def main():
    """主测试函数"""
    print("🚀 MySQL Migration Monitor 连接测试工具")
    print("=" * 50)

    # 读取配置文件
    config_path = Path("config.ini")
    if not config_path.exists():
        print("❌ 配置文件 config.ini 不存在")
        return False

    try:
        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')

        # 获取数据库列表
        databases = [db.strip() for db in config['global']['databases'].split(',')]

        # 测试源数据库
        source_config = config['source']
        target_config = config['target']

        print(f"\n📋 配置的数据库: {', '.join(databases)}")

        # 测试每个数据库
        all_success = True

        for db_name in databases:
            print(f"\n{'='*20} 测试数据库: {db_name} {'='*20}")

            # 测试源数据库
            source_ok = await test_mysql_connection(source_config, db_name, "源")

            # 测试目标数据库
            target_ok = await test_mysql_connection(target_config, db_name, "目标")

            if source_ok and target_ok:
                print(f"\n✅ 数据库 {db_name} 测试通过")
            else:
                print(f"\n❌ 数据库 {db_name} 测试失败")
                all_success = False

        print("\n" + "=" * 50)
        if all_success:
            print("🎉 所有数据库连接测试通过！")
            print("\n如果监控工具仍然不显示数据，请检查：")
            print("1. 确认表名是否匹配（源表和目标表名必须完全一致）")
            print("2. 检查用户权限是否足够")
            print("3. 查看是否有防火墙或网络限制")
            print("4. 检查是否有大表导致查询超时")
        else:
            print("❌ 部分数据库连接测试失败，请检查配置和网络")

        return all_success

    except Exception as e:
        print(f"❌ 配置读取失败: {str(e)}")
        return False


if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n\n⏹️  测试被中断")
        sys.exit(1)
