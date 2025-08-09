# PostgreSQL vs MySQL → MySQL vs MySQL 迁移指南

## 📋 迁移概述

本指南详细说明了如何将原有的 PostgreSQL vs MySQL 数据监控工具迁移为 MySQL vs MySQL 版本。

## 🔧 主要变更

### 1. 配置文件变更

#### 旧配置 (PostgreSQL vs MySQL)
```ini
[mysql]
host = 161.189.137.213
port = 8112
databases = cloud_sale
username = cdc_user
password = bFz3B!zm_rzNCi_9wk

[postgresql]
host = postgres-828575e43f8b-public.rds-pg.volces.com
port = 5432
database = datalake
username = admin
password = LBga7J@Ed9mqHrS
```

#### 新配置 (MySQL vs MySQL)
```ini
[mysql]
host = 161.189.137.213
port = 8112
databases = cloud_sale
username = cdc_user
password = bFz3B!zm_rzNCi_9wk
ignored_table_prefixes = __spliting_,__temp_,__backup_,customer_supply_demand

[mysql_target]
host = 161.189.137.213
port = 8113
databases = cloud_sale
username = cdc_user
password = bFz3B!zm_rzNCi_9wk
ignored_table_prefixes = __spliting_,__temp_,__backup_,customer_supply_demand
```

### 2. 代码变更

#### 数据库连接
- **移除**: `import asyncpg`
- **新增**: 使用 `aiomysql` 连接两个MySQL实例
- **函数变更**:
  - `connect_postgresql()` → `connect_target_mysql()`
  - `connect_mysql()` → `connect_source_mysql()`

#### 变量命名
- **PostgreSQL相关** → **目标MySQL相关**
  - `pg_rows` → `target_rows`
  - `pg_updating` → `target_updating`
  - `pg_iteration` → `target_iteration`
  - `pg_config` → `target_config`

- **MySQL相关** → **源MySQL相关**
  - `mysql_rows` → `source_rows`
  - `mysql_updating` → `source_updating`
  - `mysql_iteration` → `source_iteration`
  - `mysql_config` → `source_config`

#### SQL语法变更
- **PostgreSQL语法** → **MySQL语法**
  - `SELECT COUNT(*) FROM "schema"."table"` → `SELECT COUNT(*) FROM `schema`.`table``
  - `pg_stat_user_tables` → `INFORMATION_SCHEMA.TABLES`

### 3. 功能变更

#### 监控逻辑
- **原逻辑**: PostgreSQL ←→ MySQL 对比
- **新逻辑**: 目标MySQL ←→ 源MySQL 对比

#### 表名映射
- **保持不变**: 继续使用原有的表名映射规则
- **适用场景**: 现在适用于两个MySQL数据库之间的表名映射

## 🚀 快速迁移步骤

### 步骤1: 备份旧配置
```bash
cp config.ini config.ini.backup
```

### 步骤2: 更新配置文件
1. 将 `[postgresql]` 部分重命名为 `[mysql_target]`
2. 确保两个MySQL实例的配置都正确
3. 添加 `ignored_table_prefixes` 配置（可选）

### 步骤3: 验证配置
```bash
# 使用测试脚本验证配置
python3 test_mysql_monitor.py
```

### 步骤4: 启动新监控
```bash
# 使用新启动脚本
./start_mysql_monitor.sh

# 或手动启动
python3 cdc_monitor.py
```

## 📊 监控界面变化

### 显示内容更新
- **标题**: "PostgreSQL 数据库监控" → "MySQL vs MySQL 数据监控"
- **列名**:
  - "PG记录数" → "目标记录数"
  - "MySQL汇总数" → "源汇总数"
  - "PG更新时间" → "目标更新时间"
  - "MySQL更新时间" → "源更新时间"

### 状态显示
- **进度计算**: 基于目标MySQL vs 源MySQL的数据量
- **同步速度**: 计算目标MySQL的数据变化速度

## 🔍 验证迁移成功

### 1. 连接测试
```bash
# 测试源MySQL连接
mysql -h <source_host> -P <source_port> -u <username> -p

# 测试目标MySQL连接
mysql -h <target_host> -P <target_port> -u <username> -p
```

### 2. 表结构验证
```bash
# 运行测试脚本
python3 test_mysql_monitor.py
```

### 3. 数据对比验证
- 确认两个MySQL数据库中的表结构一致
- 验证表名映射规则正确应用
- 检查数据量对比结果合理

## ⚠️ 注意事项

### 1. 数据库权限
确保两个MySQL用户都有以下权限：
```sql
-- 源MySQL权限
GRANT SELECT ON source_database.* TO 'monitor_user'@'%';

-- 目标MySQL权限
GRANT SELECT ON target_database.* TO 'monitor_user'@'%';

-- INFORMATION_SCHEMA权限
GRANT SELECT ON INFORMATION_SCHEMA.* TO 'monitor_user'@'%';
```

### 2. 网络配置
- 确保可以访问两个MySQL实例
- 检查防火墙设置
- 验证端口是否开放

### 3. 字符集兼容性
确保两个MySQL实例使用相同的字符集配置，避免数据对比异常。

## 🎯 常见问题解决

### Q1: 连接失败
**症状**: 程序启动时报连接错误
**解决**:
1. 检查配置文件中的主机、端口、用户名、密码
2. 验证网络连通性
3. 检查MySQL服务状态

### Q2: 表找不到
**症状**: 监控界面显示表数量为0
**解决**:
1. 检查 `databases` 配置是否正确
2. 确认表名映射规则是否适用
3. 检查 `ignored_table_prefixes` 配置

### Q3: 数据不一致
**症状**: 大量表显示数据不一致
**解决**:
1. 确认两个数据库的数据确实应该一致
2. 检查表结构是否完全相同
3. 验证数据同步机制是否正常工作

## 📈 性能优化建议

### 1. 大表处理
- 使用 `ignored_table_prefixes` 忽略不重要的表
- 调整 `refresh_interval` 降低查询频率
- 考虑分批处理大量表

### 2. 网络优化
- 将监控程序部署在靠近数据库的位置
- 使用内网连接减少延迟
- 考虑使用连接池

### 3. 资源优化
- 根据表数量调整 `max_display_rows`
- 合理设置 `mysql_update_interval`
- 监控程序资源使用情况

## 🔄 回滚方案

如果需要回滚到PostgreSQL版本：
1. 恢复备份的配置文件：`cp config.ini.backup config.ini`
2. 回滚代码到PostgreSQL版本
3. 重新安装PostgreSQL依赖：`pip install asyncpg`

## 📞 技术支持

如遇到迁移问题，请提供以下信息：
- 配置文件内容（隐去敏感信息）
- 错误日志
- 数据库版本信息
- 网络环境描述