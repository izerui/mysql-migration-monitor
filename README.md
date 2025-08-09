# MySQL 数据一致性监控工具

实时监控两个 MySQL 数据库之间的数据迁移状态。

## 快速开始

```bash
./start_monitor.sh
```

## 功能特点

- **实时监控** - 双数据库对比，表名一一对应映射
- **数据迁移** - 专为数据迁移场景优化，源表与目标表一一对应
- **可视化界面** - 实时刷新，颜色编码状态
- **错误容错** - 查询失败不影响整体监控

## 配置

编辑 `config.ini`：

```ini
[mysql]
host = localhost
port = 3306
databases = db1,db2
username = root
password = pass

[mysql_target]
host = localhost
port = 3307
databases = target_db
username = root
password = pass

[monitor]
refresh_interval = 2
```

## 表名映射规则

数据迁移场景下，源数据库中的表名与目标数据库中的表名一一对应，无需进行智能转换。

例如：
- 源表 `users` → 目标表 `users`
- 源表 `orders` → 目标表 `orders`

## 界面说明

- ✅ 数据一致
- ⚠️ 数据不一致  
- ❌ 查询错误

## 系统要求

- Python 3.8+
- MySQL 访问权限

## 故障排除

1. **连接失败** - 检查 MySQL 服务状态
2. **权限错误** - 确保用户有 SELECT 权限
3. **超时** - 调整 refresh_interval

## 项目结构

```
├── cdc_monitor.py      # 主程序
├── config.ini         # 配置文件
├── start_monitor.sh   # 启动脚本
└── README.md         # 本文档
```
