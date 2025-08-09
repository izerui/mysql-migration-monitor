#!/bin/bash
# MySQL vs MySQL 数据一致性监控启动脚本
# 支持源数据库到目标数据库的数据迁移监控和一致性对比

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的信息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 显示欢迎信息
show_welcome() {
    echo -e "${GREEN}"
    echo "╔══════════════════════════════════════════════════════════════════════════════╗"
    echo "║                                                                              ║"
    echo "║                    MySQL vs MySQL 数据一致性监控工具                        ║"
    echo "║                                                                              ║"
    echo "║         实时监控源数据库到目标数据库的数据迁移状态                        ║"
    echo "║         提供简洁清晰的配置格式和高效的数据一致性对比                      ║"
    echo "║                                                                              ║"
    echo "╚══════════════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# 检查Python版本
check_python() {
    print_info "检查Python环境..."

    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
        print_success "Python版本: $PYTHON_VERSION"
    else
        print_error "未找到Python3，请先安装Python3.8+"
        exit 1
    fi
}

# 检查并安装uv包管理器
check_uv() {
    print_info "检查uv包管理器..."

    if command -v uv &> /dev/null; then
        print_success "uv已安装: $(uv --version)"
    else
        print_warning "未找到uv，正在安装..."

        # 尝试使用pip安装
        if command -v pip3 &> /dev/null; then
            pip3 install uv
            print_success "uv安装成功"
        else
            print_error "未找到pip3，请手动安装uv: pip install uv"
            exit 1
        fi
    fi
}

# 检查配置文件
check_config() {
    local config_file="config.ini"
    local args=("$@")

    # 检查是否有自定义配置文件参数
    for ((i=0; i<${#args[@]}; i++)); do
        if [[ "${args[i]}" == "--config" ]]; then
            # 处理 --config file.ini 格式
            if [[ $((i+1)) -lt ${#args[@]} ]]; then
                config_file="${args[$((i+1))]}"
            fi
            break
        elif [[ "${args[i]}" == --config=* ]]; then
            # 处理 --config=file.ini 格式
            config_file="${args[i]#--config=}"
            break
        fi
    done

    print_info "检查配置文件..."

    if [[ -f "$config_file" ]]; then
        print_success "找到配置文件: $config_file"

        # 检查配置文件格式
        if grep -q "\[global\]" "$config_file" && grep -q "\[source\]" "$config_file" && grep -q "\[target\]" "$config_file"; then
            print_success "配置文件格式正确"
        else
            print_error "配置文件格式不正确"
            print_info "请确保$config_file包含以下配置:"
            echo ""
            echo "[global]"
            echo "databases = your_database_name"
            echo ""
            echo "[source]"
            echo "# 源数据库配置"
            echo "host = your_source_mysql_host"
            echo "port = 3306"
            echo "username = your_username"
            echo "password = your_password"
            echo ""
            echo "[target]"
            echo "# 目标数据库配置"
            echo "host = your_target_mysql_host"
            echo "port = 3306"
            echo "username = your_username"
            echo "password = your_password"
            echo ""
            echo "[monitor]"
            echo "refresh_interval = 3"
            echo ""
            exit 1
        fi
    else
        print_error "未找到配置文件: $config_file"
        print_info "请复制config.ini.example为$config_file并配置数据库信息"
        exit 1
    fi
}

# 安装项目依赖
install_dependencies() {
    print_info "安装项目依赖..."

    if [[ -f "pyproject.toml" ]]; then
        uv sync
        print_success "依赖安装完成"
    else
        print_warning "未找到pyproject.toml，使用pip安装..."
        uv pip install aiomysql rich configparser
        print_success "依赖安装完成"
    fi
}

# 跳过数据库连接测试
test_connections() {
    print_info "跳过数据库连接测试..."
    print_success "将在程序启动时进行连接验证"
}

# 启动监控程序
start_monitor() {
    local config_file="config.ini"
    local args=("$@")

    # 检查是否有自定义配置文件参数
    for ((i=0; i<${#args[@]}; i++)); do
        if [[ "${args[i]}" == "--config" ]]; then
            # 处理 --config file.ini 格式
            if [[ $((i+1)) -lt ${#args[@]} ]]; then
                config_file="${args[$((i+1))]}"
            fi
            break
        elif [[ "${args[i]}" == --config=* ]]; then
            # 处理 --config=file.ini 格式
            config_file="${args[i]#--config=}"
            break
        fi
    done

    print_info "启动MySQL vs MySQL监控程序..."
    echo ""

    # 显示启动参数
    print_info "启动参数:"
    echo "  配置文件: $config_file"
    echo "  监控模式: Source MySQL → Target MySQL"
    echo ""

    # 启动监控程序
    uv run cdc_monitor.py "$@"
}

# 显示使用帮助
show_help() {
    echo ""
    echo "使用方法:"
    echo "  ./start_mysql_monitor.sh                    # 使用默认配置启动"
    echo "  ./start_mysql_monitor.sh --databases db1,db2 # 监控指定数据库"
    echo "  ./start_mysql_monitor.sh --config my.ini    # 使用自定义配置"
    echo ""
    echo "快捷键:"
    echo "  q 或 Ctrl+C  - 退出程序"
    echo "  r            - 手动刷新"
    echo "  p            - 暂停/继续"
    echo "  s            - 切换排序方式"
    echo "  f            - 切换过滤模式"
    echo ""
}

# 主程序
main() {
    show_welcome

    # 检查命令行参数
    case "$1" in
        -h|--help)
            show_help
            exit 0
            ;;
    esac

    # 执行检查步骤
    check_python
    check_uv
    check_config "$@"
    install_dependencies
    # 跳过测试连接，直接启动
    # test_connections

    # 启动监控
    start_monitor "$@"
}

# 执行主程序
main "$@"
