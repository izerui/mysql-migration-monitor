#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UI主题文件
包含UI样式、主题配置和样式常量
"""

from typing import Dict, Any

# 主题样式配置
THEME_STYLES = """
/* 基础主题样式 */
Screen {
    background: $surface;
    layout: vertical;
}

.status {
    dock: top;
    height: 1;
    background: $primary;
    color: $text;
}

.main-container {
    height: 1fr;
    width: 1fr;
}

/* 统计面板样式 */
.stats-panel {
    height: 8;
    border: solid $primary;
    margin: 1 1 0 1;
    padding: 1;
    background: $surface;
    border-title: "📊 统计信息";
}

/* 数据表格样式 */
.table-panel {
    height: 1fr;
    border: solid $primary;
    margin: 0 1 1 1;
    background: $surface;
    border-title: "📋 表格数据";
}

/* 数据表格内部样式 */
.data-table {
    height: 1fr;
    background: $surface;
}

.data-table > DataTable {
    background: $surface;
    scrollbar-background: $surface;
    scrollbar-color: $primary;
    scrollbar-corner-color: $surface;
    border: none;
}

DataTable > .datatable--header {
    background: $primary;
    color: $text;
    text-style: bold;
}

DataTable > .datatable--cursor {
    background: $accent 50%;
    color: $text;
}

DataTable > .datatable--hover {
    background: $primary 20%;
}

DataTable > .datatable--selected {
    background: $accent 30%;
    color: $text;
}

/* 状态颜色 */
.status-success {
    color: $success;
}

.status-warning {
    color: $warning;
}

.status-error {
    color: $error;
}

.status-info {
    color: $info;
}

/* 文本样式 */
.text-bold {
    text-style: bold;
}

.text-dim {
    color: $text-muted;
}

.text-bright {
    color: $text-bright;
}

/* 按钮样式 */
.button {
    dock: bottom;
    height: 1;
    background: $primary;
    color: $text;
}

/* 进度条样式 */
.progress-bar {
    height: 1;
    background: $surface;
    border: solid $primary;
}

.progress-bar-fill {
    background: $success;
}
"""

# 颜色主题配置
COLOR_THEMES = {
    "default": {
        "primary": "#0066cc",
        "secondary": "#6c757d",
        "success": "#28a745",
        "warning": "#ffc107",
        "error": "#dc3545",
        "info": "#17a2b8",
        "surface": "#f8f9fa",
        "background": "#ffffff",
        "text": "#212529",
        "text-muted": "#6c757d",
        "text-bright": "#000000",
        "accent": "#007bff",
        "border": "#dee2e6"
    },
    "dark": {
        "primary": "#0d6efd",
        "secondary": "#6c757d",
        "success": "#198754",
        "warning": "#ffc107",
        "error": "#dc3545",
        "info": "#0dcaf0",
        "surface": "#212529",
        "background": "#000000",
        "text": "#f8f9fa",
        "text-muted": "#6c757d",
        "text-bright": "#ffffff",
        "accent": "#0d6efd",
        "border": "#495057"
    },
    "green": {
        "primary": "#198754",
        "secondary": "#6c757d",
        "success": "#28a745",
        "warning": "#ffc107",
        "error": "#dc3545",
        "info": "#0dcaf0",
        "surface": "#f8f9fa",
        "background": "#ffffff",
        "text": "#212529",
        "text-muted": "#6c757d",
        "text-bright": "#000000",
        "accent": "#28a745",
        "border": "#dee2e6"
    }
}

# 图标配置
ICONS = {
    "success": "✅",
    "warning": "⚠️",
    "error": "❌",
    "info": "ℹ️",
    "loading": "🔄",
    "database": "🗄️",
    "table": "📋",
    "stats": "📊",
    "sync": "🔄",
    "pause": "⏸️",
    "play": "▶️",
    "stop": "⏹️"
}

# 状态映射
STATUS_MAPPING = {
    "consistent": {
        "icon": ICONS["success"],
        "color": "success"
    },
    "inconsistent": {
        "icon": ICONS["warning"],
        "color": "warning"
    },
    "error": {
        "icon": ICONS["error"],
        "color": "error"
    },
    "updating": {
        "icon": ICONS["loading"],
        "color": "info"
    },
    "unknown": {
        "icon": ICONS["info"],
        "color": "secondary"
    }
}

def get_color_scheme(theme_name: str = "default") -> Dict[str, str]:
    """获取指定主题的颜色方案"""
    return COLOR_THEMES.get(theme_name, COLOR_THEMES["default"])

def get_status_config(status: str) -> Dict[str, Any]:
    """获取状态配置"""
    return STATUS_MAPPING.get(status, STATUS_MAPPING["unknown"])

def get_icon(icon_name: str) -> str:
    """获取图标"""
    return ICONS.get(icon_name, "")

class ThemeManager:
    """主题管理器"""

    def __init__(self, theme_name: str = "default"):
        self.theme_name = theme_name
        self.color_scheme = get_color_scheme(theme_name)

    def get_css_styles(self) -> str:
        """获取CSS样式"""
        css = THEME_STYLES
        # 替换颜色变量
        for key, value in self.color_scheme.items():
            css = css.replace(f"${key}", value)
        return css

    def apply_theme(self, app):
        """应用主题到应用"""
        # 这里可以根据Textual的API来动态应用主题
        # 当前版本中，我们已经在CSS中定义了主题
        pass

    def switch_theme(self, theme_name: str):
        """切换主题"""
        self.theme_name = theme_name
        self.color_scheme = get_color_scheme(theme_name)

# 默认主题管理器实例
default_theme = ThemeManager()
