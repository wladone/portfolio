"""
Configuration management for the dashboard
"""

from typing import Dict, Any, Literal, Optional
from dataclasses import dataclass
import os


@dataclass
class AppConfig:
    """Application configuration"""
    title: str = "Global Electronics Lakehouse"
    icon: str = "🏪"
    layout: Literal["wide", "centered"] = "wide"
    initial_sidebar_state: Literal["expanded",
                                   "collapsed", "auto"] = "expanded"


@dataclass
class ThemeConfig:
    """Theme configuration"""
    primary_color: str = "#2563eb"  # More visible blue
    secondary_color: str = "#7c3aed"  # Purple
    accent_color: str = "#059669"  # Green
    success_color: str = "#16a34a"  # Darker green
    warning_color: str = "#d97706"  # Orange
    error_color: str = "#dc2626"  # Red

    background_light: str = "#ffffff"
    background_dark: str = "#0f172a"  # Darker, easier on eyes
    surface_light: str = "#f8fafc"
    surface_dark: str = "#1e293b"  # Lighter dark surface


@dataclass
class DataConfig:
    """Data processing configuration"""
    reorder_threshold: int = 40
    low_stock_threshold: int = 35
    healthy_stock_threshold: int = 70
    max_display_items: int = 100
    cache_timeout_seconds: int = 300


@dataclass
class DatabricksConfig:
    """Databricks connection configuration"""
    host: Optional[str] = None
    token: Optional[str] = None
    http_path: Optional[str] = None
    catalog: str = "dev_catalog"
    schema: str = "electronics"

    def __post_init__(self):
        """Load from environment variables if not provided"""
        if not self.host:
            self.host = os.getenv("DATABRICKS_HOST")
        if not self.token:
            self.token = os.getenv("DATABRICKS_TOKEN")
        if not self.http_path:
            self.http_path = os.getenv("DATABRICKS_HTTP_PATH")


class Config:
    """Central configuration manager"""

    def __init__(self):
        self.app = AppConfig()
        self.theme = ThemeConfig()
        self.data = DataConfig()
        self.databricks = DatabricksConfig()

    def get_current_theme_colors(self):
        """Get colors for current theme from session state"""
        import streamlit as st
        theme = getattr(st.session_state, 'theme', 'light')
        return self.get_theme_colors(theme)

    def get_theme_colors(self, theme: str = "light") -> Dict[str, str]:
        """Get theme-specific colors"""
        if theme == "dark":
            return {
                "primary": self.theme.primary_color,
                "secondary": self.theme.secondary_color,
                "accent": self.theme.accent_color,
                "success": self.theme.success_color,
                "warning": self.theme.warning_color,
                "error": self.theme.error_color,
                "background": self.theme.background_dark,
                "surface": self.theme.surface_dark,
            }
        else:
            return {
                "primary": self.theme.primary_color,
                "secondary": self.theme.secondary_color,
                "accent": self.theme.accent_color,
                "success": self.theme.success_color,
                "warning": self.theme.warning_color,
                "error": self.theme.error_color,
                "background": self.theme.background_light,
                "surface": self.theme.surface_light,
            }


# Global configuration instance
config = Config()
