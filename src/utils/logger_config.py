"""
统一日志配置模块
提供项目内一致的日志初始化与管理
"""
from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


class LogConfig:
    """日志配置类"""

    LEVEL_DEBUG = logging.DEBUG
    LEVEL_INFO = logging.INFO
    LEVEL_WARNING = logging.WARNING
    LEVEL_ERROR = logging.ERROR
    LEVEL_CRITICAL = logging.CRITICAL

    FORMATS = {
        "detailed": "%(asctime)s | %(name)-20s | %(levelname)-8s | %(funcName)-15s | %(message)s",
        "simple": "%(asctime)s | %(levelname)-8s | %(message)s",
        "file": "%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s",
    }

    @staticmethod
    def resolve_log_dir(script_type: str, base_dir: Path) -> Path:
        """返回统一日志目录：reports/logs/<script_type>"""
        return Path(base_dir) / "logs" / script_type

    @staticmethod
    def _safe_stdout():
        """返回安全的 stdout，避免 Windows 控制台编码报错"""
        try:
            return sys.stdout
        except Exception:
            return None

    @staticmethod
    def setup_logger(
        name: str,
        log_dir: Path,
        level: int = logging.INFO,
        console: bool = True,
        file: bool = True,
        file_prefix: str = "app",
    ) -> logging.Logger:
        """设置单个 logger 实例"""
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.handlers.clear()
        logger.propagate = False

        log_dir.mkdir(parents=True, exist_ok=True)

        if console:
            console_handler = logging.StreamHandler(LogConfig._safe_stdout())
            console_handler.setLevel(level)
            console_handler.setFormatter(logging.Formatter(LogConfig.FORMATS["detailed"]))
            logger.addHandler(console_handler)

        if file:
            date_str = datetime.now().strftime("%Y-%m-%d")
            file_path = log_dir / f"{file_prefix}_{date_str}.log"
            file_handler = logging.handlers.RotatingFileHandler(
                filename=str(file_path),
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(logging.Formatter(LogConfig.FORMATS["file"]))
            logger.addHandler(file_handler)

        return logger

    @staticmethod
    def setup_root_logger(
        log_dir: Path,
        level: int = logging.INFO,
        script_name: str = "app",
        console: bool = True,
    ) -> None:
        """设置根 logger，同时创建 error/performance 日志"""
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        date_str = datetime.now().strftime("%Y-%m-%d")
        main_log_path = log_dir / f"{script_name}_{date_str}.log"
        error_log_path = log_dir / "error.log"
        perf_log_path = log_dir / "performance.log"

        handlers = []

        main_handler = logging.handlers.RotatingFileHandler(
            filename=str(main_log_path),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        main_handler.setLevel(level)
        main_handler.setFormatter(logging.Formatter(LogConfig.FORMATS["file"]))
        handlers.append(main_handler)

        error_handler = logging.handlers.RotatingFileHandler(
            filename=str(error_log_path),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter(LogConfig.FORMATS["file"]))
        handlers.append(error_handler)

        if console:
            console_handler = logging.StreamHandler(LogConfig._safe_stdout())
            console_handler.setLevel(level)
            console_handler.setFormatter(logging.Formatter(LogConfig.FORMATS["detailed"]))
            handlers.append(console_handler)

        logging.basicConfig(level=level, handlers=handlers, force=True)
        logging.captureWarnings(True)

        performance_logger = logging.getLogger("performance")
        performance_logger.handlers.clear()
        performance_logger.setLevel(level)
        performance_logger.propagate = False
        perf_handler = logging.handlers.RotatingFileHandler(
            filename=str(perf_log_path),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        perf_handler.setLevel(level)
        perf_handler.setFormatter(logging.Formatter(LogConfig.FORMATS["file"]))
        performance_logger.addHandler(perf_handler)
