"""
统一日志配置模块
提供项目内一致的日志初始化与管理
"""
from __future__ import annotations

import json
import logging
import logging.handlers
import os
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
        "detailed": "%(asctime)s | %(name)-20s | %(levelname)-8s | %(category)-12s | %(funcName)-15s | %(message)s",
        "simple": "%(asctime)s | %(levelname)-8s | %(category)-12s | %(message)s",
        "file": "%(asctime)s | %(name)s | %(levelname)s | %(category)s | %(pathname)s:%(lineno)d | %(funcName)s | %(message)s",
    }

    class _CategoryFilter(logging.Filter):
        def __init__(self, category: str):
            super().__init__()
            self._category = category

        def filter(self, record: logging.LogRecord) -> bool:
            record.category = self._category
            return True

    class _JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "time": self.formatTime(record, self.datefmt),
                "name": record.name,
                "level": record.levelname,
                "category": getattr(record, "category", "app"),
                "func": record.funcName,
                "line": record.lineno,
                "message": record.getMessage(),
            }
            if record.exc_info:
                payload["exc_info"] = self.formatException(record.exc_info)
            return json.dumps(payload, ensure_ascii=True)

    @staticmethod
    def resolve_log_dir(script_type: str, base_dir: Path, date_str: Optional[str] = None) -> Path:
        """返回统一日志目录：reports/logs/<script_type>/<YYYY-MM-DD>"""
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")
        return Path(base_dir) / "logs" / script_type / date_str

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

        formatter = LogConfig._get_formatter("detailed")
        if console:
            console_handler = logging.StreamHandler(LogConfig._safe_stdout())
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            console_handler.addFilter(LogConfig._CategoryFilter(file_prefix))
            logger.addHandler(console_handler)

        if file:
            file_path = log_dir / f"{file_prefix}.log"
            file_handler = logging.handlers.RotatingFileHandler(
                filename=str(file_path),
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(LogConfig._get_formatter("file"))
            file_handler.addFilter(LogConfig._CategoryFilter(file_prefix))
            logger.addHandler(file_handler)

        return logger

    @staticmethod
    def setup_root_logger(
        log_dir: Path,
        level: int = logging.INFO,
        script_name: str = "app",
        console: bool = True,
        base_dir: Optional[Path] = None,
        task_log_dir: Optional[Path] = None,
    ) -> None:
        """设置根 logger，同时创建 error/performance 日志"""
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        if task_log_dir:
            task_log_dir = Path(task_log_dir)
            task_log_dir.mkdir(parents=True, exist_ok=True)

        main_log_path = log_dir / f"{script_name}.log"
        error_log_path = log_dir / "error.log"
        perf_log_path = log_dir / "performance.log"
        global_log_dir = Path(base_dir) / "logs" if base_dir else None
        if global_log_dir:
            global_log_dir.mkdir(parents=True, exist_ok=True)
        global_error_path = global_log_dir / "error.log" if global_log_dir else None
        global_perf_path = global_log_dir / "performance.log" if global_log_dir else None

        handlers = []

        main_handler = logging.handlers.RotatingFileHandler(
            filename=str(main_log_path),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        main_handler.setLevel(level)
        main_handler.setFormatter(LogConfig._get_formatter("file"))
        main_handler.addFilter(LogConfig._CategoryFilter(script_name))
        handlers.append(main_handler)

        error_handler = logging.handlers.RotatingFileHandler(
            filename=str(error_log_path),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(LogConfig._get_formatter("file"))
        error_handler.addFilter(LogConfig._CategoryFilter(script_name))
        handlers.append(error_handler)

        if task_log_dir:
            task_main_handler = logging.handlers.RotatingFileHandler(
                filename=str(task_log_dir / f"{script_name}.log"),
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            task_main_handler.setLevel(level)
            task_main_handler.setFormatter(LogConfig._get_formatter("file"))
            task_main_handler.addFilter(LogConfig._CategoryFilter(script_name))
            handlers.append(task_main_handler)

            task_error_handler = logging.handlers.RotatingFileHandler(
                filename=str(task_log_dir / "error.log"),
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            task_error_handler.setLevel(logging.ERROR)
            task_error_handler.setFormatter(LogConfig._get_formatter("file"))
            task_error_handler.addFilter(LogConfig._CategoryFilter(script_name))
            handlers.append(task_error_handler)

        if global_error_path:
            global_error_handler = logging.handlers.RotatingFileHandler(
                filename=str(global_error_path),
                maxBytes=10 * 1024 * 1024,
                backupCount=10,
                encoding="utf-8",
            )
            global_error_handler.setLevel(logging.ERROR)
            global_error_handler.setFormatter(LogConfig._get_formatter("file"))
            global_error_handler.addFilter(LogConfig._CategoryFilter(script_name))
            handlers.append(global_error_handler)

        if console:
            console_handler = logging.StreamHandler(LogConfig._safe_stdout())
            console_handler.setLevel(level)
            console_handler.setFormatter(LogConfig._get_formatter("detailed"))
            console_handler.addFilter(LogConfig._CategoryFilter(script_name))
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
        perf_handler.setFormatter(LogConfig._get_formatter("file"))
        perf_handler.addFilter(LogConfig._CategoryFilter("performance"))
        performance_logger.addHandler(perf_handler)
        if task_log_dir:
            task_perf_handler = logging.handlers.RotatingFileHandler(
                filename=str(task_log_dir / "performance.log"),
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            task_perf_handler.setLevel(level)
            task_perf_handler.setFormatter(LogConfig._get_formatter("file"))
            task_perf_handler.addFilter(LogConfig._CategoryFilter("performance"))
            performance_logger.addHandler(task_perf_handler)
        if global_perf_path:
            global_perf_handler = logging.handlers.RotatingFileHandler(
                filename=str(global_perf_path),
                maxBytes=10 * 1024 * 1024,
                backupCount=10,
                encoding="utf-8",
            )
            global_perf_handler.setLevel(level)
            global_perf_handler.setFormatter(LogConfig._get_formatter("file"))
            global_perf_handler.addFilter(LogConfig._CategoryFilter("performance"))
            performance_logger.addHandler(global_perf_handler)

    @staticmethod
    def _get_formatter(style: str) -> logging.Formatter:
        """根据环境变量选择文本或 JSON 格式"""
        if os.getenv("LOG_FORMAT", "").lower() == "json":
            return LogConfig._JsonFormatter()
        return logging.Formatter(LogConfig.FORMATS[style])
