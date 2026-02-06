"""
日志工具模块
提供便捷的日志函数与耗时记录
"""
from __future__ import annotations

import functools
import inspect
import logging
import time
from contextlib import contextmanager
from typing import Any, Callable, Optional


def get_logger(name: str = __name__) -> logging.Logger:
    """获取 logger 实例"""
    return logging.getLogger(name)


def log_function_call(func: Callable) -> Callable:
    """装饰器：记录函数调用与耗时"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        perf_logger = logging.getLogger("performance")
        logger.debug(f"开始执行: {func.__name__}()")
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            logger.debug(f"完成执行: {func.__name__}()，耗时 {elapsed:.2f}s")
            if perf_logger.handlers:
                perf_logger.info(
                    f"函数耗时 | {func.__module__}.{func.__name__} | {elapsed:.2f}s",
                    stacklevel=2,
                )
            return result
        except Exception as exc:
            elapsed = time.time() - start_time
            logger.error(
                f"执行失败: {func.__name__}()，耗时 {elapsed:.2f}s，错误: {exc}",
                exc_info=True,
            )
            if perf_logger.handlers:
                perf_logger.info(
                    f"函数失败 | {func.__module__}.{func.__name__} | {elapsed:.2f}s",
                    stacklevel=2,
                )
            raise

    return wrapper


@contextmanager
def log_time(task_name: str, logger: Optional[logging.Logger] = None):
    """上下文管理器：记录代码块执行时间"""
    if logger is None:
        logger = logging.getLogger()
    perf_logger = logging.getLogger("performance")

    # 获取调用者信息（跳过 contextmanager 和当前函数）
    caller_frame = inspect.currentframe().f_back
    caller_file = caller_frame.f_code.co_filename
    caller_line = caller_frame.f_lineno
    caller_func = caller_frame.f_code.co_name

    logger.info(f"开始: {task_name}")
    start_time = time.time()

    try:
        yield
    except Exception as exc:
        elapsed = time.time() - start_time
        logger.error(f"失败: {task_name}，耗时 {elapsed:.2f}s，错误: {exc}", exc_info=True)
        if perf_logger.handlers:
            # 创建 LogRecord 手动设置 caller 信息
            record = perf_logger.makeRecord(
                perf_logger.name, logging.INFO, caller_file, caller_line,
                f"任务失败 | {task_name} | {elapsed:.2f}s",
                args=(), exc_info=None, func=caller_func
            )
            perf_logger.handle(record)
        raise
    else:
        elapsed = time.time() - start_time
        logger.info(f"完成: {task_name}，耗时 {elapsed:.2f}s")
        if perf_logger.handlers:
            # 创建 LogRecord 手动设置 caller 信息
            record = perf_logger.makeRecord(
                perf_logger.name, logging.INFO, caller_file, caller_line,
                f"任务耗时 | {task_name} | {elapsed:.2f}s",
                args=(), exc_info=None, func=caller_func
            )
            perf_logger.handle(record)


def log_data_summary(label: str, data: Any, logger: Optional[logging.Logger] = None) -> None:
    """记录数据摘要"""
    if logger is None:
        logger = logging.getLogger()

    if hasattr(data, "shape"):
        dtypes = data.dtypes if hasattr(data, "dtypes") else "N/A"
        logger.info(f"{label}: shape={data.shape}, dtype={dtypes}")
    elif isinstance(data, dict):
        logger.info(f"{label}: keys={list(data.keys())}, size={len(data)}")
    elif isinstance(data, (list, tuple)):
        elem_type = type(data[0]) if data else "empty"
        logger.info(f"{label}: length={len(data)}, type={elem_type}")
    else:
        logger.info(f"{label}: {type(data).__name__}")
