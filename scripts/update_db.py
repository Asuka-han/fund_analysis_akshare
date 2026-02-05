#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
update_db.py - 仅更新数据库数据，不进行分析

功能：
1. 抓取指定基金和指数的历史数据
2. 更新到数据库（支持追加和替换模式）
3. 支持备份原数据库
4. 支持指定日期范围

使用示例：
  python scripts/update_db.py --funds 000001.OF 003095.OF --start-date 2021-01-01
  python scripts/update_db.py --indices 000300 --force-replace
  python scripts/update_db.py --all --backup
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
import shutil

# 保证脚本独立运行时能找到项目内模块
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
for candidate in (PROJECT_ROOT, SRC_DIR):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from src.utils.runtime_env import add_project_paths

# 统一处理冻结/普通环境下的路径与导入
REPO_ROOT, STORAGE_ROOT = add_project_paths()

from src.data_fetch.fund_fetcher import FundDataFetcher
from src.data_fetch.index_fetcher import IndexDataFetcher
from src.utils.database import fund_db
from src.utils.fund_code_manager import fund_code_manager
from src.utils.output_manager import get_output_manager
from src.utils.logger_config import LogConfig
from src.utils.logger import get_logger
import config

logger = get_logger(__name__)


def configure_logging(log_dir: Path, verbose: bool = False):
    """配置日志输出到控制台和文件"""
    level = logging.DEBUG if verbose else logging.INFO
    LogConfig.setup_root_logger(log_dir=log_dir, level=level, script_name="update_db")


def backup_database(backup_name: str = None):
    """备份数据库文件（在 WSL/挂载卷上兼容）"""
    db_path = Path(config.DATABASE_PATH)
    if not db_path.exists():
        logger.warning("数据库文件不存在，无需备份")
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if backup_name:
        backup_path = Path(config.DATA_DIR) / backup_name
    else:
        backup_path = Path(config.DATA_DIR) / f"fund_data_{timestamp}.db.bak"

    try:
        # 确保备份目录存在
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            # 优先保留元数据；若在 CIFS/WSL 上报错则降级
            shutil.copy2(db_path, backup_path)
        except Exception as copy2_err:
            logger.warning(f"copy2 失败，降级为普通复制: {copy2_err}")
            shutil.copyfile(db_path, backup_path)
        logger.info(f"数据库已备份到: {backup_path}")
        return str(backup_path)
    except PermissionError as e:
        logger.error(f"备份数据库失败：权限不足 - {e}")
        logger.info("尝试使用不同的备份路径...")
        # 尝试备份到用户目录或其他位置
        try:
            user_backup_path = Path.home() / f"fund_data_backup_{timestamp}.db"
            try:
                shutil.copy2(db_path, user_backup_path)
            except Exception as copy2_err:
                logger.warning(f"copy2 失败，降级为普通复制: {copy2_err}")
                shutil.copyfile(db_path, user_backup_path)
            logger.info(f"数据库已备份到: {user_backup_path}")
            return str(user_backup_path)
        except Exception as fallback_error:
            logger.error(f"备用备份也失败了: {fallback_error}")
            return None
    except Exception as e:
        logger.error(f"备份数据库失败: {e}")
        return None


def fetch_funds_data(fund_codes, start_date=None, end_date=None, force_replace=False):
    """获取并保存基金数据"""
    if not fund_codes:
        logger.info("没有指定基金代码，跳过基金数据获取")
        return 0
    
    fetcher = FundDataFetcher()
    total_inserted = 0
    
    for fund_code in fund_codes:
        try:
            logger.info(f"获取基金数据: {fund_code}")
            
            # 获取数据
            fund_data = fetcher.fetch_fund_data(
                fund_code, 
                start_date=start_date, 
                end_date=end_date
            )
            
            if fund_data is None or fund_data.empty:
                logger.warning(f"基金 {fund_code} 没有获取到数据")
                continue
            
            # 如果强制替换，先删除已有数据
            if force_replace:
                logger.info(f"强制替换模式: 删除基金 {fund_code} 的已有数据")
                # 这里可以添加删除逻辑，但通常数据库的插入已包含去重
            
            # 保存到数据库
            display_code = fund_code_manager.to_display_format(fund_code)
            inserted = fund_db.insert_fund_daily_data(display_code, fund_data)
            total_inserted += inserted
            
            logger.info(f"基金 {fund_code} 数据保存完成，新增 {inserted} 条记录")
            
        except Exception as e:
            logger.error(f"处理基金 {fund_code} 时出错: {e}")
    
    return total_inserted


def fetch_indices_data(index_codes, start_date=None, end_date=None, force_replace=False):
    """获取并保存指数数据"""
    if not index_codes:
        logger.info("没有指定指数代码，跳过指数数据获取")
        return 0
    
    fetcher = IndexDataFetcher()
    total_inserted = 0
    
    for index_code in index_codes:
        try:
            logger.info(f"获取指数数据: {index_code}")
            
            # 获取数据（复合指数在fetch_index_data内部处理）
            index_data = fetcher.fetch_index_data(
                index_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if index_data is None or index_data.empty:
                logger.warning(f"指数 {index_code} 没有获取到数据")
                continue
            
            # 保存到数据库
            inserted = fund_db.insert_index_daily_data(index_code, index_data)
            total_inserted += inserted
            
            logger.info(f"指数 {index_code} 数据保存完成，新增 {inserted} 条记录")
            
        except Exception as e:
            logger.error(f"处理指数 {index_code} 时出错: {e}")
    
    return total_inserted


def create_update_report(args, fund_count: int, index_count: int, backup_path: str,
                        start_time: datetime, end_time: datetime, duration):
    """生成数据库更新报告（Markdown）"""
    lines = [
        "# 数据库更新报告",
        "",
        f"- 启动时间: {start_time}",
        f"- 完成时间: {end_time}",
        f"- 总耗时: {duration}",
        f"- 数据库: {config.DATABASE_PATH}"
    ]

    if args.backup and backup_path:
        lines.append(f"- 备份文件: {backup_path}")

    lines.extend([
        "\n## 更新范围",
        f"- 基金: {args.funds if args.funds else '未指定'}",
        f"- 指数: {args.indices if args.indices else '未指定'}",
        f"- 开始日期: {args.start_date}",
        f"- 结束日期: {args.end_date}",
        f"- 年限参数: {args.years} 年"
    ])

    lines.extend([
        "\n## 结果统计",
        f"- 新增基金记录: {fund_count}",
        f"- 新增指数记录: {index_count}",
        f"- 模式: {'强制替换' if args.force_replace else '追加'}"
    ])

    lines.append("\n## 运行参数")
    lines.append(f"- 试运行: 否")
    lines.append(f"- 备份: {'是' if args.backup else '否'}")

    lines.append("\n⏰ 生成时间: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return "\n".join(lines)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='仅更新数据库数据，不进行分析')
    
    # 数据选择参数
    parser.add_argument('--funds', nargs='+', help='基金代码列表（多个用空格分隔）')
    parser.add_argument('--indices', nargs='+', help='指数代码列表（多个用空格分隔）')
    parser.add_argument('--all', action='store_true', help='更新所有配置中的基金和指数')
    
    # 日期参数
    parser.add_argument('--start-date', help='开始日期（格式：YYYY-MM-DD）')
    parser.add_argument('--end-date', help='结束日期（格式：YYYY-MM-DD）')
    parser.add_argument('--years', type=int, default=3,
                       help='获取最近多少年的数据（默认：3年）')
    
    # 操作参数
    parser.add_argument('--force-replace', action='store_true',
                       help='强制替换已有数据（默认：追加）')
    parser.add_argument('--backup', action='store_true',
                       help='更新前备份数据库')
    parser.add_argument('--backup-name', help='指定备份文件名')
    parser.add_argument('--verbose', action='store_true', help='显示详细日志')
    
    args = parser.parse_args()

    start_time = datetime.now()
    backup_path = None

    # 初始化输出管理器
    output_manager = get_output_manager('update_db', base_dir=config.REPORTS_DIR, use_timestamp=True)

    # 配置日志到统一日志目录
    configure_logging(LogConfig.resolve_log_dir('update_db', config.REPORTS_DIR), args.verbose)

    logger.info("开始更新数据库")
    logger.info("=" * 60)
    
    # 处理日期参数
    if not args.start_date and args.years:
        args.start_date = (datetime.now() - timedelta(days=365 * args.years)).strftime('%Y-%m-%d')
    if not args.end_date:
        args.end_date = datetime.now().strftime('%Y-%m-%d')
    
    # 处理数据选择
    if args.all:
        args.funds = config.FUND_CODES
        # 获取实际指数代码（支持带后缀）
        if hasattr(config, 'get_actual_benchmark_codes'):
            args.indices = config.get_actual_benchmark_codes()
        else:
            args.indices = config.BENCHMARK_IDS
        logger.info("将更新所有基金: %s", args.funds)
        logger.info("将更新所有指数: %s", args.indices)
    elif not args.funds and not args.indices:
        logger.error("请指定要更新的基金或指数，或使用 --all 参数")
        parser.print_help()
        return 1
    
    # 备份数据库
    if args.backup:
        backup_path = backup_database(args.backup_name)
        if not backup_path:
            logger.warning("备份失败，等待用户确认是否继续")
            print("备份失败，是否继续？[y/N]", end=' ')
            if input().lower() != 'y':
                return 1

    # 更新基金数据
    fund_count = 0
    if args.funds:
        logger.info("更新基金数据（%s 只）", len(args.funds))
        fund_count = fetch_funds_data(
            args.funds,
            args.start_date,
            args.end_date,
            args.force_replace
        )
    
    # 更新指数数据
    index_count = 0
    if args.indices:
        logger.info("更新指数数据（%s 个）", len(args.indices))
        index_count = fetch_indices_data(
            args.indices,
            args.start_date,
            args.end_date,
            args.force_replace
        )
    
    # 输出总结
    logger.info("=" * 60)
    logger.info("数据库更新完成")
    logger.info("基金数据: %s 条新增记录", fund_count)
    logger.info("指数数据: %s 条新增记录", index_count)
    logger.info("数据库位置: %s", config.DATABASE_PATH)
    
    if args.backup and backup_path:
        logger.info("备份文件: %s", backup_path)

    # 生成导入报告（Markdown）
    report_path = output_manager.get_path('reports', 'data_import_report.md')
    end_time = datetime.now()
    duration = end_time - start_time
    report_content = create_update_report(
        args,
        fund_count,
        index_count,
        backup_path,
        start_time,
        end_time,
        duration
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_content)
    logger.info(f"数据导入报告已保存: {report_path}")
    logger.info("导入报告: %s", report_path)
    
    # 输出目录摘要
    output_manager.print_summary()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())