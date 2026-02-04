#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Excel 数据导入脚本
将 Excel 格式的基金日频数据导入到数据库

使用示例：
测试文件在fund_analysis_project\data\test_fund_data.xlsx
python scripts/import_excel_to_db.py --input 路径 --dry-run
"""

import sys
import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import sqlite3
import argparse
import logging
from typing import Dict, List, Tuple, Optional, Any
import io

# 保证脚本独立运行时能找到项目内模块
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
for candidate in (PROJECT_ROOT, SRC_DIR):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from src.utils.runtime_env import add_project_paths

# 统一处理冻结/普通环境下的路径与导入
REPO_ROOT, STORAGE_ROOT = add_project_paths()

from src.utils.database import fund_db
from src.utils.fund_code_manager import fund_code_manager
from src.utils.output_manager import get_output_manager
import config

# 日志配置，稍后根据输出目录绑定文件
logger = logging.getLogger(__name__)


def configure_logging(log_path: Path, verbose: bool = False):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    log_path.parent.mkdir(parents=True, exist_ok=True)
    safe_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(safe_stdout),
            logging.FileHandler(str(log_path), encoding='utf-8')
        ],
        force=True
    )


class ExcelImporter:
    """Excel 数据导入器"""
    
    def __init__(self, db_path: str = None):
        """
        初始化导入器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = Path(db_path or config.DATABASE_PATH)
        self.connection = None
        self.cursor = None
        
        # 列名映射（支持多种可能的大小写和名称）
        self.column_mappings = {
            'fund_id': ['fund_id', 'fund_code', '基金代码', '代码'],
            'date': ['date', '交易日期', '日期', 'datetime'],
            'nav': ['nav', 'unit_nav', '单位净值', '净值'],
            'cumulative_nav': ['cumulative_nav', '累计净值', '累计nav', 'cum_nav'],
            'daily_growth': ['daily_growth', '日增长率', '增长率', 'return'],
            'net_assets': ['net_assets', '资产净值', '规模', 'assets']
        }
    
    def connect(self):
        """连接到数据库"""
        self.connection = sqlite3.connect(str(self.db_path))
        self.cursor = self.connection.cursor()
    
    def disconnect(self):
        """断开数据库连接"""
        if self.connection:
            self.connection.close()
    
    def detect_column_names(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        检测 DataFrame 中的列名，映射到标准列名
        
        Args:
            df: 输入的 DataFrame
            
        Returns:
            映射字典 {标准列名: 实际列名}
        """
        mapping = {}
        
        # 获取 DataFrame 的实际列名（小写处理）
        actual_columns = {col.lower().strip(): col for col in df.columns}
        
        for std_name, possible_names in self.column_mappings.items():
            for possible in possible_names:
                possible_lower = possible.lower()
                if possible_lower in actual_columns:
                    mapping[std_name] = actual_columns[possible_lower]
                    break
        
        return mapping
    
    def validate_excel_file(self, file_path: Path) -> Tuple[bool, str]:
        """
        验证 Excel 文件
        
        Args:
            file_path: Excel 文件路径
            
        Returns:
            (是否有效, 错误信息)
        """
        if not file_path.exists():
            return False, f"文件不存在: {file_path}"
        
        if file_path.suffix.lower() not in ['.xlsx', '.xls']:
            return False, f"不支持的文件格式: {file_path.suffix}"
        
        try:
            # 尝试读取 Excel 文件
            excel_file = pd.ExcelFile(file_path)
            
            # 检查是否有 sheet
            if len(excel_file.sheet_names) == 0:
                return False, "Excel 文件中没有 Sheet"
            
            return True, "文件验证通过"
            
        except Exception as e:
            return False, f"读取 Excel 文件失败: {str(e)}"
    
    def read_excel_data(self, file_path: Path, sheet_name: str = None) -> Optional[pd.DataFrame]:
        """
        读取 Excel 数据
        
        Args:
            file_path: Excel 文件路径
            sheet_name: Sheet 名称，如果为 None 则使用第一个 Sheet
            
        Returns:
            包含数据的 DataFrame，失败返回 None
        """
        try:
            excel_file = pd.ExcelFile(file_path)
            
            # 确定要读取的 Sheet
            if sheet_name:
                if sheet_name not in excel_file.sheet_names:
                    logger.warning(f"Sheet '{sheet_name}' 不存在，使用第一个 Sheet")
                    sheet_name = excel_file.sheet_names[0]
            else:
                # 尝试查找 daily_nav Sheet，否则使用第一个 Sheet
                if 'daily_nav' in excel_file.sheet_names:
                    sheet_name = 'daily_nav'
                else:
                    sheet_name = excel_file.sheet_names[0]
                    logger.info(f"使用 Sheet: {sheet_name}")
            
            # 读取数据
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            
            if df.empty:
                logger.error(f"Sheet '{sheet_name}' 中没有数据")
                return None
            
            logger.info(f"读取成功: {file_path.name} - Sheet: {sheet_name}")
            logger.info(f"数据形状: {df.shape}, 列: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"读取 Excel 数据失败: {e}")
            return None
    
    def preprocess_data(self, df: pd.DataFrame, mapping: Dict[str, str]) -> Tuple[pd.DataFrame, List[str]]:
        """
        预处理数据：重命名列、验证数据、转换类型
        
        Args:
            df: 原始 DataFrame
            mapping: 列名映射
            
        Returns:
            (处理后的 DataFrame, 错误信息列表)
        """
        errors = []
        processed_df = df.copy()
        
        # 1. 重命名列
        rename_dict = {actual: std for std, actual in mapping.items()}
        processed_df.rename(columns=rename_dict, inplace=True)
        
        # 2. 检查必需列
        required_columns = ['fund_id', 'date']
        missing_columns = [col for col in required_columns if col not in processed_df.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")
            return processed_df, errors
        
        # 3. 检查至少有一个净值列
        nav_columns = ['nav', 'cumulative_nav']
        if not any(col in processed_df.columns for col in nav_columns):
            errors.append("必须至少提供 nav 或 cumulative_nav 列之一")
            return processed_df, errors
        
        # 4. 处理基金代码格式
        if 'fund_id' in processed_df.columns:
            # 标准化基金代码格式（统一为数据库格式）
            processed_df['fund_id'] = processed_df['fund_id'].astype(str).apply(
                lambda x: fund_code_manager.to_database_format(x)
            )
        
        # 5. 处理日期格式
        if 'date' in processed_df.columns:
            try:
                processed_df['date'] = pd.to_datetime(processed_df['date'], errors='coerce')
                # 检查是否有无效日期
                invalid_dates = processed_df['date'].isna().sum()
                if invalid_dates > 0:
                    errors.append(f"发现 {invalid_dates} 个无效日期")
            except Exception as e:
                errors.append(f"日期处理失败: {e}")
        
        # 6. 处理净值数据
        for col in ['nav', 'cumulative_nav', 'daily_growth', 'net_assets']:
            if col in processed_df.columns:
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')
        
        # 7. 处理 nav 和 cumulative_nav 的转换
        self._handle_nav_conversion(processed_df, errors)
        
        # 8. 排序和去重
        processed_df = processed_df.sort_values(['fund_id', 'date'])
        
        return processed_df, errors
    
    def _handle_nav_conversion(self, df: pd.DataFrame, errors: List[str]):
        """
        处理 NAV 和 Cumulative NAV 之间的转换
        
        Args:
            df: 处理中的 DataFrame
            errors: 错误列表
        """
        try:
            # 如果只有 cumulative_nav 没有 nav，尝试转换
            if 'cumulative_nav' in df.columns and 'nav' not in df.columns:
                logger.info("只有 cumulative_nav 数据，尝试转换为 nav")
                
                # 简单起见，假设二者相同（对于普通基金）
                df['nav'] = df['cumulative_nav']
                
            # 如果只有 nav 没有 cumulative_nav，尝试转换
            elif 'nav' in df.columns and 'cumulative_nav' not in df.columns:
                logger.info("只有 nav 数据，尝试转换为 cumulative_nav")
                
                # 对于每个基金，计算累计净值（假设分红再投资）
                # 注意：这只是近似计算，实际累计净值需要考虑分红
                df['cumulative_nav'] = np.nan
                
                # 按基金分组处理
                for fund_id, group in df.groupby('fund_id'):
                    # 按日期排序
                    group_sorted = group.sort_values('date')
                    
                    # 如果第一个净值大于1，可能是累计净值
                    if group_sorted['nav'].iloc[0] > 1.5:  # 阈值可调整
                        df.loc[group_sorted.index, 'cumulative_nav'] = group_sorted['nav']
                    else:
                        # 否则假设是单位净值，无法准确计算累计净值
                        df.loc[group_sorted.index, 'cumulative_nav'] = group_sorted['nav']
                        errors.append(f"基金 {fund_id}: 无法准确计算累计净值，使用单位净值替代")
                        
            logger.info("净值转换处理完成")
            
        except Exception as e:
            errors.append(f"净值转换失败: {e}")
    
    def check_duplicates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        检查重复数据
        
        Args:
            df: 处理后的 DataFrame
            
        Returns:
            重复数据统计信息
        """
        stats = {
            'total_rows': len(df),
            'duplicate_count': 0,
            'unique_count': 0,
            'duplicate_examples': []
        }
        
        if df.empty:
            return stats
        
        # 检查重复行（基于 fund_id 和 date）
        duplicate_mask = df.duplicated(subset=['fund_id', 'date'], keep=False)
        duplicate_rows = df[duplicate_mask]
        
        stats['duplicate_count'] = len(duplicate_rows)
        stats['unique_count'] = stats['total_rows'] - stats['duplicate_count']
        
        # 收集重复示例（前5个）
        if not duplicate_rows.empty:
            duplicate_groups = duplicate_rows.groupby(['fund_id', 'date']).head(2)
            for _, row in duplicate_groups.head(5).iterrows():
                stats['duplicate_examples'].append({
                    'fund_id': row['fund_id'],
                    'date': row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else row['date'],
                    'nav': row.get('nav'),
                    'cumulative_nav': row.get('cumulative_nav')
                })
        
        return stats
    
    def import_to_database(self, df: pd.DataFrame, mode: str = 'newer-wins', 
                          dry_run: bool = False) -> Dict[str, Any]:
        """
        将数据导入数据库
        
        Args:
            df: 处理后的 DataFrame
            mode: 重复处理模式 ('newer-wins', 'skip', 'replace')
            dry_run: 试运行模式（不实际写入）
            
        Returns:
            导入统计信息
        """
        stats = {
            'total_rows': len(df),
            'successful': 0,
            'skipped': 0,
            'failed': 0,
            'errors': [],
            'funds_affected': set(),
            'date_range': {}
        }
        
        if df.empty:
            stats['errors'].append("没有数据可导入")
            return stats
        
        try:
            self.connect()
            
            # 按基金分组处理
            for fund_id, group in df.groupby('fund_id'):
                logger.info(f"处理基金: {fund_id} ({len(group)} 行)")
                
                # 按日期排序
                group_sorted = group.sort_values('date')
                
                # 记录日期范围
                if fund_id not in stats['date_range']:
                    stats['date_range'][fund_id] = {
                        'start': group_sorted['date'].min().strftime('%Y-%m-%d'),
                        'end': group_sorted['date'].max().strftime('%Y-%m-%d'),
                        'days': len(group_sorted)
                    }
                
                stats['funds_affected'].add(fund_id)
                
                # 逐行导入
                for idx, row in group_sorted.iterrows():
                    try:
                        result = self._import_single_row(row, mode, dry_run)
                        
                        if result == 'success':
                            stats['successful'] += 1
                        elif result == 'skipped':
                            stats['skipped'] += 1
                        elif result == 'failed':
                            stats['failed'] += 1
                            
                    except Exception as e:
                        stats['failed'] += 1
                        stats['errors'].append(f"第 {idx} 行导入失败: {e}")
            
            if not dry_run:
                self.connection.commit()
                logger.info("数据已提交到数据库")
            
            return stats
            
        except Exception as e:
            stats['errors'].append(f"数据库导入失败: {e}")
            return stats
            
        finally:
            self.disconnect()
    
    def _import_single_row(self, row: pd.Series, mode: str, dry_run: bool) -> str:
        """
        导入单行数据
        
        Args:
            row: 数据行
            mode: 重复处理模式
            dry_run: 试运行模式
            
        Returns:
            结果状态 ('success', 'skipped', 'failed')
        """
        try:
            # 准备数据
            fund_id = row['fund_id']
            date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])
            
            nav = row.get('nav')
            cumulative_nav = row.get('cumulative_nav')
            daily_growth = row.get('daily_growth')
            
            # 检查数据是否存在
            self.cursor.execute(
                "SELECT 1 FROM fund_daily_data WHERE fund_id = ? AND date = ?",
                (fund_id, date_str)
            )
            exists = self.cursor.fetchone() is not None
            
            # 根据模式决定操作
            if exists:
                if mode == 'skip':
                    logger.debug(f"跳过重复数据: {fund_id} - {date_str}")
                    return 'skipped'
                elif mode in ['newer-wins', 'replace']:
                    # 更新现有数据
                    if not dry_run:
                        self.cursor.execute(
                            """
                            UPDATE fund_daily_data 
                            SET nav = ?, cumulative_nav = ?, daily_growth = ?
                            WHERE fund_id = ? AND date = ?
                            """,
                            (nav, cumulative_nav, daily_growth, fund_id, date_str)
                        )
                    logger.debug(f"更新数据: {fund_id} - {date_str}")
                    return 'success'
            else:
                # 插入新数据
                if not dry_run:
                    self.cursor.execute(
                        """
                        INSERT INTO fund_daily_data 
                        (fund_id, date, nav, cumulative_nav, daily_growth)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (fund_id, date_str, nav, cumulative_nav, daily_growth)
                    )
                logger.debug(f"插入新数据: {fund_id} - {date_str}")
                return 'success'
                
        except Exception as e:
            logger.error(f"导入单行数据失败: {e}")
            return 'failed'
    
    def generate_report(self, stats: Dict[str, Any], import_mode: str, 
                       dry_run: bool = False) -> str:
        """
        生成导入报告
        
        Args:
            stats: 导入统计信息
            import_mode: 导入模式
            dry_run: 是否为试运行
            
        Returns:
            报告字符串
        """
        report = []
        report.append("=" * 60)
        report.append("Excel 数据导入报告")
        report.append("=" * 60)
        
        if dry_run:
            report.append("📝 试运行模式（未实际写入数据库）")
        
        report.append(f"📊 导入统计")
        report.append(f"  总行数: {stats['total_rows']}")
        report.append(f"  成功导入: {stats['successful']}")
        report.append(f"  跳过: {stats['skipped']}")
        report.append(f"  失败: {stats['failed']}")
        
        report.append(f"\n🎯 处理模式: {import_mode}")
        if import_mode == 'newer-wins':
            report.append("  （重复数据将被新数据覆盖）")
        elif import_mode == 'skip':
            report.append("  （重复数据将被跳过）")
        elif import_mode == 'replace':
            report.append("  （重复数据将被替换）")
        
        if stats['funds_affected']:
            report.append(f"\n📈 受影响基金 ({len(stats['funds_affected'])} 只):")
            for fund_id in sorted(stats['funds_affected']):
                date_info = stats['date_range'].get(fund_id, {})
                start = date_info.get('start', 'N/A')
                end = date_info.get('end', 'N/A')
                days = date_info.get('days', 0)
                report.append(f"  - {fund_id}: {start} 到 {end} ({days} 天)")
        
        if stats.get('duplicate_stats'):
            dup_stats = stats['duplicate_stats']
            report.append(f"\n🔄 重复数据检查:")
            report.append(f"  总行数: {dup_stats['total_rows']}")
            report.append(f"  重复行数: {dup_stats['duplicate_count']}")
            report.append(f"  唯一行数: {dup_stats['unique_count']}")
            
            if dup_stats['duplicate_examples']:
                report.append(f"  重复示例（前5个）:")
                for example in dup_stats['duplicate_examples'][:3]:
                    report.append(f"    - {example['fund_id']} | {example['date']} | NAV: {example.get('nav')}")
        
        if stats['errors']:
            report.append(f"\n❌ 错误信息 ({len(stats['errors'])} 个):")
            for i, error in enumerate(stats['errors'][:5], 1):  # 只显示前5个错误
                report.append(f"  {i}. {error}")
            if len(stats['errors']) > 5:
                report.append(f"  ... 还有 {len(stats['errors']) - 5} 个错误")
        
        report.append(f"\n⏰ 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 60)
        
        return "\n".join(report)
    
    def save_report(self, report_content: str, output_path: Optional[Path] = None, output_manager=None):
        """
        保存报告到文件
        
        Args:
            report_content: 报告内容
            output_path: 输出文件路径
        """
        if output_path is None:
            if output_manager:
                output_path = output_manager.get_path('reports', 'data_import_report.md')
            else:
                output_path = Path("reports") / f"import_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        output_path.parent.mkdir(exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"报告已保存: {output_path}")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Excel 数据导入工具')
    
    parser.add_argument('--input', '-i', required=True,
                       help='Excel 文件路径')
    
    parser.add_argument('--sheet', '-s',
                       help='Sheet 名称（默认：daily_nav 或第一个 Sheet）')
    
    parser.add_argument('--mode', '-m', choices=['newer-wins', 'skip', 'replace'],
                       default='newer-wins',
                       help='重复数据处理模式（默认：newer-wins）')
    
    parser.add_argument('--dry-run', '-d', action='store_true',
                       help='试运行模式（不实际写入数据库）')
    
    parser.add_argument('--output', '-o',
                       help='报告输出文件路径')
    
    parser.add_argument('--db-path',
                       default=str(config.DATABASE_PATH),
                       help='数据库文件路径（默认：config.DATABASE_PATH）')

    parser.add_argument('--verbose', action='store_true', help='显示详细日志')
    
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_arguments()

    start_time = datetime.now()
    output_manager = get_output_manager('import_excel_to_db', base_dir=config.REPORTS_DIR, use_timestamp=True)
    configure_logging(output_manager.get_path('logs', 'import_excel_to_db.log'), args.verbose)
    
    # 初始化导入器
    importer = ExcelImporter(db_path=args.db_path)
    
    # 验证文件
    file_path = Path(args.input)
    is_valid, message = importer.validate_excel_file(file_path)
    if not is_valid:
        logger.error(f"文件验证失败: {message}")
        sys.exit(1)
    
    logger.info(f"开始导入: {file_path.name}")
    logger.info(f"模式: {args.mode}, 试运行: {args.dry_run}")
    
    # 读取 Excel 数据
    df = importer.read_excel_data(file_path, args.sheet)
    if df is None:
        logger.error("读取 Excel 数据失败")
        sys.exit(1)
    
    # 检测列名
    column_mapping = importer.detect_column_names(df)
    logger.info(f"检测到列名映射: {column_mapping}")
    
    # 预处理数据
    processed_df, errors = importer.preprocess_data(df, column_mapping)
    
    if errors:
        logger.warning(f"数据预处理发现问题: {errors}")
    
    # 检查重复数据
    duplicate_stats = importer.check_duplicates(processed_df)
    
    # 导入到数据库
    import_stats = importer.import_to_database(
        processed_df, 
        mode=args.mode,
        dry_run=args.dry_run
    )
    
    # 合并统计信息
    import_stats['duplicate_stats'] = duplicate_stats
    
    # 生成报告
    report = importer.generate_report(
        import_stats, 
        import_mode=args.mode,
        dry_run=args.dry_run
    )
    
    # 输出报告
    print(report)
    
    # 保存报告
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = None

    importer.save_report(report, output_path, output_manager=output_manager)
    print(f"📝 导入报告: {output_path if output_path else output_manager.get_path('reports', 'data_import_report.md')}")

    # 输出目录摘要
    output_manager.print_summary()
    
    # 总结
    if import_stats['failed'] == 0:
        logger.info("✅ 导入完成！")
    else:
        logger.warning(f"⚠️ 导入完成，但有 {import_stats['failed']} 个失败")
    
    return 0 if import_stats['failed'] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())