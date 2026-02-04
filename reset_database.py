#!/usr/bin/env python
"""
数据库重置脚本
用于清理旧数据库，重新创建表结构
"""

import sqlite3
import os
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def reset_database(db_path: str = "data/fund_data.db"):
    """
    重置数据库，删除所有表并重新创建
    
    Args:
        db_path: 数据库文件路径
    """
    db_path = Path(db_path)
    
    # 如果数据库文件不存在，直接初始化
    if not db_path.exists():
        logger.info("数据库文件不存在，将创建新数据库")
        init_database(db_path)
        return
    
    try:
        # 备份旧数据库
        backup_path = db_path.with_suffix('.db.bak')
        if backup_path.exists():
            backup_path.unlink()
        db_path.rename(backup_path)
        logger.info(f"已备份旧数据库到: {backup_path}")
        
        # 创建新数据库
        init_database(db_path)
        logger.info("✅ 数据库重置完成")
        
    except Exception as e:
        logger.error(f"❌ 数据库重置失败: {e}")
        # 如果失败，尝试恢复备份
        if backup_path.exists():
            backup_path.rename(db_path)
            logger.info("已恢复备份的数据库")


def init_database(db_path: Path):
    """
    初始化数据库表结构
    
    Args:
        db_path: 数据库文件路径
    """
    try:
        # 确保目录存在
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 创建数据库连接
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # 创建基金基本信息表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS funds (
            fund_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT,
            inception_date DATE,
            manager TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 创建基金日频数据表 - 更新后的结构
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_daily_data (
            fund_id TEXT,
            date DATE,
            nav REAL NOT NULL,
            cumulative_nav REAL,
            daily_growth REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (fund_id, date),
            FOREIGN KEY (fund_id) REFERENCES funds(fund_id) ON DELETE CASCADE
        )
        ''')
        
        # 创建指数日频数据表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS index_daily_data (
            index_id TEXT NOT NULL,
            date DATE NOT NULL,
            close REAL,
            PRIMARY KEY (index_id, date)
        )
        ''')
        
        # 创建绩效指标表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS performance_metrics (
            id TEXT PRIMARY KEY,
            fund_id TEXT,
            total_return REAL,
            annual_return REAL,
            annual_volatility REAL,
            max_drawdown REAL,
            sharpe_ratio REAL,
            calmar_ratio REAL,
            start_date DATE,
            end_date DATE,
            periods INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fund_id) REFERENCES funds(fund_id) ON DELETE CASCADE
        )
        ''')
        
        # 创建索引以提高查询性能
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fund_daily_date ON fund_daily_data(date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_index_daily_date ON index_daily_data(date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fund_daily_fund_id ON fund_daily_data(fund_id)')

        # 创建带中文列名的视图，便于直接查看中文列
        try:
            cursor.execute('''
            CREATE VIEW IF NOT EXISTS v_funds_cn AS
            SELECT 
                fund_id AS "基金代码",
                name AS "基金名称",
                type AS "基金类型",
                inception_date AS "成立日期",
                manager AS "基金经理",
                created_at AS "创建时间",
                updated_at AS "更新时间"
            FROM funds
            ''')

            cursor.execute('''
            CREATE VIEW IF NOT EXISTS v_fund_daily_data_cn AS
            SELECT 
                fund_id AS "基金代码",
                date AS "交易日期",
                nav AS "单位净值",
                cumulative_nav AS "累计净值",
                daily_growth AS "日增长率",
                created_at AS "创建时间"
            FROM fund_daily_data
            ''')

            cursor.execute('''
            CREATE VIEW IF NOT EXISTS v_index_daily_data_cn AS
            SELECT 
                index_id AS "指数代码",
                date AS "交易日期",
                close AS "收盘价"
            FROM index_daily_data
            ''')

            cursor.execute('''
            CREATE VIEW IF NOT EXISTS v_performance_metrics_cn AS
            SELECT 
                id AS "记录ID",
                fund_id AS "标的代码",
                total_return AS "总收益率",
                annual_return AS "年化收益率",
                annual_volatility AS "年化波动率",
                max_drawdown AS "最大回撤",
                sharpe_ratio AS "夏普比率",
                calmar_ratio AS "Calmar比率",
                start_date AS "开始日期",
                end_date AS "结束日期",
                periods AS "样本点数",
                created_at AS "创建时间"
            FROM performance_metrics
            ''')
        except Exception as view_err:
            logger.warning(f"创建中文视图时出错: {view_err}")
        
        conn.commit()
        conn.close()
        logger.info(f"✅ 数据库初始化完成: {db_path}")
        
    except Exception as e:
        logger.error(f"❌ 数据库初始化失败: {e}")
        raise


def check_database_structure(db_path: str = "data/fund_data.db"):
    """
    检查数据库表结构
    
    Args:
        db_path: 数据库文件路径
    """
    db_path = Path(db_path)
    
    if not db_path.exists():
        logger.warning("数据库文件不存在")
        return False
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # 获取所有表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        logger.info(f"数据库中的表: {[t[0] for t in tables]}")
        
        # 检查fund_daily_data表结构
        if 'fund_daily_data' in [t[0] for t in tables]:
            cursor.execute("PRAGMA table_info(fund_daily_data)")
            columns = cursor.fetchall()
            logger.info("fund_daily_data表的列:")
            for col in columns:
                logger.info(f"  {col[1]} ({col[2]})")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"检查数据库结构失败: {e}")
        return False


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="数据库管理工具")
    parser.add_argument('action', choices=['reset', 'check', 'backup'], 
                       help='操作类型: reset-重置, check-检查, backup-备份')
    parser.add_argument('--db-path', default='data/fund_data.db', 
                       help='数据库文件路径 (默认: data/fund_data.db)')
    
    args = parser.parse_args()
    
    if args.action == 'reset':
        print("⚠️  警告：这将删除所有数据并重建数据库结构")
        confirm = input("确定要继续吗？(输入 'yes' 确认): ")
        if confirm.lower() == 'yes':
            reset_database(args.db_path)
        else:
            print("操作已取消")
    elif args.action == 'check':
        check_database_structure(args.db_path)
    elif args.action == 'backup':
        # 简单的备份功能
        db_path = Path(args.db_path)
        if db_path.exists():
            import shutil
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = db_path.with_suffix(f'.backup_{timestamp}.db')
            try:
                shutil.copy2(db_path, backup_path)
            except Exception as copy2_err:
                logger.warning(f"copy2 失败，降级为普通复制: {copy2_err}")
                import shutil as _shutil
                _shutil.copyfile(db_path, backup_path)
            logger.info(f"数据库已备份到: {backup_path}")
        else:
            logger.error("数据库文件不存在")


if __name__ == "__main__":
    main()