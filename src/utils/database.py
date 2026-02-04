# 修改文件：src/utils/database.py
# 修改内容：修复表结构和查询语句

"""
数据库操作模块
负责创建数据库、表结构，以及数据的增删改查
"""
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime

# 统一基金代码格式，避免查询不到带/不带.OF的记录
from .fund_code_manager import fund_code_manager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FundDatabase:
    """基金数据库管理类"""
    
    def __init__(self, db_path: str = "data/fund_data.db"):
        """
        初始化数据库
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 英文列名 -> 中文列名映射，便于展示与导出
        self.column_mappings = {
            'funds': {
                'fund_id': '基金代码',
                'name': '基金名称',
                'type': '基金类型',
                'inception_date': '成立日期',
                'manager': '基金经理',
                'created_at': '创建时间',
                'updated_at': '更新时间'
            },
            'fund_daily_data': {
                'fund_id': '基金代码',
                'date': '交易日期',
                'nav': '单位净值',
                'cumulative_nav': '累计净值',
                'daily_growth': '日增长率',
                'created_at': '创建时间'
            },
            'index_daily_data': {
                'index_id': '指数代码',
                'date': '交易日期',
                'close': '收盘价'
            },
            'performance_metrics': {
                'id': '记录ID',
                'fund_id': '标的代码',
                'total_return': '总收益率',
                'annual_return': '年化收益率',
                'annual_volatility': '年化波动率',
                'max_drawdown': '最大回撤',
                'sharpe_ratio': '夏普比率',
                'calmar_ratio': 'Calmar比率',
                'start_date': '开始日期',
                'end_date': '结束日期',
                'periods': '样本点数',
                'created_at': '创建时间'
            }
        }

        self._init_database()
    
    def _init_database(self):
        """初始化数据库表结构"""
        conn = sqlite3.connect(str(self.db_path))
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
        
        # 创建基金日频数据表 - 根据实际数据调整
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

        # 创建带中文列名的视图，方便直接查询中文列名
        self._create_chinese_views(cursor)
        
        conn.commit()
        conn.close()
        logger.info(f"数据库初始化完成: {self.db_path}")
    
    def get_connection(self):
        """获取数据库连接"""
        return sqlite3.connect(str(self.db_path))

    def _rename_columns(self, df: pd.DataFrame, table: str) -> pd.DataFrame:
        """将DataFrame列名转换为中文显示名"""
        mapping = self.column_mappings.get(table, {})
        if df.empty or not mapping:
            return df
        rename_dict = {col: cn for col, cn in mapping.items() if col in df.columns}
        return df.rename(columns=rename_dict)

    def _create_chinese_views(self, cursor):
        """创建中文列名视图，便于直接查询中文列"""
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
    
    def insert_fund_info(self, fund_data: Dict[str, Any]) -> bool:
        """
        插入基金基本信息
        
        Args:
            fund_data: 基金信息字典
            
        Returns:
            是否成功
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            sql = '''
            INSERT OR REPLACE INTO funds 
            (fund_id, name, type, inception_date, manager)
            VALUES (?, ?, ?, ?, ?)
            '''
            
            cursor.execute(sql, (
                fund_data['fund_id'],
                fund_data['name'],
                fund_data.get('type'),
                fund_data.get('inception_date'),
                fund_data.get('manager')
            ))
            
            conn.commit()
            conn.close()
            logger.info(f"插入基金信息成功: {fund_data['fund_id']}")
            return True
            
        except Exception as e:
            logger.error(f"插入基金信息失败: {e}")
            return False
    
    def insert_fund_daily_data(self, fund_id: str, daily_data: pd.DataFrame) -> int:
        """
        插入基金日频数据
        
        Args:
            fund_id: 基金代码
            daily_data: 日频数据DataFrame
            
        Returns:
            插入的记录数
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 准备数据
            records = []
            for _, row in daily_data.iterrows():
                # 检查是否有累积净值列，如果没有则使用单位净值
                cumulative_nav = row.get('cumulative_nav')
                if pd.isna(cumulative_nav) or cumulative_nav is None:
                    cumulative_nav = row.get('nav')
                
                records.append((
                    fund_id,
                    row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else row['date'],
                    row.get('nav'),
                    cumulative_nav,
                    row.get('daily_growth')
                ))
            
            # 批量插入
            sql = '''
            INSERT OR IGNORE INTO fund_daily_data 
            (fund_id, date, nav, cumulative_nav, daily_growth)
            VALUES (?, ?, ?, ?, ?)
            '''
            
            cursor.executemany(sql, records)
            conn.commit()
            inserted_count = cursor.rowcount
            conn.close()
            
            logger.info(f"插入基金日频数据成功: {fund_id}, 记录数: {inserted_count}")
            return inserted_count
            
        except Exception as e:
            logger.error(f"插入基金日频数据失败: {e}")
            return 0
    
    def insert_index_daily_data(self, index_id: str, daily_data: pd.DataFrame) -> int:
        """
        插入指数日频数据
        
        Args:
            index_id: 指数代码
            daily_data: 日频数据DataFrame
            
        Returns:
            插入的记录数
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 准备数据
            records = []
            for _, row in daily_data.iterrows():
                records.append((
                    index_id,
                    row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else row['date'],
                    row['close']
                ))
            
            # 批量插入
            sql = '''
            INSERT OR IGNORE INTO index_daily_data 
            (index_id, date, close)
            VALUES (?, ?, ?)
            '''
            
            cursor.executemany(sql, records)
            conn.commit()
            inserted_count = cursor.rowcount
            conn.close()
            
            logger.info(f"插入指数数据成功: {index_id}, 记录数: {inserted_count}")
            return inserted_count
            
        except Exception as e:
            logger.error(f"插入指数数据失败: {e}")
            return 0
    
    def get_fund_daily_data(self, fund_id: str, start_date: str = None, end_date: str = None,
                            use_chinese_columns: bool = False) -> pd.DataFrame:
        """
        获取基金日频数据
        
        Args:
            fund_id: 基金代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            DataFrame包含日频数据
        """
        try:
            conn = self.get_connection()

            # 同时支持带/不带.OF的基金代码
            normalized_id = fund_code_manager.to_database_format(fund_id)
            pure_id = fund_code_manager.to_akshare_format(fund_id)

            id_candidates = []
            if normalized_id:
                id_candidates.append(normalized_id)
            if pure_id and pure_id not in id_candidates:
                id_candidates.append(pure_id)

            if not id_candidates:
                return pd.DataFrame()

            id_placeholders = ",".join(["?"] * len(id_candidates))

            # 构建查询条件
            conditions = [f"fund_id IN ({id_placeholders})"]
            params = id_candidates.copy()

            if start_date:
                conditions.append("date >= ?")
                params.append(start_date)
            
            if end_date:
                conditions.append("date <= ?")
                params.append(end_date)
            
            where_clause = " AND ".join(conditions)
            sql = f'''
            SELECT date, nav, cumulative_nav, daily_growth
            FROM fund_daily_data
            WHERE {where_clause}
            ORDER BY date
            '''
            
            df = pd.read_sql_query(sql, conn, params=params)
            conn.close()
            
            # 转换日期列
            if not df.empty and 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                if use_chinese_columns:
                    df.rename(columns={'date': '交易日期'}, inplace=True)
                    df = self._rename_columns(df, 'fund_daily_data')
                    if '交易日期' in df.columns:
                        df.set_index('交易日期', inplace=True)
                else:
                    df.set_index('date', inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"获取基金数据失败: {e}")
            return pd.DataFrame()
    
    def get_all_funds(self, use_chinese_columns: bool = False) -> pd.DataFrame:
        """获取所有基金列表"""
        try:
            conn = self.get_connection()
            df = pd.read_sql_query("SELECT * FROM funds", conn)
            conn.close()
            if use_chinese_columns:
                df = self._rename_columns(df, 'funds')
            return df
        except Exception as e:
            logger.error(f"获取基金列表失败: {e}")
            return pd.DataFrame()
    
    def get_index_daily_data(self, index_id: str, start_date: str = None, end_date: str = None,
                             use_chinese_columns: bool = False) -> pd.DataFrame:
        """
        获取指数日频数据
        
        Args:
            index_id: 指数代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            DataFrame包含指数数据
        """
        try:
            conn = self.get_connection()
            
            # 构建查询条件
            conditions = ["index_id = ?"]
            params = [index_id]
            
            if start_date:
                conditions.append("date >= ?")
                params.append(start_date)
            
            if end_date:
                conditions.append("date <= ?")
                params.append(end_date)
            
            where_clause = " AND ".join(conditions)
            sql = f'''
            SELECT date, close
            FROM index_daily_data
            WHERE {where_clause}
            ORDER BY date
            '''
            
            df = pd.read_sql_query(sql, conn, params=params)
            conn.close()
            
            # 转换日期列
            if not df.empty and 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                if use_chinese_columns:
                    df.rename(columns={'date': '交易日期'}, inplace=True)
                    df = self._rename_columns(df, 'index_daily_data')
                    if '交易日期' in df.columns:
                        df.set_index('交易日期', inplace=True)
                else:
                    df.set_index('date', inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"获取指数数据失败: {e}")
            return pd.DataFrame()
    
    def check_data_exists(self, table: str, id_column: str, id_value: str) -> bool:
        """
        检查数据是否已存在
        
        Args:
            table: 表名
            id_column: ID列名
            id_value: ID值
            
        Returns:
            是否存在
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            sql = f"SELECT 1 FROM {table} WHERE {id_column} = ? LIMIT 1"
            cursor.execute(sql, (id_value,))
            result = cursor.fetchone() is not None
            
            conn.close()
            return result
            
        except Exception as e:
            logger.error(f"检查数据存在失败: {e}")
            return False

    def upsert_performance_metrics(self, df: pd.DataFrame) -> int:
        """将绩效指标写入performance_metrics表，存在则覆盖

        Args:
            df: 包含绩效指标的DataFrame，需包含fund_id、total_return、annual_return、
                annual_volatility、max_drawdown、sharpe_ratio、calmar_ratio、
                start_date、end_date、periods 等列
        Returns:
            成功写入/覆盖的行数
        """
        if df is None or df.empty:
            return 0

        required_cols = {
            'fund_id', 'total_return', 'annual_return', 'annual_volatility',
            'max_drawdown', 'sharpe_ratio', 'calmar_ratio', 'start_date', 'end_date', 'periods'
        }

        missing = required_cols - set(df.columns)
        if missing:
            logger.error(f"绩效写入失败，缺少列: {missing}")
            return 0

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            rows = []
            for _, row in df.iterrows():
                # 生成稳定的主键：fund_id|start|end
                start_str = row['start_date']
                end_str = row['end_date']
                if hasattr(start_str, 'strftime'):
                    start_str = start_str.strftime('%Y-%m-%d')
                if hasattr(end_str, 'strftime'):
                    end_str = end_str.strftime('%Y-%m-%d')
                record_id = f"{row['fund_id']}|{start_str}|{end_str}"

                rows.append((
                    record_id,
                    row['fund_id'],
                    float(row['total_return']),
                    float(row['annual_return']),
                    float(row['annual_volatility']),
                    float(row['max_drawdown']),
                    float(row['sharpe_ratio']),
                    float(row['calmar_ratio']),
                    start_str,
                    end_str,
                    int(row['periods'])
                ))

            cursor.executemany(
                '''
                INSERT OR REPLACE INTO performance_metrics (
                    id, fund_id, total_return, annual_return, annual_volatility,
                    max_drawdown, sharpe_ratio, calmar_ratio, start_date, end_date, periods
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                rows
            )

            conn.commit()
            count = cursor.rowcount
            conn.close()
            logger.info(f"绩效指标写入/更新完成: {count} 条")
            return count

        except Exception as e:
            logger.error(f"写入绩效指标失败: {e}")
            return 0
    
    def get_all_indices(self, use_chinese_columns: bool = False) -> pd.DataFrame:
        """获取所有指数列表"""
        try:
            conn = self.get_connection()
            sql = "SELECT DISTINCT index_id FROM index_daily_data"
            df = pd.read_sql_query(sql, conn)
            conn.close()
            if use_chinese_columns:
                df.rename(columns={'index_id': '指数代码'}, inplace=True)
            return df
        except Exception as e:
            logger.error(f"获取指数列表失败: {e}")
            return pd.DataFrame()


# 创建全局数据库实例
fund_db = FundDatabase()


if __name__ == "__main__":
    # 测试数据库连接和表创建
    db = FundDatabase()
    print("✅ 数据库初始化完成")
    
    # 测试获取连接
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f"📊 数据库中的表: {[t[0] for t in tables]}")