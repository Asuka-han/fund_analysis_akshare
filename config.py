# 修改文件：config.py
# 修改内容：重构指数配置，支持复合指数和业绩基准

"""
配置文件
存储项目配置参数
"""
import os
from pathlib import Path
from typing import Dict, List, Tuple, Union, Optional

from src.utils.runtime_env import add_project_paths, get_repo_root, get_storage_root

# 确保在冻结/普通环境下均能正确找到项目路径
REPO_ROOT, STORAGE_ROOT = add_project_paths()

# 项目路径（可写目录，一般为当前工作目录或可执行文件所在目录）
PROJECT_DIR = STORAGE_ROOT

# 数据路径
DATA_DIR = PROJECT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# 输出路径
REPORTS_DIR = PROJECT_DIR / "reports"

# 数据库配置
DATABASE_PATH = DATA_DIR / "fund_data.db"

# 基金配置 - 保持原有格式（带.OF后缀）
FUND_CODES = [
    "000001.OF",  # 华夏成长混合
    "510300.OF",  # 华泰柏瑞沪深300ETF
    "519133.OF",  # 海富通改革驱动混合
    "169105.OF",  # 东方红睿满沪港深混合
    "003095.OF"   # 中欧医疗健康混合A
]

# 基础指数配置（可直接从数据源获取的指数）
BASE_INDICES = {
    '000001': {
        'name': '上证综指',
        'display_name': '上证综指',
        'suffix': '.SH',  # 支持带后缀的代码
        'color': 'red',
        'weight': 1.0,
        'source': 'akshare'
    },
    '000300': {
        'name': '沪深300',
        'display_name': '沪深300',
        'suffix': '.SH',
        'color': 'green',
        'weight': 1.0,
        'source': 'akshare'
    },
    'HSI': {
        'name': '恒生指数',
        'display_name': '恒生指数',
        'suffix': '.HK',
        'color': 'orange',
        'weight': 1.0,
        'source': 'akshare'
    },
    '000933': {
        'name': '中证医药卫生指数',
        'display_name': '中证医药',
        'suffix': '.SH',
        'color': 'purple',
        'weight': 1.0,
        'source': 'akshare'
    },
    '000012': {
        'name': '上证国债指数',
        'display_name': '上证国债',
        'suffix': '.SH',
        'color': 'blue',
        'weight': 1.0,
        'source': 'akshare'
    }
}

# 复合指数配置
COMPOSITE_INDICES = {
    'MED_INNOV': {
        'name': '医疗创新指数',
        'display_name': '医疗创新',
        'color': 'purple',
        'weight': 1.0,
        'type': 'composite',
        'components': [
            {
                'base_code': '000933',  # 中证医药卫生指数
                'weight': 0.8,
                'description': '中证医药卫生指数代表医疗创新主体'
            },
            {
                'base_code': '000012',  # 上证国债指数
                'weight': 0.2,
                'description': '上证国债指数代表稳定收益部分'
            }
        ],
        'description': '医疗创新复合指数 = 中证医药卫生指数 × 80% + 上证国债指数 × 20%',
        'calculation_method': 'weighted_return'
    },
    # 示例：添加新复合指数的模板
    'CUSTOM_COMPOSITE': {
        'name': '自定义复合指数',
        'display_name': '自定义',
        'color': 'gray',
        'weight': 1.0,
        'type': 'composite',
        'components': [
            {
                'base_code': '000300',  # 沪深300
                'weight': 0.6
            },
            {
                'base_code': '000001',  # 上证综指
                'weight': 0.3
            },
            {
                'base_code': 'HSI',  # 恒生指数
                'weight': 0.1
            }
        ],
        'description': '示例：60%沪深300 + 30%上证综指 + 10%恒生指数',
        'calculation_method': 'weighted_return'
    }
}

# 合并所有指数配置
INDICES = {**BASE_INDICES, **COMPOSITE_INDICES}





# 默认基准指数
DEFAULT_BENCHMARK = '000300'

# 基金业绩基准配置
FUND_BENCHMARKS = {
    "000001.OF": "000300",  # 华夏成长混合 -> 沪深300
    "510300.OF": "000300",  # 华泰柏瑞沪深300ETF -> 沪深300
    "519133.OF": "000300",  # 海富通改革驱动混合 -> 沪深300
    "169105.OF": "HSI",     # 东方红睿满沪港深混合 -> 恒生指数
    "003095.OF": "MED_INNOV"  # 中欧医疗健康混合A -> 医疗创新指数
}





# 默认对比指数
DEFAULT_COMPARISON_INDICES = ['000300', '000001', 'HSI']

# 基金对比指数配置（每个基金可配置不同的对比指数）
FUND_COMPARISON_INDICES = {
    "000001.OF": ['000300', '000001', 'HSI'],
    "510300.OF": ['000300', '000001'],
    "519133.OF": ['000300', '000001'],
    "169105.OF": ['HSI', '000300'],
    "003095.OF": ['MED_INNOV', '000300', '000001']
}

# 月度对比指数配置（每个基金可配置用于月度表格的首选对比指数或对比指数列表）
# 值可以是单个字符串或字符串列表，示例：
#   "000001.OF": '000300'  或  "000001.OF": ['000300', '000001']
FUND_MONTHLY_COMPARISON = {
    "000001.OF": ['000300'],
    "510300.OF": ['000300'],
    "519133.OF": ['000300'],
    "169105.OF": ['HSI'],
    "003095.OF": ['MED_INNOV']
}









# 基准指数ID列表（用于持有期分析）
BENCHMARK_IDS = ['000001', '000300', 'HSI', 'MED_INNOV']

# 指数代码标准化函数
def normalize_index_code(index_code: str) -> str:
    """
    标准化指数代码（去除后缀）
    
    Args:
        index_code: 指数代码，可能带后缀（如'000300.SH'）
        
    Returns:
        去除后缀的标准化代码
    """
    # 移除常见后缀
    suffixes = ['.SH', '.SZ', '.HK', '.OF', '.SS', '.XSHE']
    for suffix in suffixes:
        if index_code.endswith(suffix):
            return index_code[:-len(suffix)]
    return index_code

def get_index_with_suffix(index_code: str) -> str:
    """
    获取带后缀的指数代码
    
    Args:
        index_code: 标准化指数代码
        
    Returns:
        带后缀的指数代码
    """
    normalized_code = normalize_index_code(index_code)
    if normalized_code in BASE_INDICES:
        suffix = BASE_INDICES[normalized_code].get('suffix', '')
        return f"{normalized_code}{suffix}"
    return normalized_code

# 获取实际指数代码列表（支持带后缀）
def get_actual_benchmark_codes() -> list:
    """获取实际指数代码列表（带后缀格式）"""
    codes = []
    for idx in BENCHMARK_IDS:
        if idx in BASE_INDICES:
            suffix = BASE_INDICES[idx].get('suffix', '')
            codes.append(f"{idx}{suffix}")
        else:
            codes.append(idx)
    return codes

# 获取指数显示名称
def get_benchmark_display_name(benchmark_id: str) -> str:
    """获取基准指数显示名称"""
    normalized_id = normalize_index_code(benchmark_id)
    if normalized_id in INDICES:
        return INDICES[normalized_id].get('display_name', INDICES[normalized_id].get('name', benchmark_id))
    return benchmark_id

# 获取指数颜色
def get_benchmark_color(benchmark_id: str) -> str:
    """获取基准指数颜色"""
    normalized_id = normalize_index_code(benchmark_id)
    if normalized_id in INDICES:
        return INDICES[normalized_id].get('color', 'gray')
    return 'gray'

# 获取基金业绩基准
def get_fund_benchmark(fund_code: str) -> str:
    """
    获取基金的业绩基准
    
    Args:
        fund_code: 基金代码（带.OF后缀）
        
    Returns:
        基准指数代码
    """
    # 如果基金有配置特定基准，则使用配置的基准
    if fund_code in FUND_BENCHMARKS:
        return FUND_BENCHMARKS[fund_code]
    
    # 否则使用默认基准
    return DEFAULT_BENCHMARK

# 获取基金对比指数
def get_fund_comparison_indices(fund_code: str) -> List[str]:
    """
    获取基金对比指数列表
    
    Args:
        fund_code: 基金代码（带.OF后缀）
        
    Returns:
        对比指数代码列表
    """
    return FUND_COMPARISON_INDICES.get(fund_code, DEFAULT_COMPARISON_INDICES)


def get_fund_monthly_comparison(fund_code: str) -> List[str]:
    """
    获取基金用于月度表的对比指数配置。

    返回一个索引ID列表（即使配置为单个字符串也会以列表形式返回），
    如果未配置则返回与 `get_fund_comparison_indices` 相同的默认列表。
    """
    val = FUND_MONTHLY_COMPARISON.get(fund_code)
    if not val:
        return get_fund_comparison_indices(fund_code)
    if isinstance(val, (list, tuple)):
        return list(val)
    return [val]

# 检查是否为复合指数
def is_composite_index(index_code: str) -> bool:
    """检查是否为复合指数"""
    normalized_code = normalize_index_code(index_code)
    return normalized_code in COMPOSITE_INDICES

# 获取复合指数配置
def get_composite_components(index_code: str) -> List[Dict]:
    """获取复合指数的成分配置"""
    normalized_code = normalize_index_code(index_code)
    if normalized_code in COMPOSITE_INDICES:
        return COMPOSITE_INDICES[normalized_code].get('components', [])
    return []

# 分析参数
RISK_FREE_RATE = 0.02  # 无风险利率（2%）
TRADING_DAYS = 252     # 年化交易日数
ANNUALIZATION_DAYS = int(os.getenv("ANNUALIZATION_DAYS", str(TRADING_DAYS)))

# 持有期配置
HOLDING_PERIODS = [30, 60, 90, 180, 360]

# 输出控制（HTML交互图）
OUTPUT_HTML_NAV_CURVE = True          # 净值曲线（交互）
OUTPUT_HTML_NAV_DRAWDOWN = True       # 净值+回撤（交互）
OUTPUT_HTML_HOLDING_DIST = True       # 持有期收益率分布（交互）

# 默认数据抓取年限（用于基金与指数）
DEFAULT_FETCH_YEARS = 5

# AKShare配置
AKSHARE_TIMEOUT = 30  # 请求超时时间（秒）
AKSHARE_MAX_RETRIES = 3  # 最大重试次数

# 创建必要的目录
for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


# 基金代码格式辅助函数
def get_akshare_fund_codes() -> list:
    """
    获取AKShare格式的基金代码列表（纯数字）
    
    Returns:
        纯数字格式的基金代码列表
    """
    from src.utils.fund_code_manager import fund_code_manager
    return fund_code_manager.batch_to_akshare(FUND_CODES)

def get_display_fund_codes() -> list:
    """
    获取显示格式的基金代码列表（带.OF后缀）
    
    Returns:
        带.OF后缀的基金代码列表
    """
    return FUND_CODES

def get_database_fund_codes() -> list:
    """
    获取数据库格式的基金代码列表（带.OF后缀）
    
    Returns:
        带.OF后缀的基金代码列表
    """
    return FUND_CODES

def get_fund_code_pairs() -> list:
    """
    获取基金代码格式对
    
    Returns:
        包含多种格式的基金代码字典列表
    """
    from src.utils.fund_code_manager import fund_code_manager
    return [fund_code_manager.get_code_pair(code) for code in FUND_CODES]

# 演示如何添加新复合指数
def add_composite_index_demo():
    """演示如何添加新的复合指数"""
    print("=== 添加新复合指数演示 ===")
    print("\n在COMPOSITE_INDICES中添加配置即可，例如：")
    print("""
NEW_COMPOSITE = {
    'NEW_COMPOSITE_ID': {
        'name': '新复合指数名称',
        'display_name': '新指数',
        'color': 'blue',
        'weight': 1.0,
        'type': 'composite',
        'components': [
            {
                'base_code': '000300',  # 沪深300
                'weight': 0.5
            },
            {
                'base_code': 'HSI',     # 恒生指数
                'weight': 0.3
            },
            {
                'base_code': '000001',  # 上证综指
                'weight': 0.2
            }
        ],
        'description': '50%沪深300 + 30%恒生指数 + 20%上证综指',
        'calculation_method': 'weighted_return'
    }
}
    """)
    print("\n然后将新指数ID添加到BENCHMARK_IDS列表中")
    print("最后在FUND_BENCHMARKS中为相关基金配置新基准")