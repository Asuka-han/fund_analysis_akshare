# 修改文件：src/data_fetch/fund_fetcher.py
# 修改内容：增加基金资产规模数据获取功能

"""
基金数据获取器
负责从数据源获取基金的历史净值等数据
"""
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Optional, Any
try:
    import config
    _FETCH_YEARS = getattr(config, 'DEFAULT_FETCH_YEARS', 3)
except Exception:
    _FETCH_YEARS = 3

logger = logging.getLogger(__name__)

# 直接导入基金代码管理器
try:
    from src.utils.fund_code_manager import fund_code_manager
    logger.info("✅ 成功导入基金代码管理器")
except ImportError as e:
    logger.error(f"❌ 导入基金代码管理器失败: {e}")
    # 创建简单的回退方案
    class SimpleFundCodeManager:
        @staticmethod
        def to_akshare_format(fund_code: str) -> str:
            return fund_code.replace('.OF', '') if fund_code else ''
        
        @staticmethod
        def to_display_format(fund_code: str) -> str:
            if fund_code.endswith('.OF'):
                return fund_code
            return f"{fund_code}.OF"
        
        @staticmethod
        def batch_to_akshare(fund_codes: List[str]) -> List[str]:
            return [code.replace('.OF', '') for code in fund_codes]
        
        @staticmethod
        def batch_to_display(fund_codes: List[str]) -> List[str]:
            return [code if code.endswith('.OF') else f"{code}.OF" for code in fund_codes]
        
        @staticmethod
        def get_code_pair(fund_code: str) -> dict:
            pure_code = fund_code.replace('.OF', '')
            display_code = fund_code if fund_code.endswith('.OF') else f"{fund_code}.OF"
            return {
                'original': fund_code,
                'akshare': pure_code,
                'display': display_code,
                'database': display_code
            }
    
    fund_code_manager = SimpleFundCodeManager()
    logger.info("⚠️ 使用简单基金代码管理器")


# 这些代码用于测试和演示，实际使用时需要替换为有效的基金代码
SAMPLE_FUND_CODES = [
    '161725',  # 招商中证白酒指数分级
    '002621',  # 中邮趋势优选灵活配置混合A
    '005918',  # 广发医疗保健股票A
    '003598'   # 南方品质优选灵活配置混合A
]

class FundDataFetcher:
    """基金数据获取器"""
    
    def __init__(self):
        """初始化数据获取器"""
        self.fund_codes = []
        self.data_cache = {}
        logger.info("✅ 基金数据获取器初始化完成")
    
    def set_fund_codes(self, codes: List[str]):
        """设置要获取数据的基金代码列表"""
        self.fund_codes = codes
        logger.info(f"📊 已设置基金代码: {codes}")
    
    def fetch_fund_data(self, fund_code: str, 
                       start_date: Optional[str] = None, 
                       end_date: Optional[str] = None,
                       include_asset_size: bool = False) -> Optional[pd.DataFrame]:
        """
        获取单个基金的数据
        
        Args:
            fund_code: 基金代码（可以是带或不带.OF后缀的格式）
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            include_asset_size: 是否包含资产规模数据
            
        Returns:
            DataFrame包含基金历史净值数据，失败则返回None
        """
        try:
            # 处理基金代码格式
            original_fund_code = fund_code
            clean_fund_code = fund_code_manager.to_akshare_format(fund_code)
            
            logger.info(f"🔍 正在获取基金 {original_fund_code} (AKShare格式: {clean_fund_code}) 的数据...")
            
            # 使用akshare获取基金净值数据
            # 根据文档，参数应该是symbol而不是fund
            fund_net_value_data = ak.fund_open_fund_info_em(symbol=clean_fund_code, indicator="单位净值走势")
            
            if fund_net_value_data.empty:
                logger.warning(f"⚠️ 基金 {original_fund_code} 没有获取到净值数据")
                return None
            
            # 重命名列名以符合内部标准
            if '净值日期' in fund_net_value_data.columns:
                fund_net_value_data.rename(columns={
                    '净值日期': 'date',
                    '单位净值': 'nav',
                    '累计净值': 'cumulative_nav',
                    '日增长率': 'daily_growth'
                }, inplace=True)
            elif 'date' in fund_net_value_data.columns:
                # 如果已经是英文列名，则保持不变
                pass
            else:
                logger.error(f"❌ 无法识别基金 {original_fund_code} 的列名格式")
                return None
            
            # 确保日期列是datetime类型
            fund_net_value_data['date'] = pd.to_datetime(fund_net_value_data['date'])
            
            # 如果没有指定日期范围，默认获取最近 DEFAULT_FETCH_YEARS 年的数据
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365 * _FETCH_YEARS)).strftime('%Y%m%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
                
            # 筛选指定日期范围内的数据
            fund_net_value_data = fund_net_value_data[
                (fund_net_value_data['date'] >= pd.to_datetime(start_date)) & 
                (fund_net_value_data['date'] <= pd.to_datetime(end_date))
            ].copy()
            
            if fund_net_value_data.empty:
                logger.warning(f"⚠️ 基金 {original_fund_code} 在指定日期范围内没有数据")
                return None
            
            # 按日期排序
            fund_net_value_data.sort_values(by='date', inplace=True)
            fund_net_value_data.reset_index(drop=True, inplace=True)
            
            # 确保累计净值列存在，如果不存在则使用单位净值
            if 'cumulative_nav' not in fund_net_value_data.columns or fund_net_value_data['cumulative_nav'].isna().all():
                logger.warning(f"⚠️ 基金 {original_fund_code} 缺少累计净值数据，使用单位净值替代")
                fund_net_value_data['cumulative_nav'] = fund_net_value_data['nav']
            
            # 如果请求获取资产规模数据，则获取并合并
            if include_asset_size:
                asset_size_data = self._fetch_fund_asset_size(clean_fund_code)
                if asset_size_data is not None and not asset_size_data.empty:
                    # 合并资产规模数据到净值数据
                    fund_net_value_data = self._merge_asset_size_data(fund_net_value_data, asset_size_data)
                else:
                    logger.warning(f"⚠️ 基金 {original_fund_code} 无法获取资产规模数据，将在净值数据中设置默认值")
                    # 添加资产规模列并填充默认值
                    fund_net_value_data['asset_size'] = None
            
            logger.info(f"✅ 基金 {original_fund_code} 数据获取成功，共 {len(fund_net_value_data)} 条记录")
            
            # 添加基金代码列（使用原始传入格式）
            fund_net_value_data['fund_id'] = original_fund_code
            
            return fund_net_value_data
            
        except Exception as e:
            logger.error(f"❌ 获取基金 {fund_code} 数据失败: {e}")
            return None
    
    def _fetch_fund_asset_size(self, fund_code: str) -> Optional[pd.DataFrame]:
        """
        获取基金的资产规模数据
        
        Args:
            fund_code: 基金代码（AKShare格式）
            
        Returns:
            DataFrame包含资产规模数据，包含'date'和'asset_size'列
        """
        try:
            logger.info(f"📊 正在获取基金 {fund_code} 的资产规模数据...")
            
            # 尝试获取基金规模数据（通常为季度数据）
            # 使用 ak.fund_fund_info_em 获取基金基本信息，其中包含资产规模
            fund_info = ak.fund_fund_info_em(symbol=fund_code)
            
            if fund_info.empty:
                logger.warning(f"⚠️ 基金 {fund_code} 没有获取到资产规模数据")
                return None
            
            # 查找包含资产规模的列
            asset_size_col = None
            for col in fund_info.columns:
                if '规模' in col or 'asset' in col.lower() or 'size' in col.lower():
                    asset_size_col = col
                    break
            
            if asset_size_col is None:
                logger.warning(f"⚠️ 基金 {fund_code} 的资产规模列未找到")
                return None
            
            # 提取资产规模数据
            asset_size_data = fund_info[['净值日期', asset_size_col]].copy()
            asset_size_data.rename(columns={
                '净值日期': 'date',
                asset_size_col: 'asset_size'
            }, inplace=True)
            
            # 清理资产规模数据，去除单位并转换为数值
            asset_size_data['date'] = pd.to_datetime(asset_size_data['date'])
            
            # 转换资产规模为数值（例如：将"123.45亿元"转换为123.45）
            def convert_asset_size(value):
                if pd.isna(value):
                    return None
                # 移除单位并转换为浮点数
                try:
                    # 尝试直接转换数字
                    return float(value)
                except:
                    # 如果是字符串，尝试提取数字部分
                    import re
                    match = re.search(r'([\d\.]+)', str(value))
                    if match:
                        return float(match.group(1))
                    return None
            
            asset_size_data['asset_size'] = asset_size_data['asset_size'].apply(convert_asset_size)
            
            # 按日期排序
            asset_size_data.sort_values(by='date', inplace=True)
            asset_size_data.reset_index(drop=True, inplace=True)
            
            logger.info(f"✅ 基金 {fund_code} 资产规模数据获取成功，共 {len(asset_size_data)} 条记录")
            return asset_size_data
            
        except Exception as e:
            logger.warning(f"⚠️ 获取基金 {fund_code} 资产规模数据失败: {e}")
            # 尝试使用其他接口获取资产规模数据
            try:
                # 使用基金基本信息接口
                fund_basic_info = ak.fund_name_em()
                fund_info = fund_basic_info[fund_basic_info['基金代码'] == fund_code]
                
                if not fund_info.empty:
                    # 查找资产规模相关字段
                    for col in ['规模', '最新规模', '资产规模']:
                        if col in fund_info.columns:
                            asset_size = fund_info.iloc[0][col]
                            if not pd.isna(asset_size):
                                # 创建一个包含当前日期的资产规模数据
                                today = datetime.now().strftime('%Y-%m-%d')
                                asset_size_data = pd.DataFrame({
                                    'date': [pd.to_datetime(today)],
                                    'asset_size': [convert_asset_size(asset_size)]
                                })
                                logger.info(f"✅ 基金 {fund_code} 从基本信息获取到资产规模: {asset_size}")
                                return asset_size_data
            except Exception as inner_e:
                logger.debug(f"从备用接口获取资产规模也失败: {inner_e}")
            
            return None
    
    def _merge_asset_size_data(self, nav_data: pd.DataFrame, asset_size_data: pd.DataFrame) -> pd.DataFrame:
        """
        将资产规模数据合并到净值数据中
        
        Args:
            nav_data: 净值数据DataFrame
            asset_size_data: 资产规模数据DataFrame
            
        Returns:
            合并后的DataFrame
        """
        # 确保两个DataFrame都有date列且为datetime类型
        nav_data = nav_data.copy()
        
        if asset_size_data.empty:
            nav_data['asset_size'] = None
            return nav_data
        
        # 由于资产规模数据是季度数据，而净值数据是日数据，
        # 我们需要使用前向填充的方法将季度数据扩展到日数据
        merged_data = nav_data.merge(asset_size_data, on='date', how='left')
        
        # 对资产规模列进行前向填充（使用最近已知的资产规模数据）
        merged_data['asset_size'] = merged_data['asset_size'].ffill()
        
        # 如果仍然有缺失值，使用后向填充（使用未来的数据填充过去）
        merged_data['asset_size'] = merged_data['asset_size'].bfill()
        
        # 如果还有缺失值，设置为None
        merged_data['asset_size'] = merged_data['asset_size'].where(
            pd.notnull(merged_data['asset_size']), None
        )
        
        return merged_data
    
    def fetch_all_funds_data(self, fund_codes: List[str], include_asset_size: bool = False) -> Dict[str, pd.DataFrame]:
        """
        获取多个基金的数据
        
        Args:
            fund_codes: 基金代码列表（可以是带或不带.OF后缀的格式）
            include_asset_size: 是否包含资产规模数据
            
        Returns:
            字典，键为基金代码，值为对应的DataFrame
        """
        results = {}
        
        for fund_code in fund_codes:
            logger.info(f"📊 正在获取基金 {fund_code} 的数据...")
            
            # 获取单个基金数据
            fund_data = self.fetch_fund_data(fund_code, include_asset_size=include_asset_size)
            
            if fund_data is not None:
                results[fund_code] = fund_data
                
                # 添加到缓存
                self.data_cache[fund_code] = fund_data
            else:
                logger.warning(f"⚠️ 基金 {fund_code} 数据获取失败")
            
            # 添加延时避免过于频繁的API调用
            time.sleep(0.5)
        
        logger.info(f"✅ 共成功获取 {len(results)} 只基金的数据")
        return results
    
    def get_fund_info(self, fund_code: str) -> dict:
        """
        获取单只基金的基本信息
        
        Args:
            fund_code: 基金代码（可以是带或不带.OF后缀的格式）
            
        Returns:
            基金基本信息字典
        """
        try:
            # 处理基金代码格式
            original_fund_code = fund_code
            clean_fund_code = fund_code_manager.to_akshare_format(fund_code)
            
            logger.info(f"🔍 正在获取基金 {original_fund_code} 的基本信息...")
            
            # 使用正确的接口获取基金基本信息
            fund_info_df = ak.fund_name_em()
            
            # 过滤指定基金代码的数据
            fund_info = fund_info_df[fund_info_df['基金代码'] == clean_fund_code]
            
            if fund_info.empty:
                logger.warning(f"未找到基金代码 {original_fund_code} (AKShare格式: {clean_fund_code}) 的基本信息")
                return {}
                
            # 转换为字典格式
            info_dict = fund_info.iloc[0].to_dict()
            
            # 提取关键字段
            result = {
                'fund_id': original_fund_code,  # 保持原始传入格式
                'name': info_dict.get('基金简称', ''),
                'type': info_dict.get('基金类型', ''),
                'pinyin_short': info_dict.get('拼音缩写', ''),
                'pinyin_full': info_dict.get('拼音全称', '')
            }
            
            # 尝试获取资产规模信息
            for asset_key in ['规模', '最新规模', '资产规模']:
                if asset_key in info_dict:
                    result['asset_size'] = info_dict.get(asset_key)
                    break
            
            logger.info(f"✅ 成功获取基金 {original_fund_code} 的基本信息")
            return result
            
        except Exception as e:
            logger.error(f"❌ 获取基金 {fund_code} 基本信息失败: {e}")
            return {}
    
if __name__ == "__main__":
    # 测试代码
    fetcher = FundDataFetcher()
    test_codes = SAMPLE_FUND_CODES[:2]  # 只测试前两个基金
    
    print("测试基金数据获取（包含资产规模）:")
    print("="*60)
    
    # 测试获取基金数据（包含资产规模）
    results = fetcher.fetch_all_funds_data(test_codes, include_asset_size=True)
    
    for code, data in results.items():
        print(f"\n基金 {code} 的前5条数据:")
        print(data[['date', 'nav', 'cumulative_nav', 'asset_size']].head() if 'asset_size' in data.columns else data.head())
        
        # 如果有资产规模数据，显示统计信息
        if 'asset_size' in data.columns:
            asset_stats = data['asset_size'].describe()
            print(f"资产规模统计: 非空值 {data['asset_size'].count()} 条，平均值 {asset_stats.get('mean', 'N/A'):.2f}")
    
    # 测试获取基金基本信息
    print("\n" + "="*60)
    print("测试获取基金基本信息:")
    for code in test_codes:
        fund_info = fetcher.get_fund_info(code)
        print(f"\n基金 {code} 的基本信息:")
        for key, value in fund_info.items():
            print(f"  {key}: {value}")