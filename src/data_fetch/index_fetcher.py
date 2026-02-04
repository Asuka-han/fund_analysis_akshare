# 修改文件：src/data_fetch/index_fetcher.py
# 修改内容：使用新的配置结构，支持复合指数和带后缀的代码

"""
指数数据获取器
负责从数据源获取各类指数的历史数据
"""
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Optional, Tuple
try:
    import config
    _FETCH_YEARS = getattr(config, 'DEFAULT_FETCH_YEARS', 3)
except Exception:
    _FETCH_YEARS = 3
import numpy as np

logger = logging.getLogger(__name__)


class IndexDataFetcher:
    """指数数据获取器"""
    
    def __init__(self):
        """初始化指数数据获取器"""
        try:
            import config
            self.config = config
        except ImportError:
            logger.error("无法导入config模块")
            self.config = None
        
        logger.info("✅ 指数数据获取器初始化完成")
    
    def _get_base_index_info(self, index_code: str) -> Tuple[str, str]:
        """
        获取基础指数信息
        
        Args:
            index_code: 指数代码（可能带后缀）
            
        Returns:
            (标准化代码, 显示名称)
        """
        if self.config:
            normalized_code = self.config.normalize_index_code(index_code)
            display_name = self.config.get_benchmark_display_name(normalized_code)
            return normalized_code, display_name
        return index_code, index_code
    
    def fetch_index_data(self, index_code: str, 
                        start_date: Optional[str] = None, 
                        end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        获取单个指数的数据
        
        Args:
            index_code: 指数代码（支持带后缀）
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            
        Returns:
            DataFrame包含指数历史数据，失败则返回None
        """
        try:
            # 标准化代码和获取显示名称
            normalized_code, display_name = self._get_base_index_info(index_code)
            
            logger.info(f"🔍 正在获取指数 {normalized_code} ({display_name}) 的数据...")
            
            # 检查是否为复合指数
            if self.config and self.config.is_composite_index(normalized_code):
                return self.calculate_composite_index(normalized_code, start_date, end_date)
            
            # 如果不是复合指数，直接从数据源获取
            # 如果没有指定日期范围，默认获取最近 DEFAULT_FETCH_YEARS 年的数据
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365 * _FETCH_YEARS)).strftime('%Y%m%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            
            index_data = None
            
            # 根据指数代码选择不同的接口
            if normalized_code == 'HSI':
                # 港股指数
                for symbol in ["HSI", "恒生指数"]:
                    try:
                        index_data = ak.stock_hk_index_daily_em(symbol=symbol)
                        break
                    except Exception as e:
                        logger.warning(f"使用 stock_hk_index_daily_em 获取 {symbol} 失败: {e}")

                if index_data is None:
                    logger.error(f"获取港股指数 {normalized_code} 所有方法都失败")
                    return None
            else:
                # A股指数
                try:
                    # 尝试主要接口
                    index_data = ak.index_zh_a_hist(symbol=normalized_code, period="daily", 
                                                  start_date=start_date, end_date=end_date)
                except Exception as e:
                    logger.warning(f"使用index_zh_a_hist获取指数 {normalized_code} 失败: {e}")
                    # 尝试备选接口
                    try:
                        # 尝试带市场后缀的代码
                        if self.config:
                            index_with_suffix = self.config.get_index_with_suffix(normalized_code)
                            if index_with_suffix != normalized_code:
                                # 尝试去掉后缀的最后部分（如.SH -> sh）
                                market_code = index_with_suffix.split('.')[-1].lower()
                                if market_code == 'sh':
                                    symbol = f"sh{normalized_code}"
                                elif market_code == 'sz':
                                    symbol = f"sz{normalized_code}"
                                else:
                                    symbol = normalized_code
                            else:
                                symbol = normalized_code
                        else:
                            symbol = normalized_code
                        
                        index_data = ak.stock_zh_index_daily_em(symbol=symbol)
                    except Exception as e2:
                        logger.error(f"获取A股指数 {normalized_code} 所有方法都失败: {e2}")
                        return None
            
            if index_data is None or index_data.empty:
                logger.warning(f"⚠️ 指数 {normalized_code} 没有获取到数据")
                return None
            
            # 标准化列名
            column_mapping = {
                'date': 'date',
                'Datetime': 'date', 
                '日期': 'date',
                '时间': 'date',
                'time': 'date',
                'Date': 'date',
                'trade_date': 'date',
                '收盘': 'close',
                '收市': 'close',
                '收市价': 'close',
                'close': 'close',
                'Close': 'close',
                '收盘价': 'close',
                'latest': 'close'
            }
            
            # 重命名现有列
            rename_dict = {}
            for col in index_data.columns:
                if col in column_mapping:
                    rename_dict[col] = column_mapping[col]
                elif col.lower() in column_mapping:
                    rename_dict[col] = column_mapping[col.lower()]
                elif col in column_mapping.values():  # 如果已经是标准名称则跳过
                    continue
            
            if rename_dict:
                index_data.rename(columns=rename_dict, inplace=True)
            
            # 确保必须的列存在
            if 'date' not in index_data.columns or 'close' not in index_data.columns:
                logger.warning(f"⚠️ 指数 {normalized_code} 返回列: {list(index_data.columns)}")
                logger.warning(f"⚠️ 指数 {normalized_code} 缺少必要列: date 或 close")
                return None
            
            # 确保日期列为datetime类型
            index_data['date'] = pd.to_datetime(index_data['date'])
            
            # 筛选指定日期范围内的数据
            index_data = index_data[
                (index_data['date'] >= pd.to_datetime(start_date)) & 
                (index_data['date'] <= pd.to_datetime(end_date))
            ].copy()
            
            if index_data.empty:
                logger.warning(f"⚠️ 指数 {normalized_code} 在指定日期范围内没有数据")
                return None
            
            # 按日期排序
            index_data.sort_values(by='date', inplace=True)
            index_data.reset_index(drop=True, inplace=True)
            
            logger.info(f"✅ 指数 {normalized_code} 数据获取成功，共 {len(index_data)} 条记录")
            
            # 添加指数代码列
            index_data['index_id'] = normalized_code
            
            return index_data
            
        except Exception as e:
            logger.error(f"❌ 获取指数 {index_code} 数据失败: {e}")
            return None
    
    def calculate_composite_index(self, composite_code: str, 
                                 start_date: Optional[str] = None,
                                 end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        计算复合指数
        
        Args:
            composite_code: 复合指数代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            复合指数的DataFrame
        """
        try:
            if not self.config:
                logger.error("无法获取配置信息")
                return None
            
            # 获取复合指数配置
            components = self.config.get_composite_components(composite_code)
            if not components:
                logger.error(f"复合指数 {composite_code} 没有配置成分")
                return None
            
            logger.info(f"🔬 正在计算复合指数 {composite_code}...")
            
            # 获取所有成分指数的数据
            component_data = {}
            for comp in components:
                base_code = comp['base_code']
                weight = comp.get('weight', 1.0)
                
                logger.info(f"  获取成分 {base_code} (权重: {weight})...")
                data = self.fetch_index_data(base_code, start_date, end_date)
                
                if data is None or data.empty:
                    logger.warning(f"  成分 {base_code} 数据获取失败")
                    continue
                
                component_data[base_code] = {
                    'data': data[['date', 'close']].copy(),
                    'weight': weight
                }
            
            if not component_data:
                logger.error(f"无法获取复合指数 {composite_code} 的任何成分数据")
                return None
            
            # 准备合并数据
            all_dates = None
            for code, comp_info in component_data.items():
                if all_dates is None:
                    all_dates = set(comp_info['data']['date'])
                else:
                    all_dates = all_dates.intersection(set(comp_info['data']['date']))
            
            if not all_dates:
                logger.error("无法找到成分指数的共同日期")
                return None
            
            # 转换为排序的日期列表
            all_dates = sorted(list(all_dates))
            
            # 创建结果DataFrame
            result_df = pd.DataFrame({'date': all_dates})
            
            # 为每个成分添加收益率列
            for code, comp_info in component_data.items():
                comp_df = comp_info['data']
                # 筛选共同日期
                comp_df = comp_df[comp_df['date'].isin(all_dates)].copy()
                comp_df.sort_values('date', inplace=True)
                
                # 计算收益率
                comp_df['return'] = comp_df['close'].pct_change()
                
                # 合并到结果DataFrame
                comp_df = comp_df[['date', 'return']]
                result_df = pd.merge(result_df, comp_df, on='date', how='left', suffixes=('', f'_{code}'))
            
            # 计算加权平均收益率
            return_columns = [col for col in result_df.columns if col.startswith('return_')]
            
            # 确保权重向量与收益率列匹配
            weights = []
            for col in return_columns:
                code = col.replace('return_', '')
                if code in component_data:
                    weights.append(component_data[code]['weight'])
                else:
                    weights.append(0)
            
            # 归一化权重
            total_weight = sum(weights)
            if total_weight > 0:
                weights = [w / total_weight for w in weights]
            
            # 计算组合收益率
            result_df['portfolio_return'] = 0
            for i, col in enumerate(return_columns):
                result_df['portfolio_return'] += result_df[col] * weights[i]
            
            # 从收益率计算净值序列（初始净值为1.0）
            result_df['close'] = (1 + result_df['portfolio_return']).cumprod()
            
            # 添加指数代码
            result_df['index_id'] = composite_code
            
            # 只保留需要的列
            result_df = result_df[['date', 'close', 'index_id']].copy()
            
            logger.info(f"✅ 复合指数 {composite_code} 计算成功，共 {len(result_df)} 条记录")
            return result_df
            
        except Exception as e:
            logger.error(f"❌ 计算复合指数 {composite_code} 失败: {e}")
            return None
    
    def fetch_all_indices_data(self, index_codes: List[str] = None) -> Dict[str, pd.DataFrame]:
        """
        获取多个指数的数据
        
        Args:
            index_codes: 指数代码列表，如果为None则使用配置中的基准指数
            
        Returns:
            字典，键为指数代码，值为对应的DataFrame
        """
        if index_codes is None:
            try:
                index_codes = self.config.get_actual_benchmark_codes()
            except:
                index_codes = ['000001', '000300', 'HSI']
        
        results = {}
        
        for idx_code in index_codes:
            # 标准化代码和获取显示名称
            normalized_code, display_name = self._get_base_index_info(idx_code)
            
            logger.info(f"📊 正在获取指数 {normalized_code} ({display_name}) 的数据...")
            
            # 获取单个指数数据
            index_data = self.fetch_index_data(idx_code)
            
            if index_data is not None:
                results[normalized_code] = index_data
            else:
                logger.warning(f"⚠️ 指数 {normalized_code} 数据获取失败")
            
            # 添加延时避免过于频繁的API调用
            time.sleep(0.5)
        
        logger.info(f"✅ 共成功获取 {len(results)} 个指数的数据")
        return results

if __name__ == "__main__":
    # 测试代码
    fetcher = IndexDataFetcher()
    
    # 测试基础指数
    test_codes = ['000300.SH', 'HSI.HK', '000001.SH']
    results = fetcher.fetch_all_indices_data(test_codes)
    
    for code, data in results.items():
        print(f"\n指数 {code} 的前5条数据:")
        print(data.head())
    
    # 测试复合指数
    print("\n" + "="*50)
    print("测试医疗创新指数计算:")
    medical_index = fetcher.calculate_composite_index('MED_INNOV')
    if medical_index is not None:
        print(medical_index.head())
    
    # 测试新复合指数
    print("\n测试自定义复合指数计算:")
    custom_index = fetcher.calculate_composite_index('CUSTOM_COMPOSITE')
    if custom_index is not None:
        print(custom_index.head())