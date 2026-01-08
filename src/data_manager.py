"""
DataManager - 資料管理模組 (修改版：支援進貨資料和加班邏輯)
負責載入、驗證、預處理所有master data和transaction data
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, Optional, Tuple
import sys
from datetime import datetime, date

# 修正：使用動態路徑設定
try:
    from config import MASTER_DATA_FILES, TRANSACTION_DATA_FILES
except ImportError:
    # 如果config.py不存在，使用預設路徑
    MASTER_DATA_FILES = {
        'system_parameters': Path('data/master_data/system_parameters.csv'),
        'item_master': Path('data/master_data/item_master.csv'),
        'staff_skill_master': Path('data/master_data/staff_skill_master.csv'),
        'workstation_capacity': Path('data/master_data/workstation_capacity.csv'),
        'route_schedule_master': Path('data/master_data/route_schedule_master.csv'),
        'item_inventory': Path('data/master_data/item_inventory.csv'),
        'branch_route_master': Path('data/master_data/branch_route_master.csv')
    }
    
    TRANSACTION_DATA_FILES = {
        'historical_orders': Path('data/transaction_data/historical_orders.csv'),
        'historical_receiving': Path('data/transaction_data/historical_receiving.csv')  # 🆕 確保包含進貨資料
    }

class DataManager:
    def __init__(self):
        """初始化DataManager"""
        self.logger = logging.getLogger(__name__)
        self.master_data = {}
        self.transaction_data = {}
        self.validation_results = {}
        
        # 🆕 新增：進貨相關資料追蹤
        self.receiving_data_available = False
        self.receiving_date_range = None
        
    def load_master_data(self) -> Dict[str, pd.DataFrame]:
        """載入所有master data檔案"""
        self.logger.info("開始載入Master Data...")
        
        for data_name, file_path in MASTER_DATA_FILES.items():
            try:
                if file_path.exists():
                    # 嘗試不同編碼
                    encodings_to_try = ['utf-8', 'cp1252', 'gbk', 'big5']
                    df = None
                    
                    for encoding in encodings_to_try:
                        try:
                            df = pd.read_csv(file_path, encoding=encoding)
                            self.logger.info(f"✅ 載入 {data_name} (編碼: {encoding}): {len(df)} 筆資料")
                            break
                        except UnicodeDecodeError:
                            continue
                    
                    if df is not None:
                        # 🆕 新增：數據清理
                        df = self._clean_master_data(df, data_name)
                        
                        # 特殊處理：系統參數資料
                        if data_name == 'system_parameters':
                            df = self._validate_system_parameters(df)
                        
                        self.master_data[data_name] = df
                    else:
                        self.logger.error(f"❌ 無法以任何編碼載入 {data_name}")
                        
                else:
                    self.logger.warning(f"⚠️  檔案不存在: {file_path}")
                    
            except Exception as e:
                self.logger.error(f"❌ 載入 {data_name} 失敗: {str(e)}")
                
        return self.master_data
        
    def load_transaction_data(self, start_date: str = None, end_date: str = None, 
                            filter_valid_items: bool = True) -> Dict[str, pd.DataFrame]:
        """🔧 修改：載入交易資料（強化進貨資料處理）"""
        self.logger.info(f"開始載入Transaction Data (日期範圍: {start_date} - {end_date})...")
        
        for data_name, file_path in TRANSACTION_DATA_FILES.items():
            try:
                if file_path.exists():
                    df = pd.read_csv(file_path, encoding='utf-8')
                    
                    # 🆕 特殊處理：進貨資料
                    if data_name == 'historical_receiving':
                        df = self._process_receiving_data(df, start_date, end_date)
                        self.receiving_data_available = len(df) > 0
                        
                        if self.receiving_data_available:
                            self._analyze_receiving_data_range(df)
                    
                    # 日期篩選邏輯（如果提供）
                    if start_date or end_date:
                        df = self._filter_by_date(df, start_date, end_date, data_name)
                    
                    # 零件過濾
                    if filter_valid_items:
                        df = self.filter_valid_items(df)
                    
                    self.transaction_data[data_name] = df
                    self.logger.info(f"✅ 載入 {data_name}: {len(df)} 筆資料")
                else:
                    self.logger.warning(f"⚠️  檔案不存在: {file_path}")
                    
            except Exception as e:
                self.logger.error(f"❌ 載入 {data_name} 失敗: {str(e)}")
                
        return self.transaction_data
    
    def _validate_system_parameters(self, params_df: pd.DataFrame) -> pd.DataFrame:
        """🆕 驗證系統參數完整性"""
        
        # 檢查必要欄位
        required_columns = ['parameter_name', 'parameter_value', 'data_type']
        missing_columns = [col for col in required_columns if col not in params_df.columns]
        
        if missing_columns:
            self.logger.error(f"系統參數檔案缺少必要欄位: {missing_columns}")
            return params_df
        
        # 檢查新增的必要參數
        required_new_params = [
            'receiving_completion_days',
            'shift_start_time', 
            'shift_end_time',
            'overtime_enabled'
        ]
        
        existing_params = params_df['parameter_name'].tolist()
        missing_params = [param for param in required_new_params if param not in existing_params]
        
        if missing_params:
            self.logger.warning(f"系統參數檔案缺少新增參數: {missing_params}")
            self.logger.info("請執行 system_parameters_update.py 更新參數檔案")
        
        # 驗證重要參數的資料型態
        type_validations = {
            'receiving_completion_days': 'integer',
            'max_overtime_hours': 'float',
            'overtime_enabled': 'string'
        }
        
        for param_name, expected_type in type_validations.items():
            param_row = params_df[params_df['parameter_name'] == param_name]
            if len(param_row) > 0:
                actual_type = param_row.iloc[0]['data_type']
                if actual_type != expected_type:
                    self.logger.warning(f"參數 {param_name} 資料型態不符: 期望 {expected_type}, 實際 {actual_type}")
        
        return params_df
    
    def _process_receiving_data(self, receiving_df: pd.DataFrame, start_date: str = None,end_date: str = None) -> pd.DataFrame:
        # 處理不同的數量欄位名稱
        if 'INVQTY' in receiving_df.columns and 'QTY' not in receiving_df.columns:
            receiving_df['QTY'] = receiving_df['INVQTY']
        
        """🆕 處理進貨資料"""
        self.logger.info(f"處理進貨資料: {len(receiving_df)} 筆原始資料")
        
        if len(receiving_df) == 0:
            return receiving_df
        
        # 檢查必要欄位
        required_columns = ['DATE', 'FRCD', 'PARTNO', 'QTY']
        missing_columns = [col for col in required_columns if col not in receiving_df.columns]
        
        # 🔧 新增：處理不同的數量欄位名稱
        if 'INVQTY' in receiving_df.columns and 'QTY' not in receiving_df.columns:
            receiving_df['QTY'] = receiving_df['INVQTY']
            self.logger.info("將 INVQTY 欄位重新命名為 QTY")

        # 重新檢查必要欄位
        missing_columns = [col for col in required_columns if col not in receiving_df.columns]

        if missing_columns:
            self.logger.error(f"進貨資料缺少必要欄位: {missing_columns}")
            return pd.DataFrame()  # 返回空DataFrame
        
        # 處理DATE欄位
        try:
            # 嘗試轉換日期格式
            if receiving_df['DATE'].dtype == 'object':
                # 處理字串格式的日期
                receiving_df['DATE'] = pd.to_datetime(receiving_df['DATE'], errors='coerce')
            
            # 移除無效日期的記錄
            invalid_dates = receiving_df['DATE'].isna()
            if invalid_dates.sum() > 0:
                self.logger.warning(f"移除 {invalid_dates.sum()} 筆無效日期的進貨記錄")
                receiving_df = receiving_df[~invalid_dates]
            
        except Exception as e:
            self.logger.error(f"處理進貨日期時發生錯誤: {str(e)}")
            return pd.DataFrame()
        
        # 處理QTY欄位
        try:
            receiving_df['QTY'] = pd.to_numeric(receiving_df['QTY'], errors='coerce')
            receiving_df['QTY'] = receiving_df['QTY'].fillna(0).astype(int)
            
            # 移除數量為0或負數的記錄
            invalid_qty = receiving_df['QTY'] <= 0
            if invalid_qty.sum() > 0:
                self.logger.warning(f"移除 {invalid_qty.sum()} 筆無效數量的進貨記錄")
                receiving_df = receiving_df[~invalid_qty]
                
        except Exception as e:
            self.logger.error(f"處理進貨數量時發生錯誤: {str(e)}")
        
        # 🆕 新增：如果沒有RECEIVING_ID，自動生成
        if 'RECEIVING_ID' not in receiving_df.columns:
            receiving_df['RECEIVING_ID'] = range(1, len(receiving_df) + 1)
            receiving_df['RECEIVING_ID'] = 'RCV_' + receiving_df['RECEIVING_ID'].astype(str).str.zfill(6)
        
        self.logger.info(f"✅ 進貨資料處理完成: {len(receiving_df)} 筆有效資料")
        
        return receiving_df
    
    def _analyze_receiving_data_range(self, receiving_df: pd.DataFrame):
        """🆕 分析進貨資料的日期範圍"""
        if len(receiving_df) == 0 or 'DATE' not in receiving_df.columns:
            return
        
        try:
            min_date = receiving_df['DATE'].min()
            max_date = receiving_df['DATE'].max()
            
            self.receiving_date_range = {
                'start_date': min_date,
                'end_date': max_date,
                'total_days': (max_date - min_date).days + 1,
                'record_count': len(receiving_df)
            }
            
            self.logger.info(f"📊 進貨資料範圍: {min_date.strftime('%Y-%m-%d')} 到 {max_date.strftime('%Y-%m-%d')} ({self.receiving_date_range['total_days']} 天)")
            
            # 按日期統計進貨筆數
            daily_counts = receiving_df.groupby(receiving_df['DATE'].dt.date).size()
            avg_daily = daily_counts.mean()
            
            self.logger.info(f"📊 平均每日進貨: {avg_daily:.1f} 筆")
            
            # 識別進貨高峰日
            peak_days = daily_counts[daily_counts > avg_daily * 1.5].head(5)
            if len(peak_days) > 0:
                self.logger.info(f"📊 進貨高峰日: {list(peak_days.index)}")
                
        except Exception as e:
            self.logger.error(f"分析進貨資料範圍時發生錯誤: {str(e)}")
    
    def _filter_by_date(self, df: pd.DataFrame, start_date: str, end_date: str, data_name: str, workdays_only: bool = True) -> pd.DataFrame:
        """🔧 修改：根據日期篩選資料（支援不同資料類型）"""
        
        # 確定日期欄位名稱
        date_column = None
        possible_date_columns = ['DATE', 'date', 'Date', 'ORDERDATE', 'ORDER_DATE']
        
        for col in possible_date_columns:
            if col in df.columns:
                date_column = col
                break
        
        if not date_column:
            self.logger.warning(f"{data_name} 找不到日期欄位，跳過日期篩選")
            return df
        
        try:
            # 確保日期欄位是datetime格式
            if df[date_column].dtype != 'datetime64[ns]':
                df[date_column] = pd.to_datetime(df[date_column])
            
            original_count = len(df)
            
            if start_date:
                start_dt = pd.to_datetime(start_date)
                df = df[df[date_column] >= start_dt]
                
            if end_date:
                end_dt = pd.to_datetime(end_date)
                df = df[df[date_column] <= end_dt]
            
            filtered_count = len(df)
            
            if filtered_count < original_count:
                self.logger.info(f"🗓️ {data_name} 日期篩選: {original_count} → {filtered_count} 筆")
            
        except Exception as e:
            self.logger.error(f"日期篩選失敗 ({data_name}): {str(e)}")

        # 🆕 新增：工作日篩選
        if workdays_only and date_column:
            pre_workday_count = len(df)
            workday_mask = df[date_column].apply(lambda x: self.is_workday(x))
            df = df[workday_mask]
            post_workday_count = len(df)
            
            if post_workday_count < pre_workday_count:
                weekend_filtered = pre_workday_count - post_workday_count
                self.logger.info(f"📅 {data_name} 工作日篩選: 移除 {weekend_filtered} 筆週末資料")
        
        return df
    
    def validate_data_consistency(self) -> Dict[str, bool]:
        """🔧 修改：檢查資料一致性（新增進貨資料檢查）"""
        self.logger.info("開始檢查資料一致性...")
        
        validation_results = {}
        
        # 檢查Master Data完整性
        validation_results['master_data_complete'] = self._validate_master_data_complete()
        
        # 檢查關聯性
        if 'item_master' in self.master_data and 'item_inventory' in self.master_data:
            validation_results['item_consistency'] = self._validate_item_consistency()
        
        # 🆕 新增：檢查進貨資料一致性
        if 'historical_receiving' in self.transaction_data:
            validation_results['receiving_data_valid'] = self._validate_receiving_data()
        
        # 檢查參數合理性
        if 'system_parameters' in self.master_data:
            validation_results['parameters_reasonable'] = self._validate_parameters()
        
        # 🆕 新增：檢查進貨與出貨資料的時間重疊
        if ('historical_orders' in self.transaction_data and 
            'historical_receiving' in self.transaction_data):
            validation_results['data_time_overlap'] = self._validate_data_time_overlap()
        
        self.validation_results = validation_results
        
        # 輸出驗證結果
        for check_name, result in validation_results.items():
            status = "✅ 通過" if result else "❌ 失敗"
            self.logger.info(f"{check_name}: {status}")
            
        return validation_results
    
    def _validate_receiving_data(self) -> bool:
        """🆕 驗證進貨資料完整性"""
        try:
            receiving_df = self.transaction_data['historical_receiving']
            
            if len(receiving_df) == 0:
                self.logger.warning("進貨資料為空")
                return False
            
            # 檢查必要欄位
            required_columns = ['DATE', 'FRCD', 'PARTNO', 'QTY']
            missing_columns = [col for col in required_columns if col not in receiving_df.columns]
            
            if missing_columns:
                self.logger.error(f"進貨資料缺少必要欄位: {missing_columns}")
                return False
            
            # 檢查資料品質
            null_counts = receiving_df[required_columns].isnull().sum()
            total_nulls = null_counts.sum()
            
            if total_nulls > 0:
                self.logger.warning(f"進貨資料有 {total_nulls} 個空值: {dict(null_counts)}")
            
            # 檢查進貨零件是否在item_master中
            if 'item_master' in self.master_data:
                item_master = self.master_data['item_master']
                valid_items = set(zip(item_master['frcd'], item_master['partno']))
                receiving_items = set(zip(receiving_df['FRCD'], receiving_df['PARTNO']))
                
                invalid_items = receiving_items - valid_items
                if invalid_items:
                    self.logger.warning(f"進貨資料中有 {len(invalid_items)} 個零件不在item_master中")
                    # 只警告，不視為失敗
            
            return True
            
        except Exception as e:
            self.logger.error(f"進貨資料驗證失敗: {str(e)}")
            return False
    
    def _validate_data_time_overlap(self) -> bool:
        """🆕 檢查進貨與出貨資料的時間重疊"""
        try:
            orders_df = self.transaction_data['historical_orders']
            receiving_df = self.transaction_data['historical_receiving']
            
            # 取得各自的日期範圍
            orders_dates = pd.to_datetime(orders_df['DATE'] if 'DATE' in orders_df.columns 
                                        else orders_df.iloc[:, 0])  # 假設第一欄是日期
            receiving_dates = pd.to_datetime(receiving_df['DATE'])
            
            orders_range = (orders_dates.min(), orders_dates.max())
            receiving_range = (receiving_dates.min(), receiving_dates.max())
            
            # 檢查是否有重疊
            overlap_start = max(orders_range[0], receiving_range[0])
            overlap_end = min(orders_range[1], receiving_range[1])
            
            has_overlap = overlap_start <= overlap_end
            
            if has_overlap:
                overlap_days = (overlap_end - overlap_start).days + 1
                self.logger.info(f"📊 進貨與出貨資料重疊 {overlap_days} 天 ({overlap_start.strftime('%Y-%m-%d')} - {overlap_end.strftime('%Y-%m-%d')})")
            else:
                self.logger.warning("⚠️ 進貨與出貨資料沒有時間重疊")
            
            return has_overlap
            
        except Exception as e:
            self.logger.error(f"時間重疊檢查失敗: {str(e)}")
            return False
    
    def _validate_master_data_complete(self) -> bool:
        """檢查Master Data是否完整"""
        required_files = ['system_parameters', 'item_master', 'workstation_capacity']
        return all(data_name in self.master_data for data_name in required_files)
    
    def _validate_item_consistency(self) -> bool:
        """檢查零件資料一致性"""
        try:
            item_master = self.master_data['item_master']
            item_inventory = self.master_data['item_inventory']
            
            # 檢查零件代碼是否一致
            master_items = set(zip(item_master['frcd'], item_master['partno']))
            inventory_items = set(zip(item_inventory['frcd'], item_inventory['partno']))
            
            missing_in_inventory = master_items - inventory_items
            missing_in_master = inventory_items - master_items
            
            if missing_in_inventory:
                self.logger.warning(f"庫存中缺少的零件: {len(missing_in_inventory)} 個")
            if missing_in_master:
                self.logger.warning(f"主檔中缺少的零件: {len(missing_in_master)} 個")
                
            return len(missing_in_inventory) == 0 and len(missing_in_master) == 0
            
        except Exception as e:
            self.logger.error(f"零件一致性檢查失敗: {str(e)}")
            return False
    
    def _validate_parameters(self) -> bool:
        """🔧 修改：檢查系統參數合理性（包含新參數）"""
        try:
            params = self.master_data['system_parameters']
            
            # 基本必要參數
            required_params = [
                'daily_work_hours',
                'picking_base_time_repack',
                'picking_base_time_no_repack'
            ]
            
            # 🆕 新增的必要參數
            new_required_params = [
                'receiving_completion_days',
                'shift_start_time',
                'shift_end_time'
            ]
            
            all_required = required_params + new_required_params
            param_names = params['parameter_name'].tolist()
            missing_params = [p for p in all_required if p not in param_names]
            
            if missing_params:
                self.logger.error(f"缺少必要參數: {missing_params}")
                return False
            
            # 🆕 檢查新參數的合理性
            validation_checks = {
                'receiving_completion_days': lambda x: 1 <= int(x) <= 7,  # 1-7天
                'max_overtime_hours': lambda x: 0.5 <= float(x) <= 6.0,   # 0.5-6小時
            }
            
            for param_name, validator in validation_checks.items():
                param_row = params[params['parameter_name'] == param_name]
                if len(param_row) > 0:
                    try:
                        value = param_row.iloc[0]['parameter_value']
                        if not validator(value):
                            self.logger.warning(f"參數值不合理: {param_name} = {value}")
                    except Exception as e:
                        self.logger.warning(f"參數驗證失敗: {param_name} - {str(e)}")
                        
            return True
            
        except Exception as e:
            self.logger.error(f"參數檢查失敗: {str(e)}")
            return False
    
    def get_parameter_value(self, parameter_name: str, default_value=None):
        """取得系統參數值"""
        if 'system_parameters' not in self.master_data:
            return default_value
            
        params = self.master_data['system_parameters']
        param_row = params[params['parameter_name'] == parameter_name]
        
        if len(param_row) == 0:
            self.logger.warning(f"參數 {parameter_name} 不存在，使用預設值: {default_value}")
            return default_value
            
        value = param_row.iloc[0]['parameter_value']
        data_type = param_row.iloc[0]['data_type']
        
        # 根據資料型態轉換
        try:
            if data_type == 'integer':
                return int(value)
            elif data_type == 'float':
                return float(value)
            else:
                return str(value)
        except ValueError:
            self.logger.warning(f"參數 {parameter_name} 值轉換失敗，使用預設值: {default_value}")
            return default_value

    def filter_valid_items(self, transaction_df: pd.DataFrame) -> pd.DataFrame:
        """過濾有效零件（只保留item_master中存在的零件）"""
        if 'item_master' not in self.master_data:
            self.logger.warning("item_master未載入，無法過濾零件")
            return transaction_df
        
        # 取得有效零件清單
        item_master = self.master_data['item_master']
        valid_items = set(zip(item_master['frcd'], item_master['partno']))
        
        # 過濾前的資料量
        original_count = len(transaction_df)
        
        # 檢查transaction_df是否有frcd和partno欄位
        if 'FRCD' in transaction_df.columns and 'PARTNO' in transaction_df.columns:
            # 建立過濾條件（注意欄位名稱大小寫）
            transaction_items = list(zip(transaction_df['FRCD'], transaction_df['PARTNO']))
            valid_mask = [item in valid_items for item in transaction_items]
            
            # 應用過濾
            filtered_df = transaction_df[valid_mask].copy()
            
            filtered_count = len(filtered_df)
            removed_count = original_count - filtered_count
            
            if removed_count > 0:
                self.logger.info(f"零件過濾: 原始 {original_count} 筆，移除 {removed_count} 筆無效零件，保留 {filtered_count} 筆")
            else:
                self.logger.info(f"零件過濾: 所有 {original_count} 筆資料都是有效零件")
            
            return filtered_df
        elif 'frcd' in transaction_df.columns and 'partno' in transaction_df.columns:
            # 小寫欄位名稱版本
            transaction_items = list(zip(transaction_df['frcd'], transaction_df['partno']))
            valid_mask = [item in valid_items for item in transaction_items]
            
            filtered_df = transaction_df[valid_mask].copy()
            
            filtered_count = len(filtered_df)
            removed_count = original_count - filtered_count
            
            if removed_count > 0:
                self.logger.info(f"零件過濾: 原始 {original_count} 筆，移除 {removed_count} 筆無效零件，保留 {filtered_count} 筆")
            
            return filtered_df
        else:
            self.logger.warning("交易資料缺少frcd/FRCD或partno/PARTNO欄位，無法過濾")
            return transaction_df

    def get_valid_items_summary(self) -> Dict:
        """取得有效零件摘要統計"""
        if 'item_master' not in self.master_data:
            return {}
        
        item_master = self.master_data['item_master']
        
        # 按樓層統計
        floor_stats = item_master['floor'].value_counts().to_dict()
        
        # 按零件前碼統計
        frcd_stats = item_master['frcd'].value_counts().head(10).to_dict()
        
        return {
            'total_valid_items': len(item_master),
            'items_by_floor': floor_stats,
            'top_10_frcd': frcd_stats,
            'unique_frcd_count': item_master['frcd'].nunique(),
            'repack_ratio': (item_master['repack'] == 'Y').mean() if 'repack' in item_master.columns else 0
        }
    
    def get_receiving_data_summary(self) -> Dict:
        """🆕 取得進貨資料摘要"""
        if not self.receiving_data_available:
            return {'available': False, 'message': '無進貨資料'}
        
        receiving_df = self.transaction_data['historical_receiving']
        
        summary = {
            'available': True,
            'total_records': len(receiving_df),
            'date_range': self.receiving_date_range,
            'unique_items': len(receiving_df[['FRCD', 'PARTNO']].drop_duplicates()),
            'total_quantity': receiving_df['QTY'].sum(),
            'avg_daily_records': 0,
            'top_item_types': {},
            'quantity_distribution': {}
        }
        
        # 計算平均每日筆數
        if self.receiving_date_range:
            summary['avg_daily_records'] = round(
                summary['total_records'] / self.receiving_date_range['total_days'], 1
            )
        
        # 統計最常見的零件類型
        summary['top_item_types'] = receiving_df['FRCD'].value_counts().head(5).to_dict()
        
        # 數量分布
        summary['quantity_distribution'] = {
            'min': int(receiving_df['QTY'].min()),
            'max': int(receiving_df['QTY'].max()),
            'mean': round(receiving_df['QTY'].mean(), 1),
            'median': int(receiving_df['QTY'].median())
        }
        
        return summary
    
    def export_data_summary(self) -> Dict:
        """🆕 匯出完整的資料摘要"""
        summary = {
            'master_data': {},
            'transaction_data': {},
            'validation_results': self.validation_results,
            'export_time': datetime.now().isoformat()
        }
        
        # Master Data摘要
        for data_name, df in self.master_data.items():
            summary['master_data'][data_name] = {
                'record_count': len(df),
                'columns': list(df.columns),
                'file_size_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
            }
        
        # Transaction Data摘要
        for data_name, df in self.transaction_data.items():
            summary['transaction_data'][data_name] = {
                'record_count': len(df),
                'columns': list(df.columns),
                'file_size_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
            }
            
            # 特殊處理：日期範圍
            if 'DATE' in df.columns:
                date_col = pd.to_datetime(df['DATE'])
                summary['transaction_data'][data_name]['date_range'] = {
                    'start': date_col.min().strftime('%Y-%m-%d'),
                    'end': date_col.max().strftime('%Y-%m-%d'),
                    'days': (date_col.max() - date_col.min()).days + 1
                }
        
        # 進貨資料特殊摘要
        if self.receiving_data_available:
            summary['receiving_summary'] = self.get_receiving_data_summary()
        
        return summary
    
    @staticmethod
    def is_workday(target_date):
        """
        判斷是否為工作日（週一到週五）
        
        Args:
            target_date: datetime.date 或 datetime.datetime 物件
        
        Returns:
            bool: True 為工作日，False 為週末
        """
        if hasattr(target_date, 'weekday'):
            weekday = target_date.weekday()  # 0=週一, 6=週日
            return weekday < 5  # 0-4 為週一到週五
        return False
    
    def _clean_master_data(self, df: pd.DataFrame, data_name: str) -> pd.DataFrame:
        """🆕 新增：清理master data"""
        try:
            # 移除所有欄位的前後空格
            for col in df.columns:
                if df[col].dtype == 'object':  # 只處理文字欄位
                    df[col] = df[col].astype(str).str.strip()
            
            # 特別處理 route_schedule_master
            if data_name == 'route_schedule_master':
                # 清理可能的空格問題
                if 'PARTCUSTID' in df.columns:
                    df['PARTCUSTID'] = df['PARTCUSTID'].str.strip()
                
                if 'ROUTECD' in df.columns:
                    df['ROUTECD'] = df['ROUTECD'].str.strip()
                
                # 確保時間欄位是數字格式
                for time_col in ['ORDERENDTIME', 'DELIVERTM']:
                    if time_col in df.columns:
                        # 移除空格並轉為字串
                        df[time_col] = df[time_col].astype(str).str.strip()
                        # 移除 'nan' 字串
                        df[time_col] = df[time_col].replace('nan', '')
                        
                        self.logger.debug(f"清理 {time_col} 欄位完成")
                
                self.logger.info(f"route_schedule_master 數據清理完成")
            
            return df
            
        except Exception as e:
            self.logger.warning(f"清理 {data_name} 數據時發生錯誤: {str(e)}")
            return df