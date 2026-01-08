"""
OrderPriorityManager - 訂單優先權管理模組 (修正時間邏輯版本)
負責處理訂單分類和優先權動態管理
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

class OrderPriorityManager:
    def __init__(self, data_manager):
        """初始化訂單優先權管理器"""
        self.logger = logging.getLogger(__name__)
        self.data_manager = data_manager
        self.route_schedule = data_manager.master_data.get('route_schedule_master')
        
        # 載入優先權相關參數
        self._load_priority_parameters()
        
    def _load_priority_parameters(self):
        """載入優先權相關參數"""
        self.params = {
            'urgent_transcd_list': self.data_manager.get_parameter_value('urgent_transcd_list', '3,6,8,A'),
            'normal_transcd_list': self.data_manager.get_parameter_value('normal_transcd_list', '1,2,4,5,7,9,C,D,E,F'),
            'sub_warehouse_routes': self.data_manager.get_parameter_value('sub_warehouse_routes', 'SDTC,SDHN'),
            'receiving_normal_priority': self.data_manager.get_parameter_value('receiving_normal_priority', 'P4'),
            'receiving_urgent_priority': self.data_manager.get_parameter_value('receiving_urgent_priority', 'P1')
        }
        
        # 轉換為列表格式
        self.urgent_transcd = [x.strip() for x in self.params['urgent_transcd_list'].split(',')]
        self.normal_transcd = [x.strip() for x in self.params['normal_transcd_list'].split(',')]
        self.sub_warehouse_routes = [x.strip() for x in self.params['sub_warehouse_routes'].split(',')]
        
        self.logger.info(f"優先權參數載入完成:")
        self.logger.info(f"緊急TRANSCD: {self.urgent_transcd}")
        self.logger.info(f"一般TRANSCD: {self.normal_transcd}")
        self.logger.info(f"副倉路線: {self.sub_warehouse_routes}")
    
    def classify_order_priority(self, order_row: pd.Series) -> Tuple[str, str, str]:
        """分類訂單優先權
        
        Returns:
            Tuple[priority_level, order_type, urgency_reason]
        """
        transcd = str(order_row.get('TRANSCD', ''))
        routecd = str(order_row.get('ROUTECD', ''))
        
        # 🔧 修改：副倉庫判斷邏輯（完整的副倉庫識別）
        partcustid = str(order_row.get('PARTCUSTID', ''))

        # 方法1: ROUTECD 直接是副倉庫代碼
        if routecd in ['SDTC', 'SDHN']:
            return 'P3', 'SUB_WAREHOUSE', f'副倉路線({routecd})'

        # 方法2: R15/R16 + SDTC/SDHN 組合
        if routecd in ['R15'] and partcustid in ['SDTC']:
            return 'P3', 'SUB_WAREHOUSE', f'副倉組合({routecd}-{partcustid})'
        elif routecd in ['R16'] and partcustid in ['SDHN']:
            return 'P3', 'SUB_WAREHOUSE', f'副倉組合({routecd}-{partcustid})'
        
        # 判斷緊急程度
        if transcd in self.normal_transcd:
            return 'P1', 'NORMAL', f'一般TRANSCD({transcd})'  # P1 = 最高優先權
        elif transcd in self.urgent_transcd:
            return 'P2', 'URGENT', f'緊急TRANSCD({transcd})'  # P2 = 第二優先權
        else:
            # 其他TRANSCD與4相同優先權
            return 'P2', 'OTHER', f'其他TRANSCD({transcd})'
    
    def calculate_deadline(self, order_row: pd.Series, order_id: str = None) -> Dict:
        """🔧 修改：根據新的資料結構計算訂單截止時間"""
        routecd = str(order_row.get('ROUTECD', ''))
        partcustid = str(order_row.get('PARTCUSTID', ''))
        order_time_str = str(order_row.get('TIME', ''))
        
        # 如果沒有提供訂單編號，嘗試從 order_row 中取得
        if order_id is None:
            order_id = str(order_row.get('INDEXNO', 'Unknown'))
        
        # 🔧 新增：副倉庫邏輯 - 如果是副倉庫路線，不需要查找時刻表
        if routecd in self.sub_warehouse_routes:
            self.logger.info(f"副倉庫路線 {routecd}，跳過班次時刻表檢查 (訂單: {order_id})")
            return self._create_sub_warehouse_deadline_result(order_row, order_time_str)
        
        # 🔧 新邏輯：使用 PARTCUSTID 從 route_schedule_master 查找對應的時刻表
        if self.route_schedule is None:
            self.logger.warning(f"訂單 {order_id}: route_schedule_master 未載入")
            return self._empty_deadline_result()
        
        # 根據 ROUTECD 和 PARTCUSTID 查找時刻表
        matching_schedules = self.route_schedule[
            (self.route_schedule['ROUTECD'] == routecd) & 
            (self.route_schedule['PARTCUSTID'] == partcustid)
        ]
        
        if len(matching_schedules) == 0:
            self.logger.warning(f"訂單 {order_id}: 找不到路線 {routecd} 據點 {partcustid} 的時刻表")
            return self._empty_deadline_result()
        
        # 取得時刻表資訊
        schedule_info = matching_schedules.iloc[0]
        
        # 解析訂單時間
        order_time = self._parse_time_string(order_time_str)
        if not order_time:
            self.logger.warning(f"訂單 {order_id}: 訂單時間格式錯誤: '{order_time_str}'")
            return self._empty_deadline_result()
        
        # 處理時間格式
        try:
            order_cutoff = self._parse_time_number(schedule_info['ORDERENDTIME'])
            delivery_time = self._parse_time_number(schedule_info['DELIVERTM'])
            
            if not order_cutoff or not delivery_time:
                self.logger.warning(f"訂單 {order_id}: 時刻表時間格式錯誤")
                return self._empty_deadline_result()
            
            # 計算可用作業時間（分鐘）
            available_minutes = self._calculate_available_minutes(order_time, delivery_time)

            # 🆕 新增：時間合理性檢查
            if available_minutes is None:
                self.logger.warning(f"訂單 {order_id}: 時間不合理，標記為無效")
                return {
                    'order_cutoff_time': None,
                    'delivery_time': None,
                    'order_time': order_time,
                    'available_minutes': None,
                    'is_late_order': True,  # 標記為遲到
                    'partcustid': partcustid,
                    'schedule_found': False,  # 🔧 修改：時間不合理視為未找到時刻表
                    'time_invalid': True    # 🆕 新增：時間無效標記
                }
            
            # 判斷是否遲到（基於截止時間）
            is_late = self._is_order_late_simple(order_time, order_cutoff)
            
            return {
                'order_cutoff_time': order_cutoff,
                'delivery_time': delivery_time,
                'order_time': order_time,
                'available_minutes': available_minutes,
                'is_late_order': is_late,
                'partcustid': partcustid,
                'schedule_found': True
            }
            
        except Exception as e:
            self.logger.warning(f"訂單 {order_id}: 時間解析錯誤: {str(e)}")
            return self._empty_deadline_result()
    
    def _is_order_late_simple(self, order_time: time, order_cutoff: time) -> bool:
        """簡化的遲到判斷"""
        if not order_time or not order_cutoff:
            return False
        
        order_minutes = order_time.hour * 60 + order_time.minute
        cutoff_minutes = order_cutoff.hour * 60 + order_cutoff.minute
        
        return order_minutes > cutoff_minutes

    def _calculate_available_minutes(self, order_time: time, delivery_time: time) -> Optional[int]:
        if not order_time or not delivery_time:
            return None
        
        order_seconds = order_time.hour * 3600 + order_time.minute * 60 + order_time.second
        delivery_seconds = delivery_time.hour * 3600 + delivery_time.minute * 60
        
        if delivery_seconds >= order_seconds:
            # 同日內，正常計算
            available_seconds = delivery_seconds - order_seconds
        else:
            # 🔧 修正：加入合理性檢查
            time_diff_hours = (order_seconds - delivery_seconds) / 3600
            
            # 如果訂單時間晚於出車時間超過6小時，視為不合理
            if time_diff_hours > 6:
                self.logger.warning(f"訂單時間 {order_time} 晚於出車時間 {delivery_time} 超過 {time_diff_hours:.1f} 小時，視為無效")
                return None
            
            # 只有在合理範圍內才進行跨日計算（例如：23:00訂單要趕明天07:00的車）
            if order_time.hour >= 20 and delivery_time.hour <= 12:
                # 合理的跨日情況
                available_seconds = (24 * 3600 - order_seconds) + delivery_seconds
            else:
                # 不合理的時間組合，拒絕
                self.logger.warning(f"不合理的時間組合：訂單時間 {order_time} vs 出車時間 {delivery_time}")
                return None
        
        available_minutes = max(0, available_seconds // 60)
        return int(available_minutes)


    def _is_order_late(self, order_time: time, order_cutoff: time, is_rescheduled: bool) -> bool:
        """ 修正：判斷訂單是否遲到（處理跨日情況）"""
        if not order_time or not order_cutoff:
            return False
        
        # 如果已經重新安排班次，則認為是遲到
        if is_rescheduled:
            return True
        
        order_minutes = order_time.hour * 60 + order_time.minute
        cutoff_minutes = order_cutoff.hour * 60 + order_cutoff.minute
        
        #  修正：處理跨日情況
        if cutoff_minutes >= order_minutes:
            # 同日內：直接比較
            return order_minutes > cutoff_minutes
        else:
            # 跨日情況：檢查是否有足夠的跨日時間
            # 從訂單時間到隔日截止時間的總時間
            cross_day_minutes = (24 * 60 - order_minutes) + cutoff_minutes
            
            # 如果跨日總時間太短（比如少於4小時），可能確實是遲到
            minimum_cross_day_time = 4 * 60  # 4小時
            return cross_day_minutes < minimum_cross_day_time
    
    
    def _can_catch_delivery_precise(self, order_seconds: int, delivery_seconds: int) -> bool:
        """ 修正：精確的時間判斷（解決同一時間和跨日邏輯問題）"""
        
        time_diff = delivery_seconds - order_seconds
        
        if time_diff > 0:
            # 同日內，有時間差，可以趕上
            return True
        elif time_diff == 0:
            # 完全同一時間，可以趕上（重要修正）
            return True
        elif time_diff >= -60:
            # 1分鐘內的微小超時，仍然不能趕上（避免TIME03錯誤）
            return False
        else:
            # 真正的跨日情況（例如：23:59 → 隔日10:00）
            available_seconds = (24 * 3600) + time_diff
            # 跨日情況需要合理的時間（至少2小時）
            return available_seconds >= 2 * 3600
    

    
    def _empty_deadline_result(self) -> Dict:
        """返回空的截止時間結果"""
        return {
            'order_cutoff_time': None,
            'delivery_time': None,
            'order_time': None,
            'available_minutes': None,
            'is_late_order': False,
            'partcustid': None,
            'schedule_found': False
        }
    
    def _create_sub_warehouse_deadline_result(self, order_row: pd.Series, order_time_str: str) -> Dict:
        """🆕 新增：為副倉庫路線創建截止時間結果"""
        
        # 解析訂單時間
        order_time = self._parse_time_string(order_time_str)
        
        # 副倉庫路線的特殊處理邏輯
        routecd = str(order_row.get('ROUTECD', ''))
        partcustid = str(order_row.get('PARTCUSTID', ''))
        
        # 副倉庫通常需要當天完成，設定預設的截止時間
        if routecd in ['SDTC', 'SDHN']:
            # 副倉庫路線：當天17:00截止
            from datetime import time
            delivery_time = time(17, 0)  # 17:00
            order_cutoff_time = time(16, 30)  # 16:30截止接單
        elif routecd == 'R15' and partcustid == 'SDTC':
            # R15+SDTC組合
            delivery_time = time(17, 0)
            order_cutoff_time = time(16, 30)
        elif routecd == 'R16' and partcustid == 'SDHN':
            # R16+SDHN組合  
            delivery_time = time(17, 0)
            order_cutoff_time = time(16, 30)
        else:
            # 其他情況使用預設時間
            delivery_time = time(17, 0)
            order_cutoff_time = time(16, 30)
        
        # 計算可用作業時間
        available_minutes = None
        if order_time and delivery_time:
            available_minutes = self._calculate_available_minutes(order_time, delivery_time)
        
        # 判斷是否遲到
        is_late = False
        if order_time and order_cutoff_time:
            is_late = self._is_order_late_simple(order_time, order_cutoff_time)
        
        return {
            'order_cutoff_time': order_cutoff_time,
            'delivery_time': delivery_time,
            'order_time': order_time,
            'available_minutes': available_minutes,
            'is_late_order': is_late,
            'partcustid': partcustid,
            'schedule_found': True  # 副倉庫視為有找到時刻表
        }

    
    def _parse_time_number(self, time_value) -> Optional[time]:
        """解析時間格式（強化版：支援各種數字格式）"""
        try:
            if pd.isna(time_value) or time_value == '':
                return None
                
            # 先轉為字串並清理
            time_str = str(time_value).strip()
            
            # 移除可能的小數點（如果是從Excel讀取的話）
            if '.' in time_str:
                time_str = time_str.split('.')[0]
            
            # 處理數字格式
            if time_str.isdigit():
                time_int = int(time_str)
                
                # 處理不同長度的數字格式
                if time_int < 100:  # 例如: 85 -> 08:05
                    hour = 0
                    minute = time_int
                elif time_int < 1000:  # 例如: 855 -> 08:55
                    hour = time_int // 100
                    minute = time_int % 100
                else:  # 例如: 1000 -> 10:00, 1350 -> 13:50
                    hour = time_int // 100
                    minute = time_int % 100
                
                # 驗證時間有效性
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    return time(hour, minute)
                else:
                    self.logger.warning(f"時間超出範圍: {hour}:{minute:02d} (原始值: {time_value})")
                    return None
            
            # 處理已經是時間格式的情況 (08:55)
            elif ':' in time_str:
                parts = time_str.split(':')
                if len(parts) >= 2:
                    hour = int(parts[0])
                    minute = int(parts[1])
                    
                    if 0 <= hour <= 23 and 0 <= minute <= 59:
                        return time(hour, minute)
                    else:
                        self.logger.warning(f"時間超出範圍: {hour}:{minute} (原始值: {time_value})")
                        return None
            
            # 如果都不符合，記錄錯誤
            self.logger.warning(f"無法解析時間格式: '{time_value}' (type: {type(time_value)})")
            return None
            
        except (ValueError, TypeError, AttributeError) as e:
            self.logger.warning(f"時間格式錯誤: '{time_value}' - {str(e)}")
            return None
    
    def _parse_time_string(self, time_str: str) -> Optional[time]:
        """解析字串格式時間（如11:42:02）"""
        if pd.isna(time_str) or time_str == '':
            return None
            
        try:
            time_str = str(time_str).strip()
            
            if ':' in time_str:
                parts = time_str.split(':')
                hour = int(parts[0])
                minute = int(parts[1])
                second = int(parts[2]) if len(parts) > 2 else 0
                
                return time(hour, minute, second)
            
        except (ValueError, IndexError) as e:
            self.logger.warning(f"字串時間格式錯誤: '{time_str}' - {str(e)}")
            
        return None
    
    def process_orders_batch(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """批次處理訂單，添加優先權和時間資訊"""
        self.logger.info(f"開始處理 {len(orders_df)} 筆訂單...")
        
        processed_orders = orders_df.copy()
        
        # 初始化新欄位
        processed_orders['priority_level'] = ''
        processed_orders['order_type'] = ''
        processed_orders['urgency_reason'] = ''
        processed_orders['delivery_time'] = None
        processed_orders['available_minutes'] = None
        processed_orders['is_late_order'] = False
        processed_orders['schedule_found'] = False  # 🆕 新增欄位
        processed_orders['time_invalid'] = False  # 初始化
        
        # 逐筆處理
        for idx, row in processed_orders.iterrows():
            # 分類優先權
            priority, order_type, reason = self.classify_order_priority(row)
            processed_orders.at[idx, 'priority_level'] = priority
            processed_orders.at[idx, 'order_type'] = order_type
            processed_orders.at[idx, 'urgency_reason'] = reason
            
            # 計算時間資訊
            time_info = self.calculate_deadline(row)
            processed_orders.at[idx, 'delivery_time'] = time_info['delivery_time']
            processed_orders.at[idx, 'available_minutes'] = time_info['available_minutes']
            processed_orders.at[idx, 'is_late_order'] = time_info['is_late_order']
            processed_orders.at[idx, 'schedule_found'] = time_info['schedule_found']
        
        # 🆕 統計和警告時間無效的訂單
        invalid_time_orders = processed_orders[processed_orders['time_invalid'] == True]
        if len(invalid_time_orders) > 0:
            self.logger.warning(f"發現 {len(invalid_time_orders)} 筆時間邏輯無效的訂單")
        
        # 統計結果
        priority_stats = processed_orders['priority_level'].value_counts()
        order_type_stats = processed_orders['order_type'].value_counts()
        schedule_found_count = len(processed_orders[processed_orders['schedule_found'] == True])
        
        self.logger.info(f"訂單分類完成:")
        self.logger.info(f"優先權分布: {dict(priority_stats)}")
        self.logger.info(f"訂單類型: {dict(order_type_stats)}")
        self.logger.info(f"找到時刻表: {schedule_found_count} 筆")
        
        late_orders = processed_orders[processed_orders['is_late_order'] == True]
        if len(late_orders) > 0:
            self.logger.warning(f"發現 {len(late_orders)} 筆遲到訂單")
        
        return processed_orders
    
    def get_priority_summary(self, processed_orders: pd.DataFrame) -> Dict:
        """取得優先權處理摘要"""
        summary = {
            'total_orders': len(processed_orders),
            'priority_distribution': processed_orders['priority_level'].value_counts().to_dict(),
            'order_type_distribution': processed_orders['order_type'].value_counts().to_dict(),
            'urgent_orders_count': len(processed_orders[processed_orders['priority_level'] == 'P1']),
            'sub_warehouse_count': len(processed_orders[processed_orders['order_type'] == 'SUB_WAREHOUSE']),
            'late_orders_count': len(processed_orders[processed_orders['is_late_order'] == True]),
            'avg_available_minutes': processed_orders['available_minutes'].mean()
        }
        
        return summary