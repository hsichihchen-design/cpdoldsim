"""
ReceivingManager - 進貨管理模組（新增）
負責處理進貨任務的優先權和期限管理
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple

class ReceivingManager:
    def __init__(self, data_manager):
        """初始化進貨管理器"""
        self.logger = logging.getLogger(__name__)
        self.data_manager = data_manager
        
        # 載入進貨相關參數
        self._load_receiving_parameters()
        
    def _load_receiving_parameters(self):
        """載入進貨相關參數"""
        self.params = {
            # 🆕 進貨完成期限（天數）
            'receiving_completion_days': self.data_manager.get_parameter_value('receiving_completion_days', 3),
            
            # 進貨優先權設定
            'receiving_normal_priority': self.data_manager.get_parameter_value('receiving_normal_priority', 'P4'),
            'receiving_urgent_priority': self.data_manager.get_parameter_value('receiving_urgent_priority', 'P2'),
            'receiving_critical_priority': self.data_manager.get_parameter_value('receiving_critical_priority', 'P1'),
            
            # 進貨任務處理時間（分鐘）
            'receiving_base_time': self.data_manager.get_parameter_value('receiving_base_time', 20),
            'receiving_time_variance': self.data_manager.get_parameter_value('receiving_time_variance', 0.2),
            
            # 緊急進貨判斷條件
            'urgent_item_codes': self.data_manager.get_parameter_value('urgent_item_codes', ''),
            'critical_quantity_threshold': self.data_manager.get_parameter_value('critical_quantity_threshold', 1000)
        }
        
        # 轉換為列表格式
        self.urgent_item_codes = [x.strip() for x in self.params['urgent_item_codes'].split(',') if x.strip()]
        
        self.logger.info(f"進貨管理參數載入完成:")
        self.logger.info(f"完成期限: {self.params['receiving_completion_days']} 天")
        self.logger.info(f"緊急零件代碼: {self.urgent_item_codes}")
    
    def process_receiving_batch(self, receiving_df: pd.DataFrame, current_date: date) -> pd.DataFrame:
        """批次處理進貨資料，添加優先權和期限資訊"""
        self.logger.info(f"開始處理 {len(receiving_df)} 筆進貨資料...")
        
        processed_receiving = receiving_df.copy()
        
        # 初始化新欄位
        processed_receiving['task_type'] = 'RECEIVING'
        processed_receiving['priority_level'] = ''
        processed_receiving['deadline_date'] = None
        processed_receiving['days_since_arrival'] = 0
        processed_receiving['is_overdue'] = False
        processed_receiving['urgency_reason'] = ''
        processed_receiving['estimated_duration'] = 0.0
        
        # 逐筆處理
        for idx, row in processed_receiving.iterrows():
            # 計算到貨天數和期限
            arrival_info = self.calculate_deadline_and_urgency(row, current_date)
            
            # 分類優先權
            priority, urgency_reason = self.classify_receiving_priority(row, arrival_info)
            
            # 估算處理時間
            duration = self.estimate_receiving_duration(row)
            
            # 更新資料
            processed_receiving.at[idx, 'priority_level'] = priority
            processed_receiving.at[idx, 'deadline_date'] = arrival_info['deadline_date']
            processed_receiving.at[idx, 'days_since_arrival'] = arrival_info['days_since_arrival']
            processed_receiving.at[idx, 'is_overdue'] = arrival_info['is_overdue']
            processed_receiving.at[idx, 'urgency_reason'] = urgency_reason
            processed_receiving.at[idx, 'estimated_duration'] = duration
        
        # 統計結果
        priority_stats = processed_receiving['priority_level'].value_counts()
        overdue_count = len(processed_receiving[processed_receiving['is_overdue'] == True])
        
        self.logger.info(f"進貨處理完成:")
        self.logger.info(f"優先權分布: {dict(priority_stats)}")
        
        if overdue_count > 0:
            self.logger.warning(f"發現 {overdue_count} 筆逾期進貨")
        
        return processed_receiving
    
    def calculate_deadline_and_urgency(self, receiving_row: pd.Series, current_date: date) -> Dict:
        """計算進貨期限和緊急程度"""
        
        # 解析到貨日期
        arrival_date_str = str(receiving_row.get('DATE', ''))
        try:
            if '-' in arrival_date_str:
                arrival_date = datetime.strptime(arrival_date_str, '%Y-%m-%d').date()
            else:
                # 處理其他日期格式
                arrival_date = datetime.strptime(arrival_date_str, '%Y%m%d').date()
        except (ValueError, TypeError):
            self.logger.warning(f"進貨日期格式錯誤: '{arrival_date_str}'，使用當前日期")
            arrival_date = current_date
        
        # 計算期限日期（到貨日期 + 完成天數）
        completion_days = self.params['receiving_completion_days']
        deadline_date = arrival_date + timedelta(days=completion_days - 1)  # 第3天要完成
        
        # 計算已經過的天數
        days_since_arrival = (current_date - arrival_date).days
        
        # 判斷是否逾期
        is_overdue = current_date > deadline_date
        
        # 判斷是否即將到期（今天是截止日）
        is_due_today = current_date == deadline_date
        
        return {
            'arrival_date': arrival_date,
            'deadline_date': deadline_date,
            'days_since_arrival': days_since_arrival,
            'is_overdue': is_overdue,
            'is_due_today': is_due_today,
            'remaining_days': max(0, (deadline_date - current_date).days)
        }
    
    def classify_receiving_priority(self, receiving_row: pd.Series, arrival_info: Dict) -> Tuple[str, str]:
        """分類進貨優先權"""
        
        frcd = str(receiving_row.get('FRCD', ''))
        partno = str(receiving_row.get('PARTNO', ''))
        quantity = receiving_row.get('QTY', 0)
        
        # 🔥 最高優先權：已逾期
        if arrival_info['is_overdue']:
            return self.params['receiving_critical_priority'], f"已逾期 {arrival_info['days_since_arrival']} 天"
        
        # 🚨 高優先權：今天截止
        if arrival_info['is_due_today']:
            return self.params['receiving_urgent_priority'], f"今天是截止日（第{self.params['receiving_completion_days']}天）"
        
        # 🔶 中優先權：緊急零件
        if frcd in self.urgent_item_codes:
            return self.params['receiving_urgent_priority'], f"緊急零件代碼({frcd})"
        
        # 🔶 中優先權：大量進貨
        if quantity >= self.params['critical_quantity_threshold']:
            return self.params['receiving_urgent_priority'], f"大量進貨({quantity}件)"
        
        # 🔸 提醒優先權：明天截止
        if arrival_info['remaining_days'] == 1:
            return self.params['receiving_urgent_priority'], f"明天截止（剩餘1天）"
        
        # 🔹 一般優先權：還有時間
        return self.params['receiving_normal_priority'], f"一般進貨（剩餘{arrival_info['remaining_days']}天）"
    
    def estimate_receiving_duration(self, receiving_row: pd.Series) -> float:
        """估算進貨處理時間（分鐘）"""
        
        base_time = self.params['receiving_base_time']
        variance_factor = self.params['receiving_time_variance']
        
        quantity = receiving_row.get('QTY', 1)
        
        # 基礎時間 + 數量影響
        quantity_factor = 1.0 + (quantity / 100) * 0.1  # 每100件增加10%時間
        estimated_time = base_time * quantity_factor
        
        # 加入隨機變動
        variance = estimated_time * variance_factor
        estimated_time += np.random.uniform(-variance, variance)
        
        # 確保在合理範圍內
        min_time = 5.0  # 最少5分鐘
        max_time = 120.0  # 最多2小時
        
        return max(min_time, min(max_time, round(estimated_time, 1)))
    
    def get_overdue_receiving_tasks(self, processed_receiving: pd.DataFrame) -> pd.DataFrame:
        """取得逾期的進貨任務"""
        overdue_tasks = processed_receiving[processed_receiving['is_overdue'] == True].copy()
        
        # 按逾期天數排序（最緊急的在前）
        overdue_tasks = overdue_tasks.sort_values('days_since_arrival', ascending=False)
        
        return overdue_tasks
    
    def get_due_today_receiving_tasks(self, processed_receiving: pd.DataFrame) -> pd.DataFrame:
        """取得今天截止的進貨任務"""
        # 今天是截止日的任務
        due_today = processed_receiving[
            (processed_receiving['is_overdue'] == False) & 
            (processed_receiving['urgency_reason'].str.contains('今天是截止日', na=False))
        ].copy()
        
        # 按優先權和數量排序
        priority_order = {
            self.params['receiving_critical_priority']: 1,
            self.params['receiving_urgent_priority']: 2, 
            self.params['receiving_normal_priority']: 3
        }
        
        due_today['priority_order'] = due_today['priority_level'].map(priority_order)
        due_today = due_today.sort_values(['priority_order', 'QTY'], ascending=[True, False])
        
        return due_today.drop('priority_order', axis=1)
    
    def generate_receiving_schedule_recommendation(self, processed_receiving: pd.DataFrame, 
                                                 available_capacity: Dict) -> Dict:
        """生成進貨排程建議"""
        
        recommendations = {
            'immediate_action_required': [],
            'today_schedule': [],
            'tomorrow_schedule': [],
            'normal_schedule': [],
            'capacity_analysis': {},
            'warnings': []
        }
        
        # 1. 立即處理：逾期任務
        overdue_tasks = self.get_overdue_receiving_tasks(processed_receiving)
        if len(overdue_tasks) > 0:
            recommendations['immediate_action_required'] = overdue_tasks.to_dict('records')
            recommendations['warnings'].append(f"有 {len(overdue_tasks)} 個逾期進貨任務需要立即處理")
        
        # 2. 今天處理：今天截止的任務
        due_today_tasks = self.get_due_today_receiving_tasks(processed_receiving)
        if len(due_today_tasks) > 0:
            recommendations['today_schedule'] = due_today_tasks.to_dict('records')
            recommendations['warnings'].append(f"有 {len(due_today_tasks)} 個進貨任務今天必須完成")
        
        # 3. 明天處理：明天截止的任務
        tomorrow_tasks = processed_receiving[
            processed_receiving['urgency_reason'].str.contains('明天截止', na=False)
        ]
        if len(tomorrow_tasks) > 0:
            recommendations['tomorrow_schedule'] = tomorrow_tasks.to_dict('records')
        
        # 4. 一般排程：其他任務
        normal_tasks = processed_receiving[
            (processed_receiving['is_overdue'] == False) & 
            (~processed_receiving['urgency_reason'].str.contains('今天是截止日|明天截止', na=False))
        ]
        recommendations['normal_schedule'] = normal_tasks.to_dict('records')
        
        # 5. 產能分析
        total_immediate_time = overdue_tasks['estimated_duration'].sum() if len(overdue_tasks) > 0 else 0
        total_today_time = due_today_tasks['estimated_duration'].sum() if len(due_today_tasks) > 0 else 0
        
        recommendations['capacity_analysis'] = {
            'immediate_hours_required': round(total_immediate_time / 60, 1),
            'today_hours_required': round(total_today_time / 60, 1),
            'available_capacity': available_capacity,
            'capacity_sufficient': self._check_capacity_sufficiency(
                total_immediate_time + total_today_time, available_capacity
            )
        }
        
        return recommendations
    
    def _check_capacity_sufficiency(self, required_minutes: float, available_capacity: Dict) -> bool:
        """檢查產能是否足夠"""
        if not available_capacity:
            return False
        
        # 簡化假設：每個工作站每小時可處理的進貨任務
        total_available_minutes = 0
        for floor, stations in available_capacity.items():
            total_available_minutes += stations * 60 * 8  # 假設每站每天8小時
        
        return required_minutes <= total_available_minutes * 0.8  # 保留20%緩衝
    
    def update_receiving_progress(self, receiving_tasks: List, completed_task_ids: List) -> Dict:
        """更新進貨進度"""
        
        progress_summary = {
            'total_tasks': len(receiving_tasks),
            'completed_tasks': len(completed_task_ids),
            'completion_rate': 0.0,
            'overdue_remaining': 0,
            'due_today_remaining': 0,
            'on_schedule_tasks': 0
        }
        
        if progress_summary['total_tasks'] > 0:
            progress_summary['completion_rate'] = round(
                progress_summary['completed_tasks'] / progress_summary['total_tasks'] * 100, 1
            )
        
        # 分析剩餘任務
        remaining_tasks = [task for task in receiving_tasks if task.task_id not in completed_task_ids]
        
        for task in remaining_tasks:
            if hasattr(task, 'is_overdue') and task.is_overdue:
                progress_summary['overdue_remaining'] += 1
            elif hasattr(task, 'urgency_reason') and '今天是截止日' in task.urgency_reason:
                progress_summary['due_today_remaining'] += 1
            else:
                progress_summary['on_schedule_tasks'] += 1
        
        return progress_summary
    
    def get_receiving_summary(self, processed_receiving: pd.DataFrame) -> Dict:
        """取得進貨處理摘要"""
        
        summary = {
            'total_receiving_items': len(processed_receiving),
            'priority_distribution': processed_receiving['priority_level'].value_counts().to_dict(),
            'overdue_count': len(processed_receiving[processed_receiving['is_overdue'] == True]),
            'due_today_count': len(processed_receiving[
                processed_receiving['urgency_reason'].str.contains('今天是截止日', na=False)
            ]),
            'total_estimated_hours': round(processed_receiving['estimated_duration'].sum() / 60, 1),
            'avg_days_since_arrival': round(processed_receiving['days_since_arrival'].mean(), 1),
            'urgent_items_count': len(processed_receiving[
                processed_receiving['urgency_reason'].str.contains('緊急零件', na=False)
            ])
        }
        
        return summary