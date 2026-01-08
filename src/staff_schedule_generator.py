"""
StaffScheduleGenerator - 人員排班模組 (修改版：單一班次)
負責生成每日人員排班，只有一種班次
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional
import random

class StaffScheduleGenerator:
    def __init__(self, data_manager):
        """初始化人員排班生成器"""
        self.logger = logging.getLogger(__name__)
        self.data_manager = data_manager
        self.staff_master = data_manager.master_data.get('staff_skill_master')
        
        # 載入排班相關參數
        self._load_staffing_parameters()
        
    def _load_staffing_parameters(self):
        """載入排班相關參數"""
        self.params = {
            'planned_staff_2f': self.data_manager.get_parameter_value('planned_staff_2f', 8),
            'planned_staff_3f': self.data_manager.get_parameter_value('planned_staff_3f', 8),
            'planned_staff_4f': self.data_manager.get_parameter_value('planned_staff_4f', 8),
            'staff_shortage_probability': self.data_manager.get_parameter_value('staff_shortage_probability', 0.03),
            'staff_shortage_reduction_min': self.data_manager.get_parameter_value('staff_shortage_reduction_min', 1),
            'staff_shortage_reduction_max': self.data_manager.get_parameter_value('staff_shortage_reduction_max', 3),
            
            # 🔧 修改：單一班次參數
            'shift_start_time': self.data_manager.get_parameter_value('shift_start_time', '08:50:00'),
            'shift_end_time': self.data_manager.get_parameter_value('shift_end_time', '17:30:00'),
            
            # 🆕 新增：加班相關參數
            'overtime_enabled': self.data_manager.get_parameter_value('overtime_enabled', 'Y'),
            'max_overtime_hours': self.data_manager.get_parameter_value('max_overtime_hours', 3.0),
            'overtime_end_time': self.data_manager.get_parameter_value('overtime_end_time', '20:30:00')
        }
        
        self.logger.info(f"排班參數載入完成: {self.params}")
    
    def generate_daily_schedule(self, date: str) -> pd.DataFrame:
        """生成指定日期的排班表"""
        self.logger.info(f"生成 {date} 的排班表...")
        
        # 重置每日已分配員工記錄
        self._daily_assigned_staff = set()
        
        schedule = []
        
        # 為每個樓層生成排班
        for floor in [2, 3, 4]:
            floor_schedule = self.generate_floor_schedule(floor, date)
            schedule.extend(floor_schedule)
        
        schedule_df = pd.DataFrame(schedule)
        
        self.logger.info(f"✅ {date} 排班生成完成，總計 {len(schedule_df)} 個班次")
        
        return schedule_df
    
    def generate_floor_schedule(self, floor: int, date: str) -> List[Dict]:
        """生成單一樓層的排班"""
        floor_key = f'planned_staff_{floor}f'
        planned_count = self.params[floor_key]
        
        # 1. 模擬人員短缺
        actual_count = self.apply_shortage_simulation(planned_count)
        
        self.logger.info(f"樓層 {floor}F: 計劃 {planned_count} 人，實際 {actual_count} 人")
        
        # 2. 選取可用人員
        available_staff = self.get_available_staff(floor)
        
        if len(available_staff) < actual_count:
            self.logger.warning(f"樓層 {floor}F 可用人員不足: 需要 {actual_count} 人，可用 {len(available_staff)} 人")
            actual_count = len(available_staff)
        
        # 3. 避免重複選取員工
        if not hasattr(self, '_daily_assigned_staff'):
            self._daily_assigned_staff = set()
        
        # 過濾掉已經被分配的員工
        unassigned_staff = [staff for staff in available_staff if staff not in self._daily_assigned_staff]
        
        if len(unassigned_staff) < actual_count:
            self.logger.warning(f"樓層 {floor}F 未分配人員不足: 需要 {actual_count} 人，可用 {len(unassigned_staff)} 人")
            actual_count = len(unassigned_staff)
        
        # 選取員工
        if actual_count > 0:
            selected_staff = np.random.choice(
                unassigned_staff, 
                size=actual_count, 
                replace=False
            )
            
            # 記錄已分配的員工
            self._daily_assigned_staff.update(selected_staff)
        else:
            selected_staff = []
        
        # 4. 分配班次和工作站
        floor_assignments = []
        for i, staff_id in enumerate(selected_staff):
            assignment = self.assign_staff_shift(staff_id, floor, i, date)
            floor_assignments.append(assignment)
        
        return floor_assignments
    
    def apply_shortage_simulation(self, planned_count: int) -> int:
        """模擬人員短缺情況"""
        shortage_prob = self.params['staff_shortage_probability']
        
        if random.random() < shortage_prob:
            # 發生人員短缺
            min_reduction = self.params['staff_shortage_reduction_min']
            max_reduction = self.params['staff_shortage_reduction_max']
            reduction = random.randint(min_reduction, max_reduction)
            actual_count = max(1, planned_count - reduction)  # 至少保留1人
            
            self.logger.info(f"發生人員短缺: 減少 {reduction} 人")
            return actual_count
        else:
            return planned_count
    
    def get_available_staff(self, floor: int) -> List[int]:
        """取得該樓層可用人員清單"""
        if self.staff_master is None:
            self.logger.error("staff_skill_master 資料未載入")
            return []
        
        # 可以在該樓層工作的人員（包含專屬該樓層和全樓層支援的人員）
        floor_staff = self.staff_master[
            (self.staff_master['floor'] == str(floor)) | 
            (self.staff_master['floor'] == 'ALL')
        ]['staff_id'].tolist()
        
        return floor_staff
    
    def assign_staff_shift(self, staff_id: int, floor: int, position: int, date: str) -> Dict:
        """🔧 修改：分配員工班次 - 單一班次版本"""
        
        # 🔧 簡化：只有一種班次
        start_time = self.params['shift_start_time']
        end_time = self.params['shift_end_time']
        
        # 計算班次時數
        start_dt = datetime.strptime(start_time, '%H:%M:%S')
        end_dt = datetime.strptime(end_time, '%H:%M:%S')
        
        # 處理跨日情況
        if end_dt <= start_dt:
            end_dt += timedelta(days=1)
        
        shift_duration = end_dt - start_dt
        shift_hours = shift_duration.total_seconds() / 3600
        
        # 工作站ID格式
        station_id = f"ST{floor}F{position+1:02d}"
        
        return {
            'date': date,
            'floor': str(floor),
            'station_id': station_id,
            'staff_id': int(staff_id),
            'shift_start_time': start_time,
            'shift_end_time': end_time,
            'shift_hours': round(shift_hours, 2),
            'is_overtime': False,  # 🆕 新增：是否為加班
            'overtime_hours': 0.0   # 🆕 新增：加班時數
        }
    
    def generate_overtime_schedule(self, base_schedule: pd.DataFrame, overtime_requirements: Dict) -> pd.DataFrame:
        """🆕 新增：生成加班排班"""
        self.logger.info(f"生成加班排班，需要加班的工作站: {len(overtime_requirements)}")
        
        overtime_schedule = []
        
        for station_id, overtime_info in overtime_requirements.items():
            # 找到該工作站的原班人員
            base_assignment = base_schedule[base_schedule['station_id'] == station_id]
            
            if len(base_assignment) == 0:
                self.logger.warning(f"找不到工作站 {station_id} 的原班人員")
                continue
            
            staff_assignment = base_assignment.iloc[0]
            
            # 檢查員工是否可以加班
            if not self._can_staff_overtime(staff_assignment['staff_id'], overtime_info):
                self.logger.warning(f"員工 {staff_assignment['staff_id']} 無法加班")
                continue
            
            # 計算加班時間
            overtime_start = staff_assignment['shift_end_time']
            overtime_duration = min(overtime_info['required_hours'], self.params['max_overtime_hours'])
            
            # 計算加班結束時間
            start_dt = datetime.strptime(overtime_start, '%H:%M:%S')
            overtime_end_dt = start_dt + timedelta(hours=overtime_duration)
            overtime_end = overtime_end_dt.strftime('%H:%M:%S')
            
            # 檢查是否超過最大加班時間限制
            max_end_time = self.params['overtime_end_time']
            if overtime_end > max_end_time:
                overtime_end = max_end_time
                # 重新計算實際加班時數
                max_end_dt = datetime.strptime(max_end_time, '%H:%M:%S')
                actual_duration = (max_end_dt - start_dt).total_seconds() / 3600
                overtime_duration = max(0, actual_duration)
            
            overtime_assignment = {
                'date': staff_assignment['date'],
                'floor': staff_assignment['floor'],
                'station_id': station_id,
                'staff_id': staff_assignment['staff_id'],
                'shift_start_time': overtime_start,
                'shift_end_time': overtime_end,
                'shift_hours': round(overtime_duration, 2),
                'is_overtime': True,
                'overtime_hours': round(overtime_duration, 2),
                'overtime_reason': overtime_info.get('reason', 'unknown')
            }
            
            overtime_schedule.append(overtime_assignment)
            
            self.logger.info(f"🕒 工作站 {station_id} 員工 {staff_assignment['staff_id']} 加班 {overtime_duration:.1f} 小時")
        
        return pd.DataFrame(overtime_schedule)
    
    def _can_staff_overtime(self, staff_id: int, overtime_info: Dict) -> bool:
        """🆕 檢查員工是否可以加班"""
        staff_info = self.get_staff_info(staff_id)
        
        if not staff_info:
            return False
        
        # 檢查員工最大工時限制
        max_daily_hours = staff_info.get('max_hours_per_day', 12.0)
        current_hours = overtime_info.get('current_hours', 8.0)
        required_overtime = overtime_info.get('required_hours', 1.0)
        
        if current_hours + required_overtime > max_daily_hours:
            return False
        
        # 檢查是否啟用加班
        if self.params['overtime_enabled'] != 'Y':
            return False
        
        return True
    
    def calculate_overtime_requirements(self, incomplete_tasks: List, current_time: datetime) -> Dict:
        """🆕 計算加班需求"""
        overtime_requirements = {}
        
        for task in incomplete_tasks:
            if not hasattr(task, 'assigned_station') or not task.assigned_station:
                continue
            
            station_id = task.assigned_station
            
            # 檢查任務類型和緊急程度
            requires_overtime = False
            reason = ""
            
            if hasattr(task, 'task_type'):
                if task.task_type == 'SHIPPING':
                    # 出貨任務：副倉庫需要當天完成
                    if hasattr(task, 'route_code') and task.route_code in ['SDTC', 'SDHN']:
                        requires_overtime = True
                        reason = "副倉庫出貨必須當天完成"
                
                elif task.task_type == 'RECEIVING':
                    # 進貨任務：檢查是否已經第3天
                    if hasattr(task, 'arrival_date'):
                        days_since_arrival = (current_time.date() - task.arrival_date).days
                        if days_since_arrival >= 2:  # 第3天（0,1,2）
                            requires_overtime = True
                            reason = f"進貨任務已放置 {days_since_arrival + 1} 天，必須完成"
            
            if requires_overtime:
                # 估算需要的加班時間
                remaining_time = task.estimated_duration * 0.7  # 假設還剩70%工作量
                required_hours = max(1.0, remaining_time / 60)  # 至少1小時
                
                overtime_requirements[station_id] = {
                    'task_id': task.task_id,
                    'required_hours': required_hours,
                    'reason': reason,
                    'current_hours': 8.0  # 假設已工作8小時
                }
        
        return overtime_requirements
    
    def validate_schedule_feasibility(self, schedule_df: pd.DataFrame) -> Dict[str, bool]:
        """驗證排班合理性（更新版）"""
        validation_results = {}
        
        # 檢查每日人員數量
        daily_counts = schedule_df.groupby(['date', 'floor']).size()
        validation_results['daily_staff_reasonable'] = all(count >= 1 for count in daily_counts)
        
        # 🔧 修改：檢查班次時間一致性
        unique_shifts = schedule_df[['shift_start_time', 'shift_end_time']].drop_duplicates()
        validation_results['consistent_shift_times'] = len(unique_shifts) <= 2  # 正常班+加班
        
        # 檢查是否有重複指派（同一天同一員工的正常班）
        normal_shifts = schedule_df[schedule_df['is_overtime'] == False]
        duplicate_check = normal_shifts.groupby(['date', 'staff_id']).size()
        duplicates = duplicate_check[duplicate_check > 1]
        validation_results['no_duplicate_assignment'] = len(duplicates) == 0
        
        if len(duplicates) > 0:
            self.logger.warning(f"發現重複分配: {len(duplicates)} 個員工在同一天被分配多次")
        
        # 檢查員工總工時是否超過上限
        daily_hours = schedule_df.groupby(['date', 'staff_id'])['shift_hours'].sum()
        
        max_hours_violations = []
        for (date, staff_id), total_hours in daily_hours.items():
            staff_info = self.get_staff_info(staff_id)
            max_hours = staff_info.get('max_hours_per_day', 12.0)
            
            if total_hours > max_hours:
                max_hours_violations.append((date, staff_id, total_hours, max_hours))
        
        validation_results['no_overtime_violation'] = len(max_hours_violations) == 0
        
        if max_hours_violations:
            self.logger.warning(f"發現工時超限: {len(max_hours_violations)} 個違規")
        
        # 輸出驗證結果
        for check_name, result in validation_results.items():
            status = "✅ 通過" if result else "❌ 失敗"
            self.logger.info(f"排班驗證 - {check_name}: {status}")
        
        return validation_results
    
    def get_staff_info(self, staff_id: int) -> Dict:
        """取得員工資訊"""
        if self.staff_master is None:
            return {}
        
        staff_row = self.staff_master[self.staff_master['staff_id'] == staff_id]
        
        if len(staff_row) == 0:
            return {}
        
        staff_info = staff_row.iloc[0]
        
        # 處理capacity_multiplier的格式問題
        try:
            capacity_multiplier = float(staff_info['capacity_multiplier'])
        except (ValueError, TypeError):
            self.logger.warning(f"員工 {staff_id} 的 capacity_multiplier 格式錯誤，使用預設值 1.0")
            capacity_multiplier = 1.0
        
        return {
            'staff_id': int(staff_info['staff_id']),
            'staff_name': staff_info['staff_name'],
            'floor': staff_info['floor'],
            'skill_level': int(staff_info['skill_level']),
            'capacity_multiplier': capacity_multiplier,
            'max_hours_per_day': float(staff_info['max_hours_per_day'])
        }
    
    def generate_period_schedule(self, start_date: str, end_date: str) -> pd.DataFrame:
        """生成一段期間的排班資料"""
        self.logger.info(f"生成期間排班: {start_date} 到 {end_date}")
        
        all_schedules = []
        
        # 生成日期範圍
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        current_date = start_dt
        while current_date <= end_dt:

            # 🆕 跳過週末
            if not self.data_manager.is_workday(current_date):
                current_date += timedelta(days=1)
                continue

            date_str = current_date.strftime('%Y-%m-%d')
            
            # 設定隨機種子確保每日結果不同但可重現
            np.random.seed(hash(date_str) % (2**32))
            random.seed(hash(date_str) % (2**32))
            
            daily_schedule = self.generate_daily_schedule(date_str)
            all_schedules.append(daily_schedule)
            current_date += timedelta(days=1)
        
        if all_schedules:
            period_schedule = pd.concat(all_schedules, ignore_index=True)
            self.logger.info(f"期間排班生成完成: {len(period_schedule)} 個班次")
            return period_schedule
        else:
            return pd.DataFrame()