"""
WaveManager - 波次管理模組 (修改版：基於班車時刻表)
負責管理基於 route_schedule_master 的波次組成和完成判斷
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import uuid
from collections import defaultdict

class WaveStatus(Enum):
    """波次狀態枚舉"""
    PLANNED = "PLANNED"           # 規劃中
    READY = "READY"              # 準備就緒
    IN_PROGRESS = "IN_PROGRESS"   # 執行中
    COMPLETED = "COMPLETED"       # 已完成
    CANCELLED = "CANCELLED"       # 取消
    DELAYED = "DELAYED"          # 延遲

class WaveType(Enum):
    """波次類型枚舉"""
    SCHEDULED = "SCHEDULED"       # 班車波次
    URGENT = "URGENT"            # 緊急波次
    RECEIVING = "RECEIVING"      # 進貨波次

@dataclass
class Wave:
    """波次物件（基於班車時刻表）"""
    wave_id: str

    # 🆕 新增：基於出車時間的屬性
    delivery_time_str: str = ""           # 出車時間字串，如 "1000"
    delivery_datetime: Optional[datetime] = None      # 完整出車時間
    latest_cutoff_time: Optional[datetime] = None     # 最晚截止時間
    
    # 🆕 新增：包含的路線和據點資訊
    included_routes: List[str] = field(default_factory=list)        # 包含的路線
    included_partcustids: List[str] = field(default_factory=list)   # 包含的據點
    cutoff_times: List[str] = field(default_factory=list)          # 各截止時間

    wave_type: WaveType = WaveType.SCHEDULED
    task_ids: List[str] = field(default_factory=list)
    status: WaveStatus = WaveStatus.PLANNED
            
    actual_start_time: Optional[datetime] = None
    actual_completion_time: Optional[datetime] = None
    total_tasks: int = 0
    completed_tasks: int = 0
    assigned_workstations: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def available_work_time_minutes(self) -> int:
        """計算可用作業時間（分鐘）"""
        if self.latest_cutoff_time and self.delivery_datetime:
            delta = self.delivery_datetime - self.latest_cutoff_time
            return max(0, int(delta.total_seconds() / 60))
        elif 'work_time_minutes' in self.metadata:
            return int(self.metadata['work_time_minutes'])
        return 0
    
    # 🆕 新增：便利屬性
    @property 
    def route_code(self) -> str:
        """相容性屬性：返回第一個路線代碼"""
        return self.included_routes[0] if self.included_routes else ""
    
    @property
    def route_group(self) -> int:
        """相容性屬性：從出車時間推導班次號"""
        try:
            # 簡化邏輯：用出車時間的小時作為班次號
            if self.delivery_datetime:
                return self.delivery_datetime.hour
            return 1
        except:
            return 1 
    

class WaveManager:
    def __init__(self, data_manager, workstation_manager):
        """初始化波次管理器"""
        self.logger = logging.getLogger(__name__)
        self.data_manager = data_manager
        self.workstation_manager = workstation_manager
        self.route_schedule = data_manager.master_data.get('route_schedule_master')
        
        # 驗證 route_schedule_master
        if self.route_schedule is None or len(self.route_schedule) == 0:
            self.logger.error("route_schedule_master 資料未載入或為空！")
            raise ValueError("route_schedule_master is required for wave management")

        # 載入波次相關參數
        self._load_wave_parameters()
        
        # 初始化波次追蹤
        self.waves: Dict[str, Wave] = {}
        self.active_waves: List[str] = []
        self.wave_history: List[str] = []
        
        # 🆕 新增：建立出車時間對應表
        self.delivery_waves_map: Dict[str, Dict] = {}  # delivery_time -> wave_info
        self.partcustid_to_waves: Dict[str, List[str]] = {}  # partcustid -> [wave_ids]
        
        self._build_delivery_waves_map()
        
    def _load_wave_parameters(self):
        """載入波次相關參數"""
        self.params = {
            'wave_preparation_minutes': self.data_manager.get_parameter_value('wave_preparation_minutes', 5),
            'early_start_buffer_minutes': self.data_manager.get_parameter_value('early_start_buffer_minutes', 30),
            'late_arrival_tolerance_minutes': self.data_manager.get_parameter_value('late_arrival_tolerance_minutes', 15),
            'auto_create_next_day_waves': self.data_manager.get_parameter_value('auto_create_next_day_waves', 'Y'),
            'min_wave_duration_minutes': self.data_manager.get_parameter_value('min_wave_duration_minutes', 30)
        }
        
        self.logger.info(f"波次參數載入完成: {self.params}")
    
        
    def assign_tasks_to_waves(self, tasks: List, current_time: datetime) -> Dict[str, List[str]]:
        """ 新方法：將任務分配到對應的波次"""
        self.logger.info(f"開始分配 {len(tasks)} 個任務到波次...")
        
        assignment_results = {
            'assigned': [],
            'unassigned': [],
            'late_assignments': [],
            'errors': []
        }
        
        for task in tasks:
            try:
                # 取得任務的路線資訊
                task_route = self._get_task_route_info(task)
                if not task_route:
                    assignment_results['unassigned'].append(task.task_id)
                    continue
                
                # 找到對應的波次
                target_wave = self._find_wave_for_task(task_route, current_time)
                if not target_wave:
                    assignment_results['unassigned'].append(task.task_id)
                    continue
                
                # 檢查是否遲到
                is_late = current_time > target_wave.order_cutoff_time
                
                # 分配任務
                target_wave.task_ids.append(task.task_id)
                target_wave.total_tasks += 1
                
                # 更新任務的波次資訊
                if hasattr(task, 'assigned_wave'):
                    task.assigned_wave = target_wave.wave_id
                
                if is_late:
                    assignment_results['late_assignments'].append(task.task_id)
                else:
                    assignment_results['assigned'].append(task.task_id)
                    
            except Exception as e:
                self.logger.error(f"分配任務 {task.task_id} 時發生錯誤: {str(e)}")
                assignment_results['errors'].append(task.task_id)
        
        # 統計結果
        self.logger.info(f"任務分配完成:")
        self.logger.info(f"  正常分配: {len(assignment_results['assigned'])}")
        self.logger.info(f"  遲到分配: {len(assignment_results['late_assignments'])}")
        self.logger.info(f"  未分配: {len(assignment_results['unassigned'])}")
        self.logger.info(f"  錯誤: {len(assignment_results['errors'])}")
        
        return assignment_results
    

    def _get_task_route_info(self, task) -> Optional[Dict]:
        """取得任務的路線資訊"""
        # 從任務物件或相關訂單資料中取得路線資訊
        if hasattr(task, 'route_code') and hasattr(task, 'route_group'):
            # 🔧 新增：處理副倉庫情況（route_group 可能為 None）
            if task.route_code and task.route_code in ['SDTC', 'SDHN']:  # 副倉庫路線
                return {
                    'route_code': task.route_code,
                    'route_group': task.route_group or 'SUB_WAREHOUSE',  # 如果為None，標記為副倉庫
                    'is_sub_warehouse': True
                }
            elif task.route_code and task.route_group is not None:
                return {
                    'route_code': task.route_code,
                    'route_group': task.route_group,
                    'is_sub_warehouse': False
                }
        
        # 如果任務沒有直接的路線資訊，從訂單資料查找
        if hasattr(task, 'order_id'):
            # 這裡需要從原始訂單資料中查找
            # 暫時返回None，實際實作時需要查詢訂單資料
            return None
        
        return None
    
    def _find_wave_for_task(self, task_route: Dict, current_time: datetime) -> Optional[Wave]:
        """為任務找到對應的波次"""
        route_code = task_route['route_code']
        route_group = task_route['route_group']
        is_sub_warehouse = task_route.get('is_sub_warehouse', False)
        
        # 🔧 新增：副倉庫任務處理
        if is_sub_warehouse or route_group == 'SUB_WAREHOUSE':
            # 為副倉庫創建特殊波次或使用現有的副倉庫波次
            sub_warehouse_wave = self._find_or_create_sub_warehouse_wave(route_code, current_time)
            return sub_warehouse_wave
        
        # 找到精確匹配的波次
        for wave in self.waves.values():
            if (wave.route_code == route_code and 
                wave.route_group == route_group and
                wave.status in [WaveStatus.PLANNED, WaveStatus.READY, WaveStatus.IN_PROGRESS]):
                return wave
        
        return None
        
    def start_wave_by_schedule(self, current_time: datetime) -> List[Dict]:
        """根據時刻表自動啟動到時間的波次"""
        started_waves = []
        
        for wave_id, wave in self.waves.items():
            if (wave.status == WaveStatus.PLANNED and 
                wave.latest_cutoff_time and  # ✅ 修改：使用 latest_cutoff_time
                current_time >= wave.latest_cutoff_time):  # ✅ 修改：使用 latest_cutoff_time
                
                result = self.start_wave(wave_id, current_time)
                if result['success']:
                    started_waves.append(result)
        
        return started_waves
    
    def complete_wave_by_schedule(self, current_time: datetime) -> List[Dict]:
        """🔧 修改：檢查預定時間到達，但不強制完成"""
        overdue_waves = []
        
        for wave_id, wave in self.waves.items():
            if (wave.status == WaveStatus.IN_PROGRESS and 
                wave.delivery_datetime and  # ✅ 修改：使用 delivery_datetime
                current_time >= wave.delivery_datetime): 
                
                # 檢查是否實際完成
                completion_result = self.check_wave_actual_completion(wave_id, current_time)
                
                if completion_result['completed']:
                    # 實際已完成
                    overdue_waves.append({
                        'wave_id': wave_id,
                        'route': f"{wave.route_code}-{wave.route_group}",
                        'status': 'completed_on_time',
                        'completed_tasks': completion_result['completed_tasks'],
                        'total_tasks': completion_result['total_tasks']
                    })
                else:
                    # 預定時間到但未完成
                    overdue_minutes = (current_time - wave.delivery_time).total_seconds() / 60
                    
                    overdue_waves.append({
                        'wave_id': wave_id,
                        'route': f"{wave.route_code}-{wave.route_group}",
                        'status': 'overdue',
                        'overdue_minutes': round(overdue_minutes, 1),
                        'completed_tasks': completion_result['completed_tasks'],
                        'total_tasks': completion_result['total_tasks'],
                        'incomplete_tasks': completion_result['incomplete_tasks']
                    })
                    
                    self.logger.warning(f"⏰ 波次 {wave_id} 預定時間已到但未完成，逾時 {overdue_minutes:.1f} 分鐘")
        
        return overdue_waves
    
    def get_waves_schedule_for_date(self, target_date: datetime) -> pd.DataFrame:
        """取得指定日期的波次時刻表"""
        waves_schedule = []
        
        for wave in self.waves.values():
            if (wave.latest_cutoff_time and  # ✅ 修改：使用 latest_cutoff_time
                wave.latest_cutoff_time.date() == target_date.date()):
                
                waves_schedule.append({
                    'wave_id': wave.wave_id,
                    'route_code': wave.route_code,
                    'route_group': wave.route_group,
                    'order_cutoff_time': wave.order_cutoff_time.strftime('%H:%M:%S'),
                    'delivery_time': wave.delivery_time.strftime('%H:%M:%S'),
                    'work_time_minutes': wave.available_work_time_minutes,
                    'total_tasks': wave.total_tasks,
                    'status': wave.status.value
                })
        
        df = pd.DataFrame(waves_schedule)
        if len(df) > 0:
            df = df.sort_values('order_cutoff_time')
        
        return df
    
    def get_waves_in_time_range(self, start_time: datetime, end_time: datetime) -> List[Wave]:
        """取得指定時間範圍內的波次"""
        waves_in_range = []
        
        for wave in self.waves.values():
            if (wave.latest_cutoff_time and wave.delivery_datetimeand
                # 波次開始時間在範圍內，或波次跨越範圍
                ((start_time <= wave.latest_cutoff_time<= end_time) or
                 (start_time <= wave.delivery_datetime<= end_time) or
                 (wave.latest_cutoff_time<= start_time and wave.delivery_datetime>= end_time))):
                waves_in_range.append(wave)
        
        # 按開始時間排序
        waves_in_range.sort(key=lambda w: w.order_cutoff_time or datetime.min)
        
        return waves_in_range
    
    def calculate_wave_count_in_period(self, start_time: datetime, end_time: datetime) -> Dict:
        """ 計算指定時間段內的波次數量（回答用戶的核心問題）"""
        waves_in_period = self.get_waves_in_time_range(start_time, end_time)
        
        # 統計分析
        route_distribution = {}
        status_distribution = {}
        time_distribution = []
        
        for wave in waves_in_period:
            # 按路線統計
            route_key = f"{wave.route_code}-{wave.route_group}"
            route_distribution[route_key] = route_distribution.get(route_key, 0) + 1
            
            # 按狀態統計
            status = wave.status.value
            status_distribution[status] = status_distribution.get(status, 0) + 1
            
            # 時間分布
            if wave.order_cutoff_time:
                time_distribution.append({
                    'wave_id': wave.wave_id,
                    'route': route_key,
                    'start_time': wave.order_cutoff_time,
                    'end_time': wave.delivery_time,
                    'work_minutes': wave.available_work_time_minutes,
                    'tasks': wave.total_tasks
                })
        
        return {
            'total_waves': len(waves_in_period),
            'time_period': f"{start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}",
            'route_distribution': route_distribution,
            'status_distribution': status_distribution,
            'wave_details': time_distribution,
            'total_work_minutes': sum(w.available_work_time_minutes for w in waves_in_period),
            'total_tasks': sum(w.total_tasks for w in waves_in_period)
        }
    
    # === 保留原有的核心方法 ===
    
    def start_wave(self, wave_id: str, current_time: datetime) -> Dict:
        """啟動波次"""
        if wave_id not in self.waves:
            return {'success': False, 'error': f'波次 {wave_id} 不存在'}
        
        wave = self.waves[wave_id]
        
        if wave.status != WaveStatus.PLANNED:
            return {'success': False, 'error': f'波次 {wave_id} 狀態不允許啟動: {wave.status.value}'}
        
        # 檢查是否提前太多啟動
        if wave.latest_cutoff_time:  # ✅ 修改：使用 latest_cutoff_time
            early_minutes = (wave.latest_cutoff_time - current_time).total_seconds() / 60
            if early_minutes > self.params['early_start_buffer_minutes']:
                return {
                    'success': False, 
                    'error': f'提前啟動時間過長: {early_minutes:.1f} 分鐘'
                }
        
        # 啟動波次
        wave.status = WaveStatus.IN_PROGRESS
        wave.actual_start_time = current_time
        
        if wave_id not in self.active_waves:
            self.active_waves.append(wave_id)
        
        self.logger.info(f" 波次 {wave_id} ({wave.route_code}-{wave.route_group}) 啟動成功")
        
        return {
            'success': True,
            'wave_id': wave_id,
            'route': f"{wave.route_code}-{wave.route_group}",
            'start_time': current_time,
            'delivery_time': wave.delivery_time,
            'available_work_minutes': wave.available_work_time_minutes
        }
    
    def track_wave_progress(self, wave_id: str, current_time: datetime) -> Dict:
        """追蹤波次進度"""
        if wave_id not in self.waves:
            return {'error': f'波次 {wave_id} 不存在'}
        
        wave = self.waves[wave_id]
        
        # 取得任務狀態
        task_status_counts = self._count_task_status(wave.task_ids)
        wave.completed_tasks = task_status_counts.get('COMPLETED', 0)
        
        # 計算進度
        progress_percent = (wave.completed_tasks / wave.total_tasks * 100) if wave.total_tasks > 0 else 0
        
        #  新增：基於班車時刻表的時間計算
        time_info = {}
        if wave.latest_cutoff_time and wave.delivery_time:
            if current_time < wave.order_cutoff_time:
                # 尚未開始
                time_info['status'] = 'waiting'
                time_info['minutes_until_start'] = (wave.latest_cutoff_time- current_time).total_seconds() / 60
            elif current_time <= wave.delivery_time:
                # 執行中
                time_info['status'] = 'in_progress'
                time_info['elapsed_minutes'] = (current_time - wave.order_cutoff_time).total_seconds() / 60
                time_info['remaining_minutes'] = (wave.delivery_datetime- current_time).total_seconds() / 60
                time_info['time_utilization'] = time_info['elapsed_minutes'] / wave.available_work_time_minutes * 100
            else:
                # 已過出車時間
                time_info['status'] = 'overdue'
                time_info['overdue_minutes'] = (current_time - wave.delivery_time).total_seconds() / 60
        
        return {
            'wave_id': wave_id,
            'route_code': wave.route_code,
            'route_group': wave.route_group,
            'status': wave.status.value,
            'progress_percent': round(progress_percent, 1),
            'completed_tasks': wave.completed_tasks,
            'total_tasks': wave.total_tasks,
            'task_status_counts': task_status_counts,
            'time_info': time_info,
            'schedule_times': {
                'order_cutoff': wave.order_cutoff_time,
                'delivery_time': wave.delivery_time,
                'work_time_minutes': wave.available_work_time_minutes
            }
        }
    
    def _count_task_status(self, task_ids: List[str]) -> Dict[str, int]:
        """統計任務狀態數量"""
        status_counts = {
            'PENDING': 0,
            'ASSIGNED': 0,
            'IN_PROGRESS': 0,
            'COMPLETED': 0,
            'PAUSED': 0,
            'CANCELLED': 0
        }
        
        for task_id in task_ids:
            if task_id in self.workstation_manager.tasks:
                task = self.workstation_manager.tasks[task_id]
                status = task.status.value
                if status in status_counts:
                    status_counts[status] += 1
        
        return status_counts
    
    def check_wave_actual_completion(self, wave_id: str, current_time: datetime) -> Dict:
        """🆕 新增：檢查波次是否實際完成（所有工作站完成該波次任務）"""
        if wave_id not in self.waves:
            return {'completed': False, 'error': f'波次 {wave_id} 不存在'}
        
        wave = self.waves[wave_id]
        
        if wave.status != WaveStatus.IN_PROGRESS:
            return {'completed': False, 'reason': f'波次狀態錯誤: {wave.status.value}'}
        
        # 檢查所有屬於此波次的任務
        incomplete_tasks = []
        completed_tasks = []
        
        for task_id in wave.task_ids:
            if task_id in self.workstation_manager.tasks:
                task = self.workstation_manager.tasks[task_id]
                if task.status.value == 'COMPLETED':
                    completed_tasks.append(task_id)
                else:
                    incomplete_tasks.append(task_id)
        
        all_completed = len(incomplete_tasks) == 0
        
        if all_completed:
            # 所有任務完成，波次完成
            wave.status = WaveStatus.COMPLETED
            wave.actual_completion_time = current_time
            wave.completed_tasks = len(completed_tasks)
            
            # 從活躍清單移除
            if wave_id in self.active_waves:
                self.active_waves.remove(wave_id)
            if wave_id not in self.wave_history:
                self.wave_history.append(wave_id)
            
            self.logger.info(f"✅ 波次 {wave_id} 實際完成（所有 {len(completed_tasks)} 個任務已完成）")
        
        return {
            'completed': all_completed,
            'total_tasks': len(wave.task_ids),
            'completed_tasks': len(completed_tasks),
            'incomplete_tasks': incomplete_tasks,
            'completion_time': current_time if all_completed else None
        }

    def can_station_start_next_wave(self, station_id: str, next_wave_id: str) -> bool:
        """🆕 新增：檢查工作站是否可以開始下一個波次的一般出貨"""
        if next_wave_id not in self.waves:
            return False
        
        next_wave = self.waves[next_wave_id]
        
        # 只限制一般出貨，其他任務可以做
        if next_wave.wave_type != WaveType.SCHEDULED:
            return True
        
        # 檢查當前所有活躍的一般出貨波次是否都完成
        for active_wave_id in self.active_waves:
            if active_wave_id in self.waves:
                active_wave = self.waves[active_wave_id]
                
                # 如果是一般出貨波次且該工作站有參與
                if (active_wave.wave_type == WaveType.SCHEDULED and 
                    station_id in active_wave.assigned_workstations):
                    
                    # 檢查該波次是否完成
                    completion_result = self.check_wave_actual_completion(active_wave_id, datetime.now())
                    if not completion_result['completed']:
                        return False  # 還有未完成的波次，不能開始下一個
        
        return True
    
    def _parse_time_string(self, time_str: str) -> Optional[time]:
        """強化版時間解析：支援各種數字格式"""
        try:
            if pd.isna(time_str) or time_str == '':
                return None
                
            # 先轉為字串並清理
            time_str = str(time_str).strip()
            
            # 移除可能的小數點
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
                    self.logger.warning(f"時間超出範圍: {hour}:{minute:02d} (原始值: {time_str})")
                    return None
            
            # 處理已經包含冒號的格式
            elif ':' in time_str:
                parts = time_str.split(':')
                if len(parts) >= 2:
                    hour = int(parts[0])
                    minute = int(parts[1])
                    
                    if 0 <= hour <= 23 and 0 <= minute <= 59:
                        return time(hour, minute)
            
            self.logger.warning(f"無法解析時間格式: '{time_str}'")
            return None
            
        except (ValueError, TypeError, AttributeError) as e:
            self.logger.warning(f"時間格式錯誤: '{time_str}' - {str(e)}")
            return None
    
    def _build_delivery_waves_map(self):
        """🆕 建立出車時間對應表"""
        if self.route_schedule is None or len(self.route_schedule) == 0:
            self.logger.error("route_schedule_master 資料未載入或為空！")
            return
        
        # 按 DELIVERTM 分組
        delivery_groups = self.route_schedule.groupby('DELIVERTM')
        
        self.delivery_waves_map = {}
        self.partcustid_to_waves = defaultdict(list)
        
        for delivery_time, group_data in delivery_groups:
            delivery_time_str = str(delivery_time).zfill(4)  # 確保4位數
            
            # 收集這個出車時間的所有資訊
            routes = group_data['ROUTECD'].unique().tolist()
            partcustids = group_data['PARTCUSTID'].tolist()
            
            # 收集所有截止時間（處理空值）
            cutoff_times = []
            for cutoff in group_data['ORDERENDTIME'].dropna().unique():
                if cutoff != '' and not pd.isna(cutoff):
                    cutoff_times.append(str(cutoff).zfill(4))
            
            # 找最晚截止時間
            latest_cutoff = max(cutoff_times) if cutoff_times else delivery_time_str
            
            wave_info = {
                'delivery_time': delivery_time_str,
                'routes': routes,
                'partcustids': partcustids,
                'cutoff_times': cutoff_times,
                'latest_cutoff': latest_cutoff,
                'weekend_only': False  # 先處理平日，週末邏輯後續加入
            }
            
            self.delivery_waves_map[delivery_time_str] = wave_info
            
            # 建立據點反向查找
            for partcustid in partcustids:
                self.partcustid_to_waves[partcustid].append(delivery_time_str)
        
        self.logger.info(f"建立出車時間對應表完成: {len(self.delivery_waves_map)} 個出車時間")
    
    def create_waves_from_schedule(self, target_date: datetime, include_weekend: bool = False) -> List[Wave]:
        """🆕 重寫：從出車時刻表建立當日波次"""
        """🆕 重寫：從出車時刻表建立當日波次（排除週末）"""
    
        # 🆕 檢查是否為工作日
        if not self.data_manager.is_workday(target_date):
            self.logger.info(f"{target_date.date()} 為週末，跳過波次建立")
            return []
        
        self.logger.info(f"從出車時刻表建立 {target_date.date()} 的波次...")
        created_waves = []
        
        # 🆕 週末邏輯處理（簡化版）
        is_saturday = target_date.weekday() == 5
        
        for delivery_time_str, wave_info in self.delivery_waves_map.items():
            # 簡化：先不處理週末邏輯，後續可擴展
            if not include_weekend and is_saturday:
                continue
                
            wave = self._create_wave_from_delivery_time(wave_info, target_date)
            if wave:
                self.waves[wave.wave_id] = wave
                created_waves.append(wave)
        
        # 按出車時間排序
        created_waves.sort(key=lambda w: w.delivery_datetime)
        
        self.logger.info(f"✅ 建立 {len(created_waves)} 個出車波次")
        return created_waves
    
    def _create_wave_from_delivery_time(self, wave_info: Dict, target_date: datetime) -> Optional[Wave]:
        """🆕 從出車時間建立波次"""
        delivery_time_str = wave_info['delivery_time']
        latest_cutoff_str = wave_info['latest_cutoff']
        
        # 解析時間
        delivery_time = self._parse_time_string(delivery_time_str)
        latest_cutoff_time = self._parse_time_string(latest_cutoff_str)
        
        if not delivery_time or not latest_cutoff_time:
            self.logger.warning(f"時間解析失敗: {delivery_time_str}, {latest_cutoff_str}")
            return None
        
        # 建立完整日期時間
        target_date_only = target_date.date()
        delivery_datetime = datetime.combine(target_date_only, delivery_time)
        cutoff_datetime = datetime.combine(target_date_only, latest_cutoff_time)
        
        # 處理跨日情況
        if delivery_time < latest_cutoff_time:
            delivery_datetime += timedelta(days=1)
        
        # 計算可用時間
        if delivery_datetime > cutoff_datetime:
            work_time_minutes = (delivery_datetime - cutoff_datetime).total_seconds() / 60
        else:
            work_time_minutes = 0
            self.logger.warning(f"出車時間早於截止時間: {delivery_time_str}")
        
        # 生成波次ID
        wave_id = f"WAVE_{delivery_time_str}_{target_date.strftime('%Y%m%d')}"
        
        wave = Wave(
            wave_id=wave_id,
            delivery_time_str=delivery_time_str,
            delivery_datetime=delivery_datetime,
            latest_cutoff_time=cutoff_datetime,
            included_routes=wave_info['routes'],
            included_partcustids=wave_info['partcustids'],
            cutoff_times=wave_info['cutoff_times'],
            metadata={
                'wave_info': wave_info,
                'work_time_minutes': work_time_minutes
            }
        )
        
        return wave
    
    def find_wave_for_partcustid(self, partcustid: str, order_time: datetime = None) -> Optional[str]:
        """🆕 根據據點找到對應的波次"""
        if partcustid not in self.partcustid_to_waves:
            self.logger.warning(f"找不到據點 {partcustid} 的波次資訊")
            return None
        
        possible_waves = self.partcustid_to_waves[partcustid]
        
        if not order_time:
            # 沒有訂單時間，返回第一個可用波次
            return f"WAVE_{possible_waves[0]}_DEFAULT" if possible_waves else None
        
        # 有訂單時間，找到可以趕上的波次
        order_time_str = order_time.strftime('%H%M')
        
        for wave_delivery_time in sorted(possible_waves):
            wave_info = self.delivery_waves_map[wave_delivery_time]
            latest_cutoff = wave_info['latest_cutoff']
            
            if order_time_str <= latest_cutoff:
                # 趕得上這一波
                wave_id = f"WAVE_{wave_delivery_time}_{order_time.strftime('%Y%m%d')}"
                return wave_id
        
        # 都趕不上，返回最後一個（或者可以擴展到明天）
        if possible_waves:
            last_wave_delivery_time = sorted(possible_waves)[-1]
            wave_id = f"WAVE_{last_wave_delivery_time}_{order_time.strftime('%Y%m%d')}"
            return wave_id
        
        return None
    
    
    def _parse_time_string(self, time_str: str) -> Optional[time]:
        """🔧 修復：解析新的時間格式（支援浮點數）"""
        try:
            time_str = str(time_str).strip()
            
            # 🆕 處理浮點數格式（如 '855.0' -> '855'）
            if '.' in time_str:
                time_str = time_str.split('.')[0]
            
            # 處理不同長度的時間字串
            if time_str.isdigit():
                time_str = time_str.zfill(4)  # 確保4位數，855 -> 0855
            
            if len(time_str) == 4:
                hour = int(time_str[:2])
                minute = int(time_str[2:])
                
                # 驗證時間範圍
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    return time(hour, minute)
                else:
                    self.logger.warning(f"時間超出範圍: {hour}:{minute}")
                    return None
            
            self.logger.warning(f"時間格式錯誤: '{time_str}'")
            return None
        
        except (ValueError, IndexError) as e:
            self.logger.warning(f"時間格式錯誤: '{time_str}' - {str(e)}")
            return None
        
    def get_floor_work_time_minutes(self, floor: int, priority_level: str) -> int:
        """根據樓層和優先權取得固定作業時間"""
        if priority_level == 'P1':  # P2改名為P1後，這是一般訂單
            if floor == 3:
                return 30  # 3樓30分鐘
            elif floor == 2:
                return 25  # 2樓25分鐘
            else:
                return 30  # 其他樓層預設30分鐘
        else:
            # P2緊急訂單和P3副倉庫/進貨使用空檔時間
            return self.available_work_time_minutes