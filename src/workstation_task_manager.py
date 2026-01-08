"""
WorkstationTaskManager - 工作站任務管理模組 (修改版：支援據點分配和波次截止時間)
負責管理工作站任務分配和執行約束，支援出貨和進貨任務
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, time, timedelta, date
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import random
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Any


class TaskStatus(Enum):
    """任務狀態枚舉"""
    PENDING = "PENDING"           # 等待中
    ASSIGNED = "ASSIGNED"         # 已分配
    IN_PROGRESS = "IN_PROGRESS"   # 執行中
    COMPLETED = "COMPLETED"       # 已完成
    PAUSED = "PAUSED"            # 暫停（異常處理）
    CANCELLED = "CANCELLED"       # 取消

class TaskType(Enum):
    """🆕 任務類型枚舉"""
    SHIPPING = "SHIPPING"         # 出貨任務
    RECEIVING = "RECEIVING"       # 進貨任務

class StationStatus(Enum):
    """工作站狀態枚舉"""
    IDLE = "IDLE"                # 空閒
    STARTING_UP = "STARTING_UP"  # 啟動中
    BUSY = "BUSY"                # 忙碌
    MAINTENANCE = "MAINTENANCE"   # 維護中
    RESERVED = "RESERVED"        # 異常處理預留

@dataclass
class Task:
    """🔧 修改：任務物件（支援據點分配）"""
    task_id: str
    order_id: str  # 對於進貨任務，這是進貨單號
    frcd: str
    partno: str
    quantity: int
    floor: int
    priority_level: str
    requires_repack: bool
    estimated_duration: float  # 分鐘
    
    # 🆕 新增：任務類型
    task_type: TaskType = TaskType.SHIPPING
    
    # 🆕 新增：據點資訊（出貨任務用）
    partcustid: Optional[str] = None    # 據點ID
    wave_id: Optional[str] = None       # 波次ID
    delivery_deadline: Optional[datetime] = None  # 出車截止時間
    available_work_minutes: Optional[int] = None  # 可用作業時間
    
    # 🆕 新增：進貨專用欄位
    arrival_date: Optional[date] = None  # 到貨日期
    deadline_date: Optional[date] = None  # 截止日期
    days_since_arrival: int = 0
    is_overdue: bool = False
    
    # 路線資訊（出貨任務用）
    route_code: Optional[str] = None
    route_group: Optional[int] = None
    assigned_wave: Optional[str] = None
    
    # 分配資訊
    assigned_station: Optional[str] = None
    assigned_staff: Optional[int] = None
    status: TaskStatus = TaskStatus.PENDING
    start_time: Optional[datetime] = None
    estimated_completion: Optional[datetime] = None
    actual_completion: Optional[datetime] = None

    # 🆕 新增：區分預估和實際時間
    actual_duration: Optional[float] = None  # 實際執行時間（包含隨機性）
    actual_start_time: Optional[datetime] = None  # 實際開始時間
    wave_sequence_number: Optional[int] = None  # 🆕 在波次中的順序號

@dataclass
class PartcustidGroup:
    """🆕 新增：據點分組物件"""
    partcustid: str
    route_code: str
    tasks: List[Task]
    total_workload_minutes: float = 0.0
    task_count: int = 0
    
    def __post_init__(self):
        self.total_workload_minutes = sum(task.estimated_duration for task in self.tasks)
        self.task_count = len(self.tasks)

@dataclass
class StationAssignment:
    """🆕 新增：工作站分配物件"""
    station_id: str
    partcustid_groups: List[PartcustidGroup]
    total_workload_minutes: float
    total_partcustids: int
    estimated_completion_time: Optional[datetime] = None
    
    def __post_init__(self):
        self.total_workload_minutes = sum(group.total_workload_minutes for group in self.partcustid_groups)
        self.total_partcustids = len(self.partcustid_groups)
        
@dataclass
class WorkStation:
    """工作站物件"""
    station_id: str
    floor: int
    is_fixed: bool  # 固定工作站 vs 臨時工作站
    status: StationStatus = StationStatus.IDLE
    current_task: Optional[Task] = None
    assigned_staff: Optional[int] = None
    startup_time: Optional[datetime] = None
    available_time: Optional[datetime] = None
    reserved_for_exception: bool = False

class WorkstationTaskManager:
    def __init__(self, data_manager, wave_manager=None):
        """初始化工作站任務管理器"""
        self.logger = logging.getLogger(__name__)
        self.data_manager = data_manager
        self.wave_manager = wave_manager
        self.item_master = data_manager.master_data.get('item_master')
        self.workstation_capacity = data_manager.master_data.get('workstation_capacity')
        self.staff_master = data_manager.master_data.get('staff_skill_master')
        
        # 載入工作站相關參數
        self._load_workstation_parameters()
        
        # 初始化工作站和任務追蹤
        self.workstations: Dict[str, WorkStation] = {}
        self.tasks: Dict[str, Task] = {}
        self.task_queue: List[str] = []  # 任務佇列
        
        # 🆕 新增：加班任務追蹤
        self.overtime_tasks: Dict[str, Task] = {}
        self.pending_overtime_requirements: Dict[str, Dict] = {}
        
        # 🆕 新增：據點分配追蹤
        self.partcustid_assignments: Dict[str, StationAssignment] = {}  # station_id -> assignment
        self.station_availability_tracker: Dict[str, datetime] = {}
        
        # 初始化工作站
        self._initialize_workstations()
        # 初始化所有工作站為可用
        for station_id in self.workstations.keys():
            self.station_availability_tracker[station_id] = datetime.now()
        
        def _update_station_availability(self, station_id: str, available_time: datetime):
            """更新工作站可用時間"""
            self.station_availability_tracker[station_id] = available_time

        def _get_available_gap_stations(self, current_time: datetime, used_stations: set) -> List[str]:
            """取得可用的空檔工作站"""
            available_stations = []
            for station_id, available_time in self.station_availability_tracker.items():
                if station_id not in used_stations and available_time <= current_time:
                    available_stations.append(station_id)
            return available_stations
        
    def _load_workstation_parameters(self):
        """載入工作站相關參數"""
        # 從系統參數載入（單位：秒）
        self.raw_params = {
            'station_startup_time_seconds': self.data_manager.get_parameter_value('station_startup_time_minutes', 180),
            'picking_base_time_repack_seconds': self.data_manager.get_parameter_value('picking_base_time_repack', 45),
            'picking_base_time_no_repack_seconds': self.data_manager.get_parameter_value('picking_base_time_no_repack', 30),
            'repack_additional_time_seconds': self.data_manager.get_parameter_value('repack_additional_time', 15),
            'task_interruption_allowed': self.data_manager.get_parameter_value('task_interruption_allowed', 'Y'),
            'skill_impact_multiplier': self.data_manager.get_parameter_value('skill_impact_multiplier', 0.2),
            'min_task_duration_seconds': self.data_manager.get_parameter_value('min_task_duration', 15),
            'max_task_duration_seconds': self.data_manager.get_parameter_value('max_task_duration', 300),
            
            # 🆕 新增：進貨相關參數
            'receiving_time_per_piece_seconds': self.data_manager.get_parameter_value('receiving_time_per_piece', 5),  # 每零件5秒
            'receiving_completion_days': self.data_manager.get_parameter_value('receiving_completion_days', 3),
            'receiving_time_variance_factor': self.data_manager.get_parameter_value('receiving_time_variance_factor', 0.15),  # 15%變動
            
            # 🆕 新增：據點分配參數
            'max_partcustids_per_station': self.data_manager.get_parameter_value('max_partcustids_per_station', 12),
            'time_buffer_minutes': self.data_manager.get_parameter_value('time_buffer_minutes', 10),  # 時間緩衝
        }
        
        # 轉換為分鐘單位以便內部使用
        self.params = {
            'station_startup_time_minutes': self.raw_params['station_startup_time_seconds'] / 60.0,
            'picking_base_time_repack': self.raw_params['picking_base_time_repack_seconds'] / 60.0,
            'picking_base_time_no_repack': self.raw_params['picking_base_time_no_repack_seconds'] / 60.0,
            'repack_additional_time': self.raw_params['repack_additional_time_seconds'] / 60.0,
            'task_interruption_allowed': self.raw_params['task_interruption_allowed'],
            'skill_impact_multiplier': self.raw_params['skill_impact_multiplier'],
            'min_task_duration': self.raw_params['min_task_duration_seconds'] / 60.0,
            'max_task_duration': self.raw_params['max_task_duration_seconds'] / 60.0,
            
            # 進貨相關參數
            'receiving_time_per_piece': self.raw_params['receiving_time_per_piece_seconds'] / 60.0,  # 轉為分鐘
            'receiving_completion_days': self.raw_params['receiving_completion_days'],
            'receiving_time_variance_factor': self.raw_params['receiving_time_variance_factor'],
            
            # 據點分配參數
            'max_partcustids_per_station': self.raw_params['max_partcustids_per_station'],
            'time_buffer_minutes': self.raw_params['time_buffer_minutes'],
        }
        
        self.logger.info(f"工作站參數載入完成（已轉換為分鐘）:")
        self.logger.info(f"  每零件處理時間: {self.params['receiving_time_per_piece']:.3f} 分鐘 ({self.raw_params['receiving_time_per_piece_seconds']} 秒)")
        self.logger.info(f"  進貨完成期限: {self.params['receiving_completion_days']} 天")
        self.logger.info(f"  最大據點數/工作站: {self.params['max_partcustids_per_station']}")
        self.logger.info(f"  時間變動係數: ±{self.params['receiving_time_variance_factor']*100:.0f}%")
    
    def _initialize_workstations(self):
        """初始化所有工作站"""
        if self.workstation_capacity is None:
            self.logger.error("workstation_capacity 資料未載入")
            return
        
        self.logger.info("初始化工作站...")
        
        for _, capacity_row in self.workstation_capacity.iterrows():
            floor = int(capacity_row['floor'])
            fixed_stations = int(capacity_row['fixed_stations'])
            temp_stations = int(capacity_row['temp_stations'])
            
            # 建立固定工作站
            for i in range(fixed_stations):
                station_id = f"ST{floor}F{i+1:02d}"
                self.workstations[station_id] = WorkStation(
                    station_id=station_id,
                    floor=floor,
                    is_fixed=True
                )
            
            # 建立臨時工作站
            for i in range(temp_stations):
                station_id = f"ST{floor}T{i+1:02d}"
                self.workstations[station_id] = WorkStation(
                    station_id=station_id,
                    floor=floor,
                    is_fixed=False
                )
        
        self.logger.info(f"✅ 工作站初始化完成，總計 {len(self.workstations)} 個工作站")
    
    def create_tasks_from_orders(self, processed_orders: pd.DataFrame) -> List[Task]:
        """🔧 修改：從處理過的訂單建立出貨任務（支援據點分配）"""
        self.logger.info(f"從 {len(processed_orders)} 筆訂單建立出貨任務...")
        
        created_tasks = []
        
        for idx, order in processed_orders.iterrows():
            # 取得零件資訊
            item_info = self._get_item_info(order['FRCD'], order['PARTNO'])
            if not item_info:
                self.logger.warning(f"零件資訊缺失: {order['FRCD']}-{order['PARTNO']}")
                continue
            
            # 從訂單中取得路線資訊
            route_code = str(order.get('ROUTECD', ''))
            route_group = None
            
            # 處理副倉庫和 ROUTEGRP 問題
            routegrp_value = order.get('ROUTEGRP', None)
            is_sub_warehouse = route_code in ['SDTC', 'SDHN']
            
            if is_sub_warehouse:
                route_group = None
            else:
                if routegrp_value is not None and not pd.isna(routegrp_value):
                    try:
                        routegrp_str = str(routegrp_value)
                        if routegrp_str.startswith('0'):
                            route_group = int(routegrp_str.lstrip('0')) if routegrp_str.lstrip('0') else 0
                        else:
                            route_group = int(routegrp_str)
                    except (ValueError, TypeError):
                        self.logger.warning(f"訂單 {order['INDEXNO']} ROUTEGRP 格式錯誤，跳過此訂單")
                        continue
                else:
                    self.logger.warning(f"訂單 {order['INDEXNO']} 缺少ROUTEGRP，跳過此訂單")
                    continue
            
            # 🆕 新增：取得據點和時間資訊
            partcustid = str(order.get('PARTCUSTID', ''))
            delivery_deadline = order.get('delivery_time')  # 從 order_priority_manager 處理結果
            available_minutes = order.get('available_minutes')
            
            # 🔧 修改：建立出貨任務
            task = Task(
                task_id=f"T_SHIP_{order['INDEXNO']}",
                order_id=order['INDEXNO'],
                frcd=order['FRCD'],
                partno=order['PARTNO'],
                quantity=order['SALEQTY'],
                floor=item_info['floor'],
                priority_level=order.get('priority_level', 'P2'),
                requires_repack=(item_info['repack'] == 'Y'),
                estimated_duration=0,  # 待計算
                task_type=TaskType.SHIPPING,  # 🆕 出貨任務
                partcustid=partcustid if partcustid else None,  # 🆕 據點ID
                route_code=route_code if route_code else None,
                route_group=route_group,
                delivery_deadline=delivery_deadline,  # 🆕 截止時間
                available_work_minutes=available_minutes  # 🆕 可用時間
            )
            
            # 計算預估執行時間
            task.estimated_duration = self.calculate_estimated_duration_fixed(task)
            
            self.tasks[task.task_id] = task
            created_tasks.append(task)
        
        self.logger.info(f"✅ 建立 {len(created_tasks)} 個出貨任務")
        
        return created_tasks
    
    def create_tasks_from_receiving(self, processed_receiving: pd.DataFrame, current_date: date) -> List[Task]:
        """🔧 修改：從處理過的進貨資料建立進貨任務（新增時間變動）"""
        self.logger.info(f"從 {len(processed_receiving)} 筆進貨資料建立進貨任務...")
        
        created_tasks = []
        
        for idx, receiving in processed_receiving.iterrows():
            # 取得零件資訊
            item_info = self._get_item_info(receiving['FRCD'], receiving['PARTNO'])
            if not item_info:
                self.logger.warning(f"零件資訊缺失: {receiving['FRCD']}-{receiving['PARTNO']}")
                continue
            
            # 解析到貨日期
            arrival_date = self._parse_date(receiving.get('DATE', ''))
            if not arrival_date:
                arrival_date = current_date
            
            # 計算截止日期
            deadline_date = arrival_date + timedelta(days=self.params['receiving_completion_days'] - 1)
            
            # 計算已經過的天數
            days_since_arrival = (current_date - arrival_date).days
            is_overdue = current_date > deadline_date
            
            # 🔧 修改：使用固定時間計算
            estimated_duration = self.calculate_estimated_duration_fixed(task)
            
            # 建立進貨任務
            task = Task(
                task_id=f"T_RCV_{receiving.get('RECEIVING_ID', idx)}",
                order_id=str(receiving.get('RECEIVING_ID', f"RCV_{idx}")),
                frcd=receiving['FRCD'],
                partno=receiving['PARTNO'],
                quantity=receiving.get('QTY', 1),
                floor=item_info['floor'],
                priority_level=receiving.get('priority_level', 'P4'),
                requires_repack=False,  # 進貨通常不需要再包裝
                estimated_duration=estimated_duration,  # 🔧 使用固定計算
                task_type=TaskType.RECEIVING,  # 🆕 進貨任務
                arrival_date=arrival_date,
                deadline_date=deadline_date,
                days_since_arrival=days_since_arrival,
                is_overdue=is_overdue
            )
            
            self.tasks[task.task_id] = task
            created_tasks.append(task)
        
        # 統計結果
        overdue_count = sum(1 for task in created_tasks if task.is_overdue)
        due_today_count = sum(1 for task in created_tasks if task.deadline_date == current_date)
        
        self.logger.info(f"✅ 建立 {len(created_tasks)} 個進貨任務")
        if overdue_count > 0:
            self.logger.warning(f"🚨 其中 {overdue_count} 個已逾期")
        if due_today_count > 0:
            self.logger.info(f"⏰ 其中 {due_today_count} 個今天截止")
        
        return created_tasks
    
    def _calculate_receiving_duration(self, receiving_row: pd.Series, item_info: Dict) -> float:
        """🔧 修改：計算進貨處理時間（按零件數量 × 每件時間，無基礎時間）"""
        time_per_piece = self.params['receiving_time_per_piece']  # 每零件時間（分鐘）
        
        # 📦 零件數量
        quantity = receiving_row.get('QTY', 1)
        
        # 🧮 核心計算：零件數量 × 每件時間
        calculated_time = quantity * time_per_piece
        
        # 🔧 零件複雜度影響（如果零件需要再包裝，處理時間稍長）
        complexity_factor = 1.0
        if item_info.get('repack') == 'Y':
            complexity_factor = 1.1  # 複雜零件增加10%時間
        
        # 📊 隨機變動（模擬人員熟練度和零件狀況差異）
        variance_factor = self.params['receiving_time_variance_factor']
        random_multiplier = random.uniform(1 - variance_factor, 1 + variance_factor)
        
        # 計算最終時間
        total_time = calculated_time * complexity_factor * random_multiplier
        
        # 確保在合理範圍內（最少1分鐘，最多根據數量合理上限）
        min_time = max(1.0, quantity * time_per_piece * 0.5)  # 最少為理論時間的一半，但不少於1分鐘
        max_time = quantity * time_per_piece * 3  # 最多為理論時間的3倍
        total_time = max(min_time, min(max_time, total_time))
        
        return round(total_time, 2)
    


    def assign_tasks_to_stations(self, tasks: List[Task], staff_schedule: pd.DataFrame, 
                                current_time: datetime) -> Dict[str, List[str]]:
        """🔧 修正：確保總是返回有效的字典結果"""
        self.logger.info(f"開始分階段分配 {len(tasks)} 個任務到工作站...")
        
        # 🔧 初始化結果字典（確保不會返回None）
        assignment_results = {
            'assigned': [],
            'unassigned': [],
            'errors': [],
            'overtime_required': [],
            'wave_analysis': {}
        }
        
        try:
            # 🆕 按任務類型和波次分組
            task_groups = self._group_tasks_by_type_and_wave(tasks, current_time)
            
            # 🆕 追蹤已分配的工作站（避免重複分配）
            assigned_stations = set()
            
            # 🆕 第1階段：P1 一般出貨（按波次處理）
            if 'shipping_waves' in task_groups:
                for wave_id, wave_tasks in task_groups['shipping_waves'].items():
                    self.logger.info(f"🌊 處理波次 {wave_id}: {len(wave_tasks)} 個出貨任務")
                    
                    # 按優先權分離
                    p1_tasks = [task for task in wave_tasks if task.priority_level == 'P1']
                    p2_tasks = [task for task in wave_tasks if task.priority_level == 'P2'] 
                    p3_tasks = [task for task in wave_tasks if task.priority_level == 'P3']
                    
                    # P1 一般訂單（最高優先權）
                    if p1_tasks:
                        p1_result = self._assign_p1_wave_tasks(p1_tasks, staff_schedule, current_time)
                        assignment_results['assigned'].extend(p1_result['assigned'])
                        assignment_results['unassigned'].extend(p1_result['unassigned'])
                        assignment_results['errors'].extend(p1_result['errors'])
                        assigned_stations.update(p1_result['used_stations'])
                    
                    # P2 緊急訂單（利用空檔）
                    if p2_tasks:
                        p2_result = self._assign_p2_gap_tasks(p2_tasks, staff_schedule, current_time, assigned_stations)
                        assignment_results['assigned'].extend(p2_result['assigned'])
                        assignment_results['unassigned'].extend(p2_result['unassigned'])
                        assigned_stations.update(p2_result['used_stations'])
                    
                    # P3 副倉庫（最後空檔）
                    if p3_tasks:
                        p3_result = self._assign_p3_and_receiving_gap_tasks(p3_tasks, staff_schedule, current_time, assigned_stations)
                        assignment_results['assigned'].extend(p3_result['assigned'])
                        assignment_results['unassigned'].extend(p3_result['unassigned'])
                        assigned_stations.update(p3_result['used_stations'])
            
            # 🆕 處理其他類型任務（進貨等）
            other_tasks = []
            for task_type in ['overdue_receiving', 'normal_receiving', 'due_today_receiving']:
                if task_type in task_groups:
                    other_tasks.extend(task_groups[task_type])
            
            if other_tasks:
                other_result = self._assign_p3_and_receiving_gap_tasks(other_tasks, staff_schedule, current_time, assigned_stations)
                assignment_results['assigned'].extend(other_result['assigned'])
                assignment_results['unassigned'].extend(other_result['unassigned'])
            
            # 統計結果
            assigned_count = len(assignment_results['assigned'])
            unassigned_count = len(assignment_results['unassigned'])
            
            self.logger.info(f"✅ 分階段任務分配完成: 已分配 {assigned_count}, 未分配 {unassigned_count}")
            
            return assignment_results
            
        except Exception as e:
            self.logger.error(f"任務分配發生錯誤: {str(e)}")
            # 🔧 確保即使出錯也返回有效字典
            assignment_results['errors'] = [task.task_id for task in tasks]
            return assignment_results



    def _group_tasks_by_type_and_wave(self, tasks: List[Task], current_time: datetime) -> Dict[str, Any]:
        """🆕 新增：按任務類型和波次分組"""
        task_groups = {
            'shipping_waves': defaultdict(list),  # wave_id -> tasks
            'overdue_receiving': [],
            'sub_warehouse_shipping': [],
            'normal_receiving': [],
            'due_today_receiving': []
        }
        
        current_date = current_time.date()
        
        for task in tasks:
            if task.task_type == TaskType.SHIPPING:
                # 🔧 修改：完整的副倉庫識別邏輯
                is_sub_warehouse = (
                    task.route_code in ['SDTC', 'SDHN'] or  # 直接副倉庫路線
                    (task.route_code == 'R15' and task.partcustid == 'SDTC') or  # R15-SDTC 組合
                    (task.route_code == 'R16' and task.partcustid == 'SDHN')     # R16-SDHN 組合
                )
                
                if is_sub_warehouse:
                    task_groups['sub_warehouse_shipping'].append(task)
                else:
                    # 🆕 按波次分組一般出貨任務
                    wave_id = self._determine_task_wave_id(task, current_time)
                    task_groups['shipping_waves'][wave_id].append(task)
                    
            elif task.task_type == TaskType.RECEIVING:
                if task.is_overdue:
                    task_groups['overdue_receiving'].append(task)
                elif task.deadline_date == current_date:
                    task_groups['due_today_receiving'].append(task)
                else:
                    task_groups['normal_receiving'].append(task)
        
        # 記錄分組結果
        self.logger.info(f"📊 任務分組結果:")
        self.logger.info(f"  一般出貨波次: {len(task_groups['shipping_waves'])} 個波次")
        for wave_id, wave_tasks in task_groups['shipping_waves'].items():
            self.logger.info(f"    {wave_id}: {len(wave_tasks)} 個任務")
        for group_name, group_tasks in task_groups.items():
            if group_name != 'shipping_waves' and group_tasks:
                self.logger.info(f"  {group_name}: {len(group_tasks)} 個任務")
        
        return task_groups
    
    def _determine_task_wave_id(self, task: Task, current_time: datetime) -> str:
        """🆕 修改：確定任務所屬波次"""
        if not task.partcustid:
            return "WAVE_DEFAULT"
        
        # 🆕 使用 WaveManager 的新方法
        if hasattr(self, 'wave_manager'):
            wave_id = self.wave_manager.find_wave_for_partcustid(task.partcustid, current_time)
            return wave_id if wave_id else "WAVE_DEFAULT"
        
        # 備用邏輯
        return f"WAVE_UNKNOWN_{current_time.strftime('%H%M')}"
    
    def _assign_wave_tasks_with_partcustid_grouping(self, wave_tasks: List[Task], 
                                                   staff_schedule: pd.DataFrame,
                                                   current_time: datetime,
                                                   assigned_stations: set) -> Dict:
        """🆕 新增：使用據點分組演算法分配波次任務"""
        wave_result = {
            'assigned': [],
            'unassigned': [],
            'errors': [],
            'overtime_required': [],
            'assigned_stations': set(),
            'analysis': {}
        }
        
        if not wave_tasks:
            return wave_result
        
        # 🔍 Step 1: 檢查截止時間約束
        deadline_check = self._check_wave_deadline_feasibility(wave_tasks, current_time)
        wave_result['analysis']['deadline_check'] = deadline_check
        
        if not deadline_check['feasible']:
            self.logger.warning(f"⚠️ 波次截止時間不可行: {deadline_check.get('feasibility_reason', 'unknown')}")
            self.logger.warning(f"   需要 {deadline_check.get('estimated_stations_needed', 0):.1f} 個工作站")
            self.logger.warning(f"   可用 {deadline_check.get('max_available_stations', 0)} 個工作站")
            self.logger.warning(f"   自動觸發加班邏輯")
            
            wave_result['overtime_required'] = [task.task_id for task in wave_tasks]
            wave_result['unassigned'] = [task.task_id for task in wave_tasks]
            wave_result['analysis']['infeasible_reason'] = deadline_check.get('feasibility_reason')
            return wave_result
        
        # 🏗️ Step 2: 按據點分組
        partcustid_groups = self._group_tasks_by_partcustid(wave_tasks)
        wave_result['analysis']['partcustid_groups'] = len(partcustid_groups)
        
        # 📊 Step 3: 使用 Bin Packing 演算法分配
        station_assignments = self._assign_partcustids_to_stations(
            partcustid_groups, current_time, assigned_stations, deadline_check['available_minutes']
        )
        
        # 📋 Step 4: 執行任務分配
        for assignment in station_assignments:
            try:
                success = self._execute_station_assignment(assignment, staff_schedule, current_time)
                if success:
                    # 記錄所有分配的任務
                    for group in assignment.partcustid_groups:
                        for task in group.tasks:
                            wave_result['assigned'].append(task.task_id)
                    wave_result['assigned_stations'].add(assignment.station_id)
                else:
                    # 分配失敗，記錄為未分配
                    for group in assignment.partcustid_groups:
                        for task in group.tasks:
                            wave_result['unassigned'].append(task.task_id)
            except Exception as e:
                self.logger.error(f"執行工作站分配時發生錯誤: {str(e)}")
                for group in assignment.partcustid_groups:
                    for task in group.tasks:
                        wave_result['errors'].append(task.task_id)
        
        # 記錄分析結果
        wave_result['analysis']['required_stations'] = len(station_assignments)
        wave_result['analysis']['assigned_stations'] = len(wave_result['assigned_stations'])
        wave_result['analysis']['assignment_efficiency'] = len(wave_result['assigned']) / len(wave_tasks) if wave_tasks else 0
        
        self.logger.info(f"📊 波次分配完成: 需要 {len(station_assignments)} 個工作站，成功分配 {len(wave_result['assigned'])}/{len(wave_tasks)} 個任務")
        
        return wave_result

    def _check_wave_deadline_feasibility(self, wave_tasks: List[Task], current_time: datetime) -> Dict:
        """🔧 修復：檢查波次截止時間可行性（修正變量定義順序）"""
        
        if not wave_tasks:
            return {'feasible': True, 'available_minutes': 0, 'required_minutes': 0}
        
        # 找到最早的截止時間
        deadlines = [task.delivery_deadline for task in wave_tasks if task.delivery_deadline]
        
        if not deadlines:
            return {'feasible': True, 'available_minutes': 480, 'required_minutes': 0}  # 假設8小時
        
        earliest_deadline = min(deadlines)
        
        # 🔧 修復：處理時間類型不匹配問題
        if isinstance(earliest_deadline, time):
            # 如果是 time 對象，轉換為同一天的 datetime 對象
            current_date = current_time.date()
            earliest_deadline_dt = datetime.combine(current_date, earliest_deadline)
            
            # 檢查是否跨日（如果截止時間早於當前時間，表示是明天）
            if earliest_deadline_dt <= current_time:
                earliest_deadline_dt += timedelta(days=1)
                
        elif isinstance(earliest_deadline, datetime):
            earliest_deadline_dt = earliest_deadline
        else:
            # 無法識別的時間格式
            self.logger.warning(f"無法識別的截止時間格式: {type(earliest_deadline)}")
            return {'feasible': True, 'available_minutes': 480, 'required_minutes': 0}
        
        # 計算可用時間（減去緩衝時間）
        available_minutes = (earliest_deadline_dt - current_time).total_seconds() / 60
        available_minutes -= self.params['time_buffer_minutes']  # 減去緩衝時間
        
        # 計算所需時間
        total_workload = sum(task.estimated_duration for task in wave_tasks)
        
        # 🔧 修復：先定義所有需要的變量
        max_partcustids_per_station = self.params['max_partcustids_per_station']
        
        # 計算唯一據點數量
        unique_partcustids = set()
        for task in wave_tasks:
            if task.partcustid:
                unique_partcustids.add(task.partcustid)
        
        # 🔧 修復：基於實際約束的可行性判斷
        # 計算所需工作站數（基於據點數量約束）
        stations_needed_by_partcustids = max(1, len(unique_partcustids) / max_partcustids_per_station)
        
        # 計算所需工作站數（基於時間約束）
        if available_minutes > 0:
            stations_needed_by_time = max(1, total_workload / available_minutes)
        else:
            stations_needed_by_time = float('inf')
        
        # 取兩者中較大的值
        estimated_stations_needed = max(stations_needed_by_partcustids, stations_needed_by_time)
        
        # 🔧 修復：檢查系統工作站容量
        max_available_stations = len(self.workstations)
        
        # 🔧 修復：更嚴格的可行性判斷
        time_feasible = available_minutes > 0
        capacity_feasible = estimated_stations_needed <= max_available_stations
        
        # 🔧 修正：改為檢查多工作站分配的可行性
        if len(unique_partcustids) > 0 and max_partcustids_per_station > 0:
            # 計算如果按據點約束分配，最繁忙的工作站需要多長時間（僅供記錄）
            avg_workload_per_partcustid = total_workload / len(unique_partcustids)
            max_partcustids_in_station = min(max_partcustids_per_station, len(unique_partcustids))
            max_single_station_time = avg_workload_per_partcustid * max_partcustids_in_station
            # 🔧 修正：不再將單工作站時間作為可行性判斷依據
            single_station_feasible = True  # 總是為True，因為我們可以用多個工作站
        else:
            single_station_feasible = True  # 總是為True
            max_single_station_time = total_workload

        workload_reasonable = total_workload <= available_minutes * max_available_stations

        # 🔧 修正：移除 single_station_feasible 的檢查，只檢查總體容量
        overall_feasible = time_feasible and capacity_feasible and workload_reasonable
        
        # 生成詳細的可行性報告
        feasibility_reason = []
        if not time_feasible:
            feasibility_reason.append(f"時間不足(可用:{available_minutes:.1f}分鐘)")
        if not capacity_feasible:
            feasibility_reason.append(f"工作站不足(需要:{estimated_stations_needed:.1f}, 可用:{max_available_stations})")
        if not single_station_feasible:
            feasibility_reason.append(f"單工作站超時(預估:{max_single_station_time:.1f}分鐘 > {available_minutes:.1f}分鐘)")
        if not workload_reasonable:
            feasibility_reason.append(f"工作負載過重(總負載:{total_workload:.1f}, 容量:{available_minutes * max_available_stations:.1f})")
        
        if not feasibility_reason:
            feasibility_reason.append("所有約束條件都滿足")
        
        result = {
            'feasible': overall_feasible,
            'earliest_deadline': earliest_deadline_dt,
            'available_minutes': max(0, available_minutes),
            'required_minutes': total_workload,
            'unique_partcustids': len(unique_partcustids),
            'stations_needed_by_partcustids': stations_needed_by_partcustids,
            'stations_needed_by_time': stations_needed_by_time,
            'estimated_stations_needed': estimated_stations_needed,
            'max_available_stations': max_available_stations,
            'max_single_station_time': max_single_station_time,
            'single_station_feasible': single_station_feasible,
            'feasibility_reason': '; '.join(feasibility_reason)
        }
        
        # 記錄詳細的可行性分析
        self.logger.info(f"🕐 波次可行性分析:")
        self.logger.info(f"   可用時間: {available_minutes:.1f} 分鐘")
        self.logger.info(f"   總工作負載: {total_workload:.1f} 分鐘")
        self.logger.info(f"   據點數量: {len(unique_partcustids)} 個")
        self.logger.info(f"   據點約束需要工作站: {stations_needed_by_partcustids:.1f} 個")
        self.logger.info(f"   時間約束需要工作站: {stations_needed_by_time:.1f} 個")
        self.logger.info(f"   單工作站最大負載: {max_single_station_time:.1f} 分鐘")
        self.logger.info(f"   最大可用工作站: {max_available_stations} 個")
        self.logger.info(f"   可行性結果: {'✅ 可行' if overall_feasible else '❌ 不可行'}")
        self.logger.info(f"   原因: {result['feasibility_reason']}")
        
        return result

    def _group_tasks_by_partcustid(self, tasks: List[Task]) -> List[PartcustidGroup]:
        """🆕 新增：按據點分組任務"""
        partcustid_dict = defaultdict(list)
        
        for task in tasks:
            if task.partcustid:
                partcustid_dict[task.partcustid].append(task)
            else:
                # 沒有據點的任務單獨成組
                partcustid_dict[f'NO_PARTCUSTID_{task.task_id}'].append(task)
        
        groups = []
        for partcustid, group_tasks in partcustid_dict.items():
            # 取得該組的路線代碼（應該相同）
            route_code = group_tasks[0].route_code if group_tasks else 'UNKNOWN'
            
            group = PartcustidGroup(
                partcustid=partcustid,
                route_code=route_code,
                tasks=group_tasks
            )
            groups.append(group)
        
        # 按工作量排序（大的據點優先分配）
        groups.sort(key=lambda g: g.total_workload_minutes, reverse=True)
        
        self.logger.info(f"📊 據點分組完成: {len(groups)} 個據點群組")
        for group in groups[:5]:  # 顯示前5個最大的
            self.logger.info(f"  {group.partcustid}: {group.task_count}任務, {group.total_workload_minutes:.1f}分鐘")
        
        return groups
    

    def _assign_partcustids_to_stations(self, partcustid_groups: List[PartcustidGroup],
                                    current_time: datetime, assigned_stations: set,
                                    available_minutes: float) -> List[StationAssignment]:
        """🔧 修復：使用 Bin Packing 演算法分配據點到工作站（簡化診斷）"""
        
        # 🚨 強制診斷：使用 print() 確保一定顯示
        print("🔥 DEBUG: _assign_partcustids_to_stations 開始執行")
        print(f"🔥 DEBUG: 輸入參數 - 據點群組數: {len(partcustid_groups)}")
        print(f"🔥 DEBUG: 輸入參數 - 可用時間: {available_minutes:.1f}分鐘")
        print(f"🔥 DEBUG: 輸入參數 - 已分配工作站: {assigned_stations}")
        
        # 🎯 目標：用最少工作站，在時間限制內完成所有任務
        max_partcustids = self.params['max_partcustids_per_station']
        max_time_per_station = available_minutes
        
        print(f"🔥 DEBUG: 約束條件 - 最大據點: {max_partcustids}")
        print(f"🔥 DEBUG: 約束條件 - 最大時間: {max_time_per_station:.1f}分鐘")
        
        assignments = []
        
        # 🔧 修復：按樓層分組處理，確保跨樓層分配
        floor_groups = defaultdict(list)
        for group in partcustid_groups:
            # 取得該據點任務的樓層
            if group.tasks:
                floor = group.tasks[0].floor
                floor_groups[floor].append(group)
        
        print(f"🔥 DEBUG: 樓層分組 - {dict((floor, len(groups)) for floor, groups in floor_groups.items())}")
        
        # 為每個樓層分配工作站
        for floor, floor_partcustid_groups in floor_groups.items():
            print(f"🔥 DEBUG: 處理樓層 {floor} - {len(floor_partcustid_groups)} 個據點群組")
            
            current_assignment = None
            
            # 按工作量排序（大的據點優先分配）
            floor_partcustid_groups.sort(key=lambda g: g.total_workload_minutes, reverse=True)
            
            for i, partcustid_group in enumerate(floor_partcustid_groups):
                print(f"🔥 DEBUG: 處理據點 {i+1}/{len(floor_partcustid_groups)}: {partcustid_group.partcustid}")
                print(f"🔥 DEBUG: 據點工作負載: {partcustid_group.total_workload_minutes:.1f}分鐘")

                # 🔧 修復：正確的容量檢查邏輯
                can_fit_current = False
                
                if current_assignment is not None:
                    new_partcustid_count = current_assignment.total_partcustids + 1
                    new_total_time = current_assignment.total_workload_minutes + partcustid_group.total_workload_minutes
                    
                    # 🚨 關鍵檢查：約束條件
                    partcustid_ok = new_partcustid_count <= max_partcustids
                    time_ok = new_total_time <= max_time_per_station
                    
                    can_fit_current = partcustid_ok and time_ok
                    
                    print(f"🔥 DEBUG: 容量檢查 - 工作站: {current_assignment.station_id}")
                    print(f"🔥 DEBUG: 容量檢查 - 據點: {new_partcustid_count}/{max_partcustids} ({'OK' if partcustid_ok else 'FAIL'})")
                    print(f"🔥 DEBUG: 容量檢查 - 時間: {new_total_time:.1f}/{max_time_per_station:.1f} ({'OK' if time_ok else 'FAIL'})")
                    print(f"🔥 DEBUG: 容量檢查 - 結果: {'可加入' if can_fit_current else '需要新工作站'}")
                else:
                    print(f"🔥 DEBUG: 無current_assignment，需要新工作站")
                
                if can_fit_current:
                    # 加入當前工作站
                    current_assignment.partcustid_groups.append(partcustid_group)
                    
                    # 🚨 關鍵修復：手動更新統計數據
                    current_assignment.total_partcustids = len(current_assignment.partcustid_groups)
                    current_assignment.total_workload_minutes = sum(g.total_workload_minutes for g in current_assignment.partcustid_groups)
                    
                    print(f"🔥 DEBUG: 據點 {partcustid_group.partcustid} 加入工作站 {current_assignment.station_id}")
                    print(f"🔥 DEBUG: 更新後統計: {current_assignment.total_partcustids}據點, {current_assignment.total_workload_minutes:.1f}分鐘")
                else:
                    # 需要新工作站
                    if current_assignment:
                        assignments.append(current_assignment)
                        print(f"🔥 DEBUG: 完成工作站 {current_assignment.station_id} - {current_assignment.total_partcustids}據點, {current_assignment.total_workload_minutes:.1f}分鐘")
                    
                    print(f"🔥 DEBUG: 尋找樓層{floor}的新工作站...")
                    print(f"🔥 DEBUG: 當前已分配: {assigned_stations}")
                    
                    # 🔧 關鍵診斷：檢查工作站查找邏輯
                    available_station = self._find_next_available_station_by_floor(assigned_stations, floor)
                    
                    print(f"🔥 DEBUG: 工作站查找結果: {available_station}")
                    
                    if available_station:
                        current_assignment = StationAssignment(
                            station_id=available_station,
                            partcustid_groups=[partcustid_group],
                            total_workload_minutes=partcustid_group.total_workload_minutes,  # 🚨 修復
                            total_partcustids=1  # 🚨 修復
                        )
                        assigned_stations.add(available_station)
                        print(f"🔥 DEBUG: 新工作站 {available_station} 開始處理據點 {partcustid_group.partcustid}")
                        print(f"🔥 DEBUG: 更新assigned_stations: {assigned_stations}")
                    else:
                        print(f"🔥 DEBUG: ❌❌❌ 找不到樓層{floor}的可用工作站！這是問題所在！")
                        print(f"🔥 DEBUG: ❌❌❌ 據點 {partcustid_group.partcustid} 無法分配")
                        current_assignment = None
                        continue
            
            # 加入該樓層的最後一個工作站
            if current_assignment:
                assignments.append(current_assignment)
                print(f"🔥 DEBUG: 完成樓層{floor}最後工作站 {current_assignment.station_id}")

        # 🆕 最終結果診斷
        print(f"🔥 DEBUG: 最終結果 - {len(assignments)} 個工作站分配")
        
        for i, assignment in enumerate(assignments, 1):
            print(f"🔥 DEBUG: 工作站{i} ({assignment.station_id}): {assignment.total_partcustids}據點, {assignment.total_workload_minutes:.1f}分鐘")
            
            # 🚨 檢查約束違反
            if assignment.total_partcustids > max_partcustids:
                print(f"🔥 DEBUG: ❌❌❌ 約束違反！{assignment.station_id} 據點數超限: {assignment.total_partcustids} > {max_partcustids}")
            
            if assignment.total_workload_minutes > max_time_per_station:
                print(f"🔥 DEBUG: ❌❌❌ 約束違反！{assignment.station_id} 時間超限: {assignment.total_workload_minutes:.1f} > {max_time_per_station:.1f}")
        
        print(f"🔥 DEBUG: _assign_partcustids_to_stations 結束")
        
        # 計算每個工作站的預計完成時間
        for assignment in assignments:
            start_time = current_time + timedelta(minutes=self.params['station_startup_time_minutes'])
            assignment.estimated_completion_time = start_time + timedelta(minutes=assignment.total_workload_minutes)
        
        return assignments




    def _find_next_available_station(self, assigned_stations: set, target_floor: int) -> Optional[str]:
        """🆕 新增：找到下一個可用工作站"""
        # 優先使用目標樓層的工作站
        floor_stations = [
            station for station in self.workstations.values()
            if (station.floor == target_floor and 
                station.station_id not in assigned_stations and
                not station.reserved_for_exception)
        ]
        
        if floor_stations:
            # 優先使用固定工作站
            fixed_stations = [s for s in floor_stations if s.is_fixed and s.status == StationStatus.IDLE]
            if fixed_stations:
                return fixed_stations[0].station_id
            
            # 其次使用忙碌的固定工作站
            busy_fixed = [s for s in floor_stations if s.is_fixed]
            if busy_fixed:
                return busy_fixed[0].station_id
            
            # 最後使用臨時工作站
            return floor_stations[0].station_id
        
        return None
    
    def _find_next_available_station_by_floor(self, assigned_stations: set, target_floor: int) -> Optional[str]:
        """🔧 修復：按順序查找該樓層的可用工作站"""
        
        # 🆕 修復：按工作站編號順序查找
        floor_stations = []
        for station_id, station in self.workstations.items():
            if (station.floor == target_floor and 
                station_id not in assigned_stations and
                not station.reserved_for_exception):
                floor_stations.append((station_id, station))
        
        if not floor_stations:
            return None
        
        # 🚨 關鍵修復：按工作站編號排序，確保按 ST2F01, ST2F02, ST2F03... 順序分配
        floor_stations.sort(key=lambda x: x[0])  # 按 station_id 排序
        
        # 優先使用固定工作站
        for station_id, station in floor_stations:
            if station.is_fixed and station.status == StationStatus.IDLE:
                return station_id
        
        # 其次使用其他固定工作站
        for station_id, station in floor_stations:
            if station.is_fixed:
                return station_id
        
        # 最後使用臨時工作站
        return floor_stations[0][0]

    def _execute_station_assignment(self, assignment: StationAssignment, 
                                staff_schedule: pd.DataFrame, current_time: datetime) -> bool:
        """🔧 修正：改善員工分配邏輯，支援臨時工作站"""
        try:
            station_id = assignment.station_id
            
            # 🔧 修正：改善員工查找邏輯
            staff_id = self._find_available_staff_for_station(station_id, staff_schedule)
            
            if staff_id is None:
                self.logger.warning(f"⚠️ 工作站 {station_id} 找不到可用員工，嘗試分配空閒員工")
                # 🔧 新增：為臨時工作站分配空閒員工
                staff_id = self._assign_idle_staff_to_station(station_id, staff_schedule)
            
            if staff_id is None:
                self.logger.error(f"❌ 工作站 {station_id} 無法分配員工")
                return False
            
            # 記錄工作站分配
            self.partcustid_assignments[station_id] = assignment
            
            # 分配所有任務到工作站
            success_count = 0
            for group in assignment.partcustid_groups:
                for task in group.tasks:
                    success = self._assign_single_task_to_station(task, station_id, staff_id, current_time)
                    if success:
                        success_count += 1
            
            self.logger.info(f"✅ 工作站 {station_id} 分配完成: {success_count} 個任務 (員工: {staff_id})")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"執行工作站分配失敗: {str(e)}")
            return False

    # 3. 新增員工查找方法
    def _find_available_staff_for_station(self, station_id: str, staff_schedule: pd.DataFrame) -> Optional[int]:
        """尋找工作站對應的員工"""
        
        # 方法1：直接匹配工作站ID
        station_staff = staff_schedule[staff_schedule['station_id'] == station_id]
        if len(station_staff) > 0:
            return int(station_staff.iloc[0]['staff_id'])
        
        # 方法2：匹配樓層的固定工作站員工（臨時工作站使用固定工作站員工）
        if station_id.startswith('ST') and 'T' in station_id:  # 臨時工作站
            floor = station_id[2]  # 取得樓層號
            floor_fixed_stations = staff_schedule[
                staff_schedule['station_id'].str.startswith(f'ST{floor}F')
            ]
            
            if len(floor_fixed_stations) > 0:
                # 使用該樓層第一個固定工作站的員工
                return int(floor_fixed_stations.iloc[0]['staff_id'])
        
        return None
    
    def _assign_single_task_to_station(self, task: Task, station_id: str, 
                                      staff_id: int, current_time: datetime) -> bool:
        """🆕 新增：分配單一任務到工作站"""
        try:
            station = self.workstations[station_id]
            
            # 更新任務資訊
            task.assigned_station = station_id
            task.assigned_staff = staff_id
            task.status = TaskStatus.ASSIGNED
            
            # 計算開始時間（如果工作站空閒則需要啟動時間）
            if station.status == StationStatus.IDLE:
                startup_duration = timedelta(minutes=self.params['station_startup_time_minutes'])
                task.start_time = current_time + startup_duration
                station.startup_time = current_time
                station.status = StationStatus.STARTING_UP
            else:
                # 排到現有任務之後
                task.start_time = station.available_time or current_time
            
            # 計算完成時間
            task.estimated_completion = task.start_time + timedelta(minutes=task.estimated_duration)
            
            # 更新工作站狀態
            if not station.current_task:  # 第一個任務
                station.current_task = task
            station.assigned_staff = staff_id
            station.available_time = task.estimated_completion
            
            if station.status == StationStatus.IDLE:
                station.status = StationStatus.STARTING_UP
            elif station.status != StationStatus.STARTING_UP:
                station.status = StationStatus.BUSY
            
            return True
            
        except Exception as e:
            self.logger.error(f"分配任務失敗 {task.task_id} -> {station_id}: {str(e)}")
            return False
    
    
    def _assign_idle_staff_to_station(self, station_id: str, staff_schedule: pd.DataFrame) -> Optional[int]:
        """為工作站分配空閒員工"""
        
        station = self.workstations.get(station_id)
        if not station:
            return None
        
        target_floor = station.floor
        
        # 找到該樓層的所有員工
        floor_staff = staff_schedule[staff_schedule['floor'] == str(target_floor)]
        
        if len(floor_staff) > 0:
            # 簡化邏輯：使用該樓層第一個員工
            staff_id = int(floor_staff.iloc[0]['staff_id'])
            self.logger.info(f"🔄 為工作站 {station_id} 分配樓層 {target_floor} 的員工 {staff_id}")
            return staff_id
        
        return None


    
    def _assign_other_stage_tasks(self, stage_tasks: List[Task], staff_schedule: pd.DataFrame,
                                 current_time: datetime, assigned_stations: set) -> Dict:
        """🆕 新增：分配其他階段任務（使用原邏輯）"""
        stage_result = {
            'assigned': [],
            'unassigned': [],
            'errors': [],
            'overtime_required': [],
            'assigned_stations': set()
        }
        
        # 在階段內按優先權排序
        stage_tasks_sorted = self._sort_tasks_within_stage(stage_tasks)
        
        for task in stage_tasks_sorted:
            try:
                # 找到適合的工作站（排除已分配的）
                suitable_station = self._find_suitable_station_excluding_assigned(
                    task, staff_schedule, current_time, assigned_stations
                )
                
                if suitable_station:
                    # 分配任務
                    success = self._assign_task_to_station(task, suitable_station, staff_schedule, current_time)
                    if success:
                        stage_result['assigned'].append(task.task_id)
                        stage_result['assigned_stations'].add(suitable_station)
                        assigned_stations.add(suitable_station)
                    else:
                        stage_result['unassigned'].append(task.task_id)
                else:
                    stage_result['unassigned'].append(task.task_id)
                    
                    # 檢查是否需要加班
                    if self._task_requires_overtime(task, current_time):
                        stage_result['overtime_required'].append(task.task_id)
                        
            except Exception as e:
                self.logger.error(f"分配任務 {task.task_id} 時發生錯誤: {str(e)}")
                stage_result['errors'].append(task.task_id)
        
        return stage_result
    
    def calculate_estimated_duration_fixed(self, task: Task) -> float:
        """🆕 新增：計算固定預估時間（波次分配前使用，無隨機性）"""
        
        if task.task_type == TaskType.RECEIVING:
            # 進貨任務：純零件數量計算
            time_per_piece = self.params['receiving_time_per_piece']
            return task.quantity * time_per_piece
        else:
            # 出貨任務：只考慮repack
            if task.requires_repack:
                base_time = self.params['picking_base_time_repack']
                additional_time = self.params['repack_additional_time']
            else:
                base_time = self.params['picking_base_time_no_repack']
                additional_time = 0

            total_time = base_time + additional_time
            
            # 如果零件有特定時間設定，優先使用（取平均值，無隨機）
            item_info = self._get_item_info(task.frcd, task.partno)
            if item_info:
                if task.requires_repack:
                    item_base_time = self._safe_float_conversion(
                        item_info.get('picktime_repack_mean'), 
                        self.raw_params['picking_base_time_repack_seconds']
                    ) / 60.0
                else:
                    item_base_time = self._safe_float_conversion(
                        item_info.get('picktime_norepack_mea'), 
                        self.raw_params['picking_base_time_no_repack_seconds']
                    ) / 60.0
                
                total_time = item_base_time + (additional_time if task.requires_repack else 0)
            
            # 確保在合理範圍內（無隨機變動）
            total_time = max(self.params['min_task_duration'], 
                            min(self.params['max_task_duration'], total_time))
            
            return round(total_time, 2)

    def calculate_actual_duration_with_randomness(self, task: Task, staff_skill_info: Optional[Dict] = None) -> float:
        """🔧 修改：計算實際執行時間（包含隨機性和員工差異）"""
        
        # 從固定時間開始
        base_time = task.estimated_duration  # 使用之前計算的固定時間
        
        # 考慮員工技能影響
        if staff_skill_info:
            capacity_multiplier = staff_skill_info.get('capacity_multiplier', 1.0)
            skill_level = staff_skill_info.get('skill_level', 3)
            
            skill_factor = 1.0 - (skill_level - 3) * self.params['skill_impact_multiplier']
            skill_factor = max(0.5, min(1.5, skill_factor))
            
            base_time = base_time * skill_factor * (1.0 / capacity_multiplier)
        
        # 加入隨機變動（±15%）
        variation = base_time * 0.15
        actual_time = base_time + random.uniform(-variation, variation)
        
        # 確保在合理範圍內
        actual_time = max(self.params['min_task_duration'], 
                        min(self.params['max_task_duration'], actual_time))
        
        return round(actual_time, 2)

    # 🔧 修改原有方法：保持向後相容性
    def calculate_task_duration(self, task: Task, staff_skill_info: Optional[Dict] = None) -> float:
        """🔧 修改：向後相容的任務時間計算（預設使用固定計算）"""
        if staff_skill_info is None:
            # 無員工資訊時使用固定計算
            return self.calculate_estimated_duration_fixed(task)
        else:
            # 有員工資訊時使用隨機計算
            return self.calculate_actual_duration_with_randomness(task, staff_skill_info)
    
    def _sort_tasks_within_stage(self, tasks: List[Task]) -> List[Task]:
        """🆕 新增：在同一階段內按優先權排序任務"""
        
        def stage_task_key(task: Task) -> tuple:
            # 在同階段內，按優先權 → 樓層 → 數量排序
            priority_order = {'P1': 1, 'P2': 2, 'P3': 3, 'P4': 4}
            priority_value = priority_order.get(task.priority_level, 5)
            
            return (priority_value, task.floor, -task.quantity)
        
        return sorted(tasks, key=stage_task_key)

    def _find_suitable_station_excluding_assigned(self, task: Task, staff_schedule: pd.DataFrame, 
                                                current_time: datetime, assigned_stations: set) -> Optional[str]:
        """🆕 新增：找到適合的工作站（排除已分配的工作站）"""
        
        # 篩選該樓層的工作站（排除已分配和異常預留的）
        floor_stations = [
            station for station in self.workstations.values()
            if (station.floor == task.floor and 
                not station.reserved_for_exception and
                station.station_id not in assigned_stations)
        ]
        
        if not floor_stations:
            return None
        
        # 🟢 優先使用空閒工作站
        idle_stations = [s for s in floor_stations if s.status.value == 'IDLE']
        if idle_stations:
            # 優先使用固定工作站
            fixed_idle = [s for s in idle_stations if s.is_fixed]
            if fixed_idle:
                return fixed_idle[0].station_id
            else:
                return idle_stations[0].station_id
        
        # 🟡 沒有空閒工作站，找最早可用的忙碌工作站
        available_stations = []
        for station in floor_stations:
            if station.status.value == 'BUSY' and station.available_time:
                available_stations.append((station, station.available_time))
        
        if available_stations:
            # 按可用時間排序
            available_stations.sort(key=lambda x: x[1])
            return available_stations[0][0].station_id
        
        return None
    
    def _assign_task_to_station(self, task: Task, station_id: str, 
                               staff_schedule: pd.DataFrame, current_time: datetime) -> bool:
        """將任務分配到指定工作站"""
        try:
            station = self.workstations[station_id]
            
            # 找到分配給該工作站的員工
            station_staff = staff_schedule[
                staff_schedule['station_id'] == station_id
            ]
            
            if len(station_staff) == 0:
                self.logger.warning(f"工作站 {station_id} 沒有分配員工")
                return False
            
            staff_info = station_staff.iloc[0]
            staff_id = int(staff_info['staff_id'])
            
            # 取得員工技能資訊
            staff_skill_info = self._get_staff_skill_info(staff_id)
            
            # 重新計算精確的執行時間
            precise_duration = self.calculate_task_duration(task, staff_skill_info)
            task.estimated_duration = precise_duration
            
            # 更新任務資訊
            task.assigned_station = station_id
            task.assigned_staff = staff_id
            task.status = TaskStatus.ASSIGNED
            
            # 計算開始時間
            if station.status == StationStatus.IDLE:
                # 工作站需要啟動
                startup_duration = timedelta(minutes=self.params['station_startup_time_minutes'])
                task.start_time = current_time + startup_duration
                station.startup_time = current_time
                station.status = StationStatus.STARTING_UP
            else:
                # 工作站已在使用，排到可用時間
                task.start_time = station.available_time or current_time
            
            # 計算完成時間
            task.estimated_completion = task.start_time + timedelta(minutes=task.estimated_duration)
            
            # 更新工作站狀態
            station.current_task = task
            station.assigned_staff = staff_id
            station.available_time = task.estimated_completion
            
            if station.status == StationStatus.IDLE:
                station.status = StationStatus.STARTING_UP
            elif station.status != StationStatus.STARTING_UP:
                station.status = StationStatus.BUSY
            
            task_type_str = "進貨" if task.task_type == TaskType.RECEIVING else "出貨"
            self.logger.debug(f"✅ {task_type_str}任務 {task.task_id} 分配到工作站 {station_id} (員工: {staff_id})")
            
            return True
            
        except Exception as e:
            self.logger.error(f"分配任務失敗 {task.task_id} -> {station_id}: {str(e)}")
            return False
    
    # === 保留所有其他原有方法 ===
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """解析日期字串"""
        if pd.isna(date_str) or date_str == '':
            return None
        
        try:
            date_str = str(date_str).strip()
            
            if '-' in date_str:
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            elif len(date_str) == 8:
                return datetime.strptime(date_str, '%Y%m%d').date()
            else:
                return datetime.strptime(date_str, '%Y/%m/%d').date()
                
        except (ValueError, TypeError):
            self.logger.warning(f"日期格式錯誤: '{date_str}'")
            return None

    def get_tasks_requiring_overtime(self, current_time: datetime) -> List[Task]:
        """🆕 新增：取得需要加班的任務"""
        requiring_overtime = []
        
        for task in self.tasks.values():
            if task.status not in [TaskStatus.PENDING, TaskStatus.ASSIGNED, TaskStatus.IN_PROGRESS]:
                continue
            
            needs_overtime = False
            overtime_reason = ""
            
            if task.task_type == TaskType.SHIPPING:
                # 出貨任務：副倉庫必須當天完成
                if task.route_code in ['SDTC', 'SDHN']:
                    # 檢查是否接近下班時間且未完成
                    if self._is_near_end_of_day(current_time) and task.status != TaskStatus.COMPLETED:
                        needs_overtime = True
                        overtime_reason = "副倉庫出貨必須當天完成"
                        
            elif task.task_type == TaskType.RECEIVING:
                # 進貨任務：檢查是否已經第3天
                if task.deadline_date and current_time.date() >= task.deadline_date:
                    if task.status != TaskStatus.COMPLETED:
                        needs_overtime = True
                        overtime_reason = f"進貨已到期限（第{self.params['receiving_completion_days']}天）"
            
            if needs_overtime:
                # 添加加班原因到任務metadata
                if not hasattr(task, 'overtime_reason'):
                    task.overtime_reason = overtime_reason
                requiring_overtime.append(task)
        
        return requiring_overtime

    def _is_near_end_of_day(self, current_time: datetime, threshold_hours: float = 2.0) -> bool:
        """檢查是否接近下班時間"""
        # 假設下班時間是17:30
        end_of_day = current_time.replace(hour=17, minute=30, second=0, microsecond=0)
        threshold_time = end_of_day - timedelta(hours=threshold_hours)
        
        return current_time >= threshold_time

    def create_overtime_tasks(self, overtime_requirements: Dict[str, Dict]) -> List[Task]:
        """🆕 新增：創建加班任務"""
        overtime_tasks = []
        
        for station_id, requirement in overtime_requirements.items():
            task_id = requirement.get('task_id')
            if task_id and task_id in self.tasks:
                original_task = self.tasks[task_id]
                
                # 創建加班任務（副本）
                overtime_task = Task(
                    task_id=f"{task_id}_OT",
                    order_id=original_task.order_id,
                    frcd=original_task.frcd,
                    partno=original_task.partno,
                    quantity=original_task.quantity,
                    floor=original_task.floor,
                    priority_level='P1',  # 加班任務優先權最高
                    requires_repack=original_task.requires_repack,
                    estimated_duration=requirement.get('required_hours', 1.0) * 60,  # 轉為分鐘
                    task_type=original_task.task_type,
                    assigned_station=station_id,
                    partcustid=original_task.partcustid,
                    route_code=original_task.route_code,
                    route_group=original_task.route_group
                )
                
                # 複製進貨相關屬性
                if original_task.task_type == TaskType.RECEIVING:
                    overtime_task.arrival_date = original_task.arrival_date
                    overtime_task.deadline_date = original_task.deadline_date
                    overtime_task.days_since_arrival = original_task.days_since_arrival
                    overtime_task.is_overdue = True  # 需要加班的都算逾期
                
                self.overtime_tasks[overtime_task.task_id] = overtime_task
                overtime_tasks.append(overtime_task)
                
                # 標記原任務為已處理
                original_task.status = TaskStatus.CANCELLED
                
                self.logger.info(f"🕒 創建加班任務: {overtime_task.task_id} (原因: {requirement.get('reason', 'unknown')})")
        
        return overtime_tasks

    def get_tasks_by_type(self, task_type: TaskType) -> List[Task]:
        """🆕 新增：依任務類型取得任務"""
        return [task for task in self.tasks.values() if task.task_type == task_type]

    def get_overdue_receiving_tasks(self, current_date: date) -> List[Task]:
        """🆕 新增：取得逾期的進貨任務"""
        overdue_tasks = []
        
        for task in self.tasks.values():
            if (task.task_type == TaskType.RECEIVING and 
                task.deadline_date and 
                current_date > task.deadline_date and
                task.status != TaskStatus.COMPLETED):
                overdue_tasks.append(task)
        
        # 按逾期天數排序（最緊急的在前）
        overdue_tasks.sort(key=lambda t: (current_date - t.deadline_date).days, reverse=True)
        
        return overdue_tasks

    def get_due_today_tasks(self, current_date: date) -> List[Task]:
        """🆕 新增：取得今天截止的任務"""
        due_today = []
        
        for task in self.tasks.values():
            if (task.task_type == TaskType.RECEIVING and 
                task.deadline_date == current_date and
                task.status != TaskStatus.COMPLETED):
                due_today.append(task)
        
        # 按優先權排序
        priority_order = {'P1': 1, 'P2': 2, 'P3': 3, 'P4': 4}
        due_today.sort(key=lambda t: priority_order.get(t.priority_level, 5))
        
        return due_today

    def _task_requires_overtime(self, task: Task, current_time: datetime) -> bool:
        """🆕 檢查任務是否需要加班"""
        if task.task_type == TaskType.RECEIVING:
            # 進貨：已到期限
            return task.deadline_date and current_time.date() >= task.deadline_date
        elif task.task_type == TaskType.SHIPPING:
            # 出貨：副倉庫且接近下班
            return (task.route_code in ['SDTC', 'SDHN'] and 
                    self._is_near_end_of_day(current_time))
        
        return False

    def complete_task(self, task_id: str, current_time: datetime) -> bool:
        """完成任務"""
        if task_id not in self.tasks:
            self.logger.error(f"任務 {task_id} 不存在")
            return False
        
        task = self.tasks[task_id]
        
        if task.status != TaskStatus.IN_PROGRESS:
            self.logger.warning(f"任務 {task_id} 不在執行中狀態")
            return False
        
        # 更新任務狀態
        task.status = TaskStatus.COMPLETED
        task.actual_completion = current_time
        
        # 更新工作站狀態
        if task.assigned_station:
            station = self.workstations[task.assigned_station]
            station.current_task = None
            station.status = StationStatus.IDLE
            station.available_time = current_time
        
        task_type_str = "進貨" if task.task_type == TaskType.RECEIVING else "出貨"
        self.logger.info(f"✅ {task_type_str}任務 {task_id} 完成")
        return True

    def get_task_summary_by_type(self) -> Dict:
        """🆕 新增：按任務類型取得摘要"""
        summary = {
            'shipping_tasks': {'total': 0, 'pending': 0, 'in_progress': 0, 'completed': 0},
            'receiving_tasks': {'total': 0, 'pending': 0, 'in_progress': 0, 'completed': 0, 'overdue': 0},
            'overtime_tasks': len(self.overtime_tasks),
            'total_tasks': len(self.tasks),
            'partcustid_assignments': len(self.partcustid_assignments)  # 🆕 新增據點分配統計
        }
        
        for task in self.tasks.values():
            if task.task_type == TaskType.SHIPPING:
                summary['shipping_tasks']['total'] += 1
                if task.status == TaskStatus.PENDING:
                    summary['shipping_tasks']['pending'] += 1
                elif task.status == TaskStatus.IN_PROGRESS:
                    summary['shipping_tasks']['in_progress'] += 1
                elif task.status == TaskStatus.COMPLETED:
                    summary['shipping_tasks']['completed'] += 1
                    
            elif task.task_type == TaskType.RECEIVING:
                summary['receiving_tasks']['total'] += 1
                if task.status == TaskStatus.PENDING:
                    summary['receiving_tasks']['pending'] += 1
                elif task.status == TaskStatus.IN_PROGRESS:
                    summary['receiving_tasks']['in_progress'] += 1
                elif task.status == TaskStatus.COMPLETED:
                    summary['receiving_tasks']['completed'] += 1
                
                if task.is_overdue:
                    summary['receiving_tasks']['overdue'] += 1
        
        return summary

    def get_workstation_summary(self, current_time: datetime) -> Dict:
        """取得工作站狀態摘要"""
        summary = {
            'total_stations': len(self.workstations),
            'status_distribution': {},
            'floor_distribution': {},
            'utilization_stats': {},
            'task_type_distribution': {},  # 🆕 新增：任務類型分布
            'partcustid_distribution': {}   # 🆕 新增：據點分布
        }
        
        # 統計狀態分布
        task_types = {'SHIPPING': 0, 'RECEIVING': 0, 'NONE': 0}
        partcustid_count = 0
        
        for station in self.workstations.values():
            status = station.status.value
            summary['status_distribution'][status] = summary['status_distribution'].get(status, 0) + 1
            
            floor = station.floor
            summary['floor_distribution'][floor] = summary['floor_distribution'].get(floor, 0) + 1
            
            # 統計任務類型
            if station.current_task:
                task_type = station.current_task.task_type.value
                task_types[task_type] += 1
            else:
                task_types['NONE'] += 1
            
            # 統計據點分配
            if station.station_id in self.partcustid_assignments:
                assignment = self.partcustid_assignments[station.station_id]
                partcustid_count += assignment.total_partcustids
        
        summary['task_type_distribution'] = task_types
        summary['partcustid_distribution'] = {
            'total_assigned_partcustids': partcustid_count,
            'stations_with_partcustids': len(self.partcustid_assignments)
        }
        
        # 計算利用率
        busy_stations = sum(1 for s in self.workstations.values() 
                           if s.status in [StationStatus.BUSY, StationStatus.STARTING_UP])
        
        summary['utilization_stats'] = {
            'busy_stations': busy_stations,
            'idle_stations': len(self.workstations) - busy_stations,
            'utilization_rate': round(busy_stations / len(self.workstations) * 100, 1) if self.workstations else 0
        }
        
        return summary

    def monitor_station_progress(self, station_id: str, current_time: datetime) -> Dict:
        """監控工作站進度"""
        if station_id not in self.workstations:
            return {'error': f'工作站 {station_id} 不存在'}
        
        station = self.workstations[station_id]
        
        progress_info = {
            'station_id': station_id,
            'status': station.status.value,
            'floor': station.floor,
            'is_fixed': station.is_fixed,
            'assigned_staff': station.assigned_staff,
            'current_task': None,
            'startup_info': None,
            'availability': None,
            'partcustid_assignment': None  # 🆕 新增據點分配資訊
        }
        
        # 🆕 據點分配資訊
        if station_id in self.partcustid_assignments:
            assignment = self.partcustid_assignments[station_id]
            progress_info['partcustid_assignment'] = {
                'total_partcustids': assignment.total_partcustids,
                'total_workload_minutes': assignment.total_workload_minutes,
                'estimated_completion': assignment.estimated_completion_time,
                'partcustid_list': [group.partcustid for group in assignment.partcustid_groups]
            }
        
        # 當前任務資訊
        if station.current_task:
            task = station.current_task
            task_progress = {
                'task_id': task.task_id,
                'task_type': task.task_type.value,
                'priority': task.priority_level,
                'start_time': task.start_time,
                'estimated_completion': task.estimated_completion,
                'estimated_duration': task.estimated_duration,
                'item_info': f"{task.frcd}-{task.partno} ({task.quantity}件)",
                'partcustid': task.partcustid  # 🆕 新增據點資訊
            }
            
            # 計算進度百分比
            if task.start_time and task.estimated_completion and task.status == TaskStatus.IN_PROGRESS:
                total_duration = (task.estimated_completion - task.start_time).total_seconds()
                elapsed_duration = (current_time - task.start_time).total_seconds()
                
                if total_duration > 0:
                    progress_percent = min(100, (elapsed_duration / total_duration) * 100)
                    task_progress['progress_percent'] = round(progress_percent, 1)
                    
                    remaining_seconds = max(0, total_duration - elapsed_duration)
                    task_progress['remaining_minutes'] = round(remaining_seconds / 60, 1)
                else:
                    task_progress['progress_percent'] = 100
                    task_progress['remaining_minutes'] = 0
            else:
                task_progress['progress_percent'] = 0
                task_progress['remaining_minutes'] = task.estimated_duration
            
            progress_info['current_task'] = task_progress
        
        # 啟動資訊
        if station.status == StationStatus.STARTING_UP and station.startup_time:
            startup_duration = self.params['station_startup_time_minutes'] * 60  # 轉為秒
            elapsed_startup = (current_time - station.startup_time).total_seconds()
            remaining_startup = max(0, startup_duration - elapsed_startup)
            
            progress_info['startup_info'] = {
                'startup_progress_percent': min(100, (elapsed_startup / startup_duration) * 100),
                'remaining_startup_seconds': round(remaining_startup)
            }
        
        # 可用性資訊
        if station.available_time:
            if current_time < station.available_time:
                wait_seconds = (station.available_time - current_time).total_seconds()
                progress_info['availability'] = {
                    'status': 'busy',
                    'available_at': station.available_time,
                    'wait_minutes': round(wait_seconds / 60, 1)
                }
            else:
                progress_info['availability'] = {
                    'status': 'available_now'
                }
        else:
            progress_info['availability'] = {
                'status': 'available_now' if station.status == StationStatus.IDLE else 'unknown'
            }
        
        return progress_info

    def _get_item_info(self, frcd: str, partno: str) -> Optional[Dict]:
        """取得零件資訊"""
        if self.item_master is None:
            return None
        
        item_row = self.item_master[
            (self.item_master['frcd'] == frcd) & 
            (self.item_master['partno'] == partno)
        ]
        
        if len(item_row) == 0:
            return None
        
        return item_row.iloc[0].to_dict()

    def _get_staff_skill_info(self, staff_id: int) -> Optional[Dict]:
        """取得員工技能資訊"""
        if self.staff_master is None:
            return None
        
        staff_row = self.staff_master[self.staff_master['staff_id'] == staff_id]
        
        if len(staff_row) == 0:
            return None
        
        staff_info = staff_row.iloc[0].to_dict()
        
        # 處理capacity_multiplier格式
        try:
            staff_info['capacity_multiplier'] = float(staff_info['capacity_multiplier'])
        except (ValueError, TypeError):
            staff_info['capacity_multiplier'] = 1.0
        
        return staff_info

    def _safe_float_conversion(self, value, default: float) -> float:
        """安全的浮點數轉換"""
        try:
            if pd.isna(value) or value == '':
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    # === 保留所有異常處理相關方法 ===
    
    def reserve_station_for_exception(self, station_id: str, exception_task) -> Dict:
        """為異常處理預留工作站"""
        if station_id not in self.workstations:
            return {'success': False, 'error': f'工作站 {station_id} 不存在'}
        
        station = self.workstations[station_id]
        
        if station.status != StationStatus.IDLE:
            return {'success': False, 'error': f'工作站 {station_id} 不是空閒狀態'}
        
        station.reserved_for_exception = True
        station.status = StationStatus.RESERVED
        
        self.logger.info(f"🛡️ 工作站 {station_id} 已預留給異常處理")
        
        return {
            'success': True,
            'message': f'工作站 {station_id} 已預留',
            'reserved_station': station_id
        }

    def interrupt_current_task(self, station_id: str, interruption_reason: str) -> Dict:
        """中斷當前任務（僅限異常處理）"""
        if not self.params['task_interruption_allowed'] == 'Y':
            return {'success': False, 'error': '系統不允許任務中斷'}
        
        if station_id not in self.workstations:
            return {'success': False, 'error': f'工作站 {station_id} 不存在'}
        
        station = self.workstations[station_id]
        
        if not station.current_task or station.current_task.status != TaskStatus.IN_PROGRESS:
            return {'success': False, 'error': f'工作站 {station_id} 沒有執行中的任務'}
        
        # 暫停任務
        task = station.current_task
        task.status = TaskStatus.PAUSED
        station.status = StationStatus.RESERVED
        station.reserved_for_exception = True
        
        task_type_str = "進貨" if task.task_type == TaskType.RECEIVING else "出貨"
        self.logger.warning(f"⚠️ 工作站 {station_id} {task_type_str}任務 {task.task_id} 被中斷: {interruption_reason}")
        
        return {
            'success': True,
            'interrupted_task': task.task_id,
            'reason': interruption_reason,
            'station_status': StationStatus.RESERVED
        }

    def resume_interrupted_task(self, station_id: str, interrupted_task_id: str, 
                               current_time: datetime) -> Dict:
        """恢復被中斷的任務"""
        if station_id not in self.workstations:
            return {'success': False, 'error': f'工作站 {station_id} 不存在'}
        
        if interrupted_task_id not in self.tasks:
            return {'success': False, 'error': f'任務 {interrupted_task_id} 不存在'}
        
        station = self.workstations[station_id]
        task = self.tasks[interrupted_task_id]
        
        if task.status != TaskStatus.PAUSED:
            return {'success': False, 'error': f'任務 {interrupted_task_id} 不在暫停狀態'}
        
        # 恢復任務
        task.status = TaskStatus.IN_PROGRESS
        station.current_task = task
        station.status = StationStatus.BUSY
        station.reserved_for_exception = False
        
        # 重新計算完成時間（加入中斷時間補償）
        remaining_duration = task.estimated_duration * 0.5  # 假設完成了一半
        task.estimated_completion = current_time + timedelta(minutes=remaining_duration)
        station.available_time = task.estimated_completion
        
        task_type_str = "進貨" if task.task_type == TaskType.RECEIVING else "出貨"
        self.logger.info(f"▶️ {task_type_str}任務 {interrupted_task_id} 在工作站 {station_id} 恢復執行")
        
        return {
            'success': True,
            'resumed_task': interrupted_task_id,
            'new_completion_time': task.estimated_completion,
            'station_status': StationStatus.BUSY
        }
    
    def _enforce_workload_distribution(self, assignments: List[StationAssignment], 
                                    max_time_per_station: float) -> List[StationAssignment]:
        """🆕 新增：強制執行工作負載分散"""
        
        redistributed = []
        
        for assignment in assignments:
            if assignment.total_workload_minutes > max_time_per_station:
                self.logger.warning(f"🚨 工作站 {assignment.station_id} 負載過重 ({assignment.total_workload_minutes:.1f} > {max_time_per_station})")
                
                # 嘗試拆分負載
                split_assignments = self._split_overloaded_assignment(assignment, max_time_per_station)
                redistributed.extend(split_assignments)
            else:
                redistributed.append(assignment)
        
        return redistributed
    
    def _prioritize_receiving_over_subwarehouse(self, tasks: List[Task], available_gap_time: float) -> List[Task]:
        """空檔少時進貨優先於副倉庫"""
        receiving_tasks = [task for task in tasks if task.task_type == TaskType.RECEIVING]
        subwarehouse_tasks = [task for task in tasks if task.task_type == TaskType.SHIPPING and task.priority_level == 'P3']
        
        # 如果可用空檔時間 < 60分鐘，進貨優先
        if available_gap_time < 60:
            return receiving_tasks + subwarehouse_tasks
        else:
            return subwarehouse_tasks + receiving_tasks
        
    def _assign_p1_wave_tasks(self, p1_tasks: List[Task], staff_schedule: pd.DataFrame, 
                            current_time: datetime) -> Dict:
        """分配P1一般訂單波次任務（最高優先權）"""
        self.logger.info(f"🎯 分配P1一般訂單波次任務: {len(p1_tasks)} 個")
        
        result = {
            'assigned': [],
            'unassigned': [],
            'errors': [],
            'overtime_required': [],
            'used_stations': set(),
            'analysis': {}
        }
        
        if not p1_tasks:
            return result
        
        # 按波次分組P1任務
        wave_groups = defaultdict(list)
        for task in p1_tasks:
            wave_id = self._determine_task_wave_id(task, current_time)
            wave_groups[wave_id].append(task)
        
        # 逐波次處理P1任務
        for wave_id, wave_tasks in wave_groups.items():
            self.logger.info(f"  處理波次 {wave_id}: {len(wave_tasks)} 個P1任務")
            
            # 🔧 使用樓層固定時間檢查
            wave_feasibility = self._check_p1_wave_feasibility(wave_tasks, current_time)
            result['analysis'][wave_id] = wave_feasibility
            
            if not wave_feasibility['feasible']:
                self.logger.warning(f"⚠️ 波次 {wave_id} 不可行: {wave_feasibility.get('reason', 'unknown')}")
                result['unassigned'].extend([task.task_id for task in wave_tasks])
                continue
            
            # 按據點分組並分配到工作站
            partcustid_groups = self._group_tasks_by_partcustid(wave_tasks)
            station_assignments = self._assign_partcustids_to_stations_with_fixed_time(
                partcustid_groups, current_time, result['used_stations']
            )
            
            # 執行分配
            for assignment in station_assignments:
                try:
                    success = self._execute_station_assignment(assignment, staff_schedule, current_time)
                    if success:
                        for group in assignment.partcustid_groups:
                            for task in group.tasks:
                                result['assigned'].append(task.task_id)
                        result['used_stations'].add(assignment.station_id)
                    else:
                        for group in assignment.partcustid_groups:
                            for task in group.tasks:
                                result['unassigned'].append(task.task_id)
                except Exception as e:
                    self.logger.error(f"P1分配錯誤: {str(e)}")
                    for group in assignment.partcustid_groups:
                        for task in group.tasks:
                            result['errors'].append(task.task_id)
        
        self.logger.info(f"✅ P1分配完成: 已分配 {len(result['assigned'])}, 使用工作站 {len(result['used_stations'])} 個")
        return result

    def _assign_p2_gap_tasks(self, p2_tasks: List[Task], staff_schedule: pd.DataFrame,
                            current_time: datetime, used_stations: set) -> Dict:
        """分配P2緊急訂單到空檔工作站"""
        self.logger.info(f"🚨 分配P2緊急訂單到空檔: {len(p2_tasks)} 個")
        
        result = {
            'assigned': [],
            'unassigned': [],
            'errors': [],
            'overtime_required': [],
            'used_stations': set(),
            'analysis': {}
        }
        
        if not p2_tasks:
            return result
        
        # 找到空檔工作站
        available_gap_stations = self._get_available_gap_stations(current_time, used_stations)
        self.logger.info(f"  可用空檔工作站: {len(available_gap_stations)} 個")
        
        if not available_gap_stations:
            self.logger.warning("⚠️ 沒有可用空檔工作站，P2任務全部未分配")
            result['unassigned'] = [task.task_id for task in p2_tasks]
            return result
        
        # 按優先權和樓層排序P2任務
        p2_tasks_sorted = sorted(p2_tasks, key=lambda t: (t.floor, -t.quantity))
        
        # 逐個分配到空檔工作站
        for task in p2_tasks_sorted:
            # 找該樓層的空檔工作站
            floor_gap_stations = [
                station_id for station_id in available_gap_stations 
                if self.workstations[station_id].floor == task.floor and 
                station_id not in result['used_stations']
            ]
            
            if floor_gap_stations:
                station_id = floor_gap_stations[0]  # 取第一個可用的
                
                # 分配任務
                success = self._assign_single_task_to_station(task, station_id, 
                                                            self._get_station_staff(station_id, staff_schedule), 
                                                            current_time)
                if success:
                    result['assigned'].append(task.task_id)
                    result['used_stations'].add(station_id)
                    self.logger.info(f"  P2任務 {task.task_id} 分配到空檔工作站 {station_id}")
                else:
                    result['unassigned'].append(task.task_id)
            else:
                result['unassigned'].append(task.task_id)
        
        self.logger.info(f"✅ P2分配完成: 已分配 {len(result['assigned'])}, 未分配 {len(result['unassigned'])}")
        return result

    def _assign_p3_and_receiving_gap_tasks(self, p3_and_receiving_tasks: List[Task], 
                                        staff_schedule: pd.DataFrame,
                                        current_time: datetime, used_stations: set) -> Dict:
        """分配P3副倉庫和進貨任務到剩餘空檔"""
        self.logger.info(f"📦 分配P3副倉庫和進貨到剩餘空檔: {len(p3_and_receiving_tasks)} 個")
        
        result = {
            'assigned': [],
            'unassigned': [],
            'errors': [],
            'overtime_required': [],
            'used_stations': set(),
            'analysis': {}
        }
        
        if not p3_and_receiving_tasks:
            return result
        
        # 找到剩餘空檔工作站
        remaining_gap_stations = self._get_available_gap_stations(current_time, used_stations)
        
        # 計算剩餘空檔時間
        total_gap_time = self._calculate_total_gap_time(remaining_gap_stations, current_time)
        
        # 🔧 空檔少時進貨優先
        prioritized_tasks = self._prioritize_receiving_over_subwarehouse(p3_and_receiving_tasks, total_gap_time)
        
        # 逐個分配
        for task in prioritized_tasks:
            # 找適合的空檔工作站
            suitable_stations = [
                station_id for station_id in remaining_gap_stations
                if (self.workstations[station_id].floor == task.floor and 
                    station_id not in result['used_stations'])
            ]
            
            if suitable_stations:
                station_id = suitable_stations[0]
                
                success = self._assign_single_task_to_station(task, station_id,
                                                            self._get_station_staff(station_id, staff_schedule),
                                                            current_time)
                if success:
                    result['assigned'].append(task.task_id)
                    result['used_stations'].add(station_id)
                    task_type_str = "進貨" if task.task_type.value == 'RECEIVING' else "副倉庫"
                    self.logger.info(f"  {task_type_str}任務 {task.task_id} 分配到空檔工作站 {station_id}")
                else:
                    result['unassigned'].append(task.task_id)
            else:
                result['unassigned'].append(task.task_id)
        
        self.logger.info(f"✅ P3+進貨分配完成: 已分配 {len(result['assigned'])}, 未分配 {len(result['unassigned'])}")
        return result

    def _check_p1_wave_feasibility(self, wave_tasks: List[Task], current_time: datetime) -> Dict:
        """檢查P1波次可行性（使用樓層固定時間）"""
        
        if not wave_tasks:
            return {'feasible': True, 'reason': 'no tasks'}
        
        # 按樓層分組統計
        floor_stats = defaultdict(lambda: {'task_count': 0, 'total_time': 0, 'partcustids': set()})
        
        for task in wave_tasks:
            floor = task.floor
            floor_stats[floor]['task_count'] += 1
            floor_stats[floor]['total_time'] += task.estimated_duration
            if task.partcustid:
                floor_stats[floor]['partcustids'].add(task.partcustid)
        
        # 檢查每個樓層的可行性
        feasibility_issues = []
        
        for floor, stats in floor_stats.items():
            # 取得該樓層的固定時間
            if floor == 3:
                available_time = 30  # 3樓30分鐘
            elif floor == 2:
                available_time = 25  # 2樓25分鐘
            else:
                available_time = 30  # 其他樓層預設30分鐘
            
            # 檢查時間約束
            total_workload = stats['total_time']
            partcustid_count = len(stats['partcustids'])
            
            # 計算所需工作站數（基於據點約束）
            max_partcustids_per_station = self.params['max_partcustids_per_station']
            stations_needed_by_partcustids = max(1, -(-partcustid_count // max_partcustids_per_station))  # 向上取整
            
            # 計算所需工作站數（基於時間約束）
            stations_needed_by_time = max(1, -(-int(total_workload) // available_time))  # 向上取整
            
            required_stations = max(stations_needed_by_partcustids, stations_needed_by_time)
            
            # 檢查該樓層可用工作站數
            floor_stations = [s for s in self.workstations.values() if s.floor == floor]
            max_floor_stations = len(floor_stations)
            
            if required_stations > max_floor_stations:
                feasibility_issues.append(f"樓層{floor}需要{required_stations}個工作站，但只有{max_floor_stations}個")
        
        feasible = len(feasibility_issues) == 0
        
        return {
            'feasible': feasible,
            'reason': '; '.join(feasibility_issues) if feasibility_issues else 'feasible',
            'floor_analysis': dict(floor_stats)
        }

    def _assign_partcustids_to_stations_with_fixed_time(self, partcustid_groups: List, 
                                                    current_time: datetime, 
                                                    assigned_stations: set) -> List:
        """使用樓層固定時間的據點分配邏輯"""
        
        assignments = []
        
        # 按樓層分組處理
        floor_groups = defaultdict(list)
        for group in partcustid_groups:
            if group.tasks:
                floor = group.tasks[0].floor
                floor_groups[floor].append(group)
        
        for floor, floor_partcustid_groups in floor_groups.items():
            # 取得該樓層的固定時間約束
            if floor == 3:
                max_time_per_station = 30  # 3樓30分鐘
            elif floor == 2:
                max_time_per_station = 25  # 2樓25分鐘
            else:
                max_time_per_station = 30  # 其他樓層預設30分鐘
            
            max_partcustids = self.params['max_partcustids_per_station']
            
            print(f"🔥 DEBUG: 樓層{floor} 固定時間約束: {max_time_per_station}分鐘")
            
            current_assignment = None
            
            # 按工作量排序（大的據點優先分配）
            floor_partcustid_groups.sort(key=lambda g: g.total_workload_minutes, reverse=True)
            
            for i, partcustid_group in enumerate(floor_partcustid_groups):
                print(f"🔥 DEBUG: 處理據點 {i+1}/{len(floor_partcustid_groups)}: {partcustid_group.partcustid}")
                print(f"🔥 DEBUG: 據點工作負載: {partcustid_group.total_workload_minutes:.1f}分鐘")

                can_fit_current = False
                
                if current_assignment is not None:
                    new_partcustid_count = current_assignment.total_partcustids + 1
                    new_total_time = current_assignment.total_workload_minutes + partcustid_group.total_workload_minutes
                    
                    # 檢查約束條件
                    partcustid_ok = new_partcustid_count <= max_partcustids
                    time_ok = new_total_time <= max_time_per_station  # 🔧 使用樓層固定時間
                    
                    can_fit_current = partcustid_ok and time_ok
                    
                    print(f"🔥 DEBUG: 容量檢查 - 工作站: {current_assignment.station_id}")
                    print(f"🔥 DEBUG: 容量檢查 - 據點: {new_partcustid_count}/{max_partcustids} ({'OK' if partcustid_ok else 'FAIL'})")
                    print(f"🔥 DEBUG: 容量檢查 - 時間: {new_total_time:.1f}/{max_time_per_station:.1f} ({'OK' if time_ok else 'FAIL'})")
                    print(f"🔥 DEBUG: 容量檢查 - 結果: {'可加入' if can_fit_current else '需要新工作站'}")
                else:
                    print(f"🔥 DEBUG: 無current_assignment，需要新工作站")
                
                if can_fit_current:
                    # 加入當前工作站
                    current_assignment.partcustid_groups.append(partcustid_group)
                    
                    # 手動更新統計數據
                    current_assignment.total_partcustids = len(current_assignment.partcustid_groups)
                    current_assignment.total_workload_minutes = sum(g.total_workload_minutes for g in current_assignment.partcustid_groups)
                    
                    print(f"🔥 DEBUG: 據點 {partcustid_group.partcustid} 加入工作站 {current_assignment.station_id}")
                    print(f"🔥 DEBUG: 更新後統計: {current_assignment.total_partcustids}據點, {current_assignment.total_workload_minutes:.1f}分鐘")
                else:
                    # 需要新工作站
                    if current_assignment:
                        assignments.append(current_assignment)
                        print(f"🔥 DEBUG: 完成工作站 {current_assignment.station_id} - {current_assignment.total_partcustids}據點, {current_assignment.total_workload_minutes:.1f}分鐘")
                    
                    # 找新工作站
                    available_station = self._find_next_available_station_by_floor(assigned_stations, floor)
                    
                    print(f"🔥 DEBUG: 工作站查找結果: {available_station}")
                    
                    if available_station:
                        current_assignment = StationAssignment(
                            station_id=available_station,
                            partcustid_groups=[partcustid_group],
                            total_workload_minutes=partcustid_group.total_workload_minutes,
                            total_partcustids=1
                        )
                        assigned_stations.add(available_station)
                        print(f"🔥 DEBUG: 新工作站 {available_station} 開始處理據點 {partcustid_group.partcustid}")
                    else:
                        print(f"🔥 DEBUG: ❌❌❌ 找不到樓層{floor}的可用工作站！")
                        current_assignment = None
                        continue
            
            # 加入該樓層的最後一個工作站
            if current_assignment:
                assignments.append(current_assignment)
                print(f"🔥 DEBUG: 完成樓層{floor}最後工作站 {current_assignment.station_id}")

        return assignments

    def _get_available_gap_stations(self, current_time: datetime, used_stations: set) -> List[str]:
        """取得可用的空檔工作站"""
        available_stations = []
        
        for station_id, station in self.workstations.items():
            if (station_id not in used_stations and 
                not station.reserved_for_exception and
                station.status.value in ['IDLE', 'STARTING_UP']):
                
                # 檢查工作站是否真的可用
                if hasattr(self, 'station_availability_tracker'):
                    available_time = self.station_availability_tracker.get(station_id, current_time)
                    if available_time <= current_time:
                        available_stations.append(station_id)
                else:
                    available_stations.append(station_id)
        
        return available_stations

    def _get_station_staff(self, station_id: str, staff_schedule: pd.DataFrame) -> Optional[int]:
        """取得工作站分配的員工ID"""
        station_staff = staff_schedule[staff_schedule['station_id'] == station_id]
        if len(station_staff) > 0:
            return int(station_staff.iloc[0]['staff_id'])
        return None

    def _calculate_total_gap_time(self, gap_stations: List[str], current_time: datetime) -> float:
        """計算總空檔時間"""
        if not gap_stations:
            return 0.0
        
        # 簡化計算：假設每個空檔工作站有30分鐘可用時間
        return len(gap_stations) * 30.0

    def _prioritize_receiving_over_subwarehouse(self, tasks: List[Task], available_gap_time: float) -> List[Task]:
        """空檔少時進貨優先於副倉庫"""
        receiving_tasks = [task for task in tasks if task.task_type.value == 'RECEIVING']
        subwarehouse_tasks = [task for task in tasks if task.task_type.value == 'SHIPPING' and task.priority_level == 'P3']
        
        # 如果可用空檔時間 < 60分鐘，進貨優先
        if available_gap_time < 60:
            self.logger.info(f"空檔時間少({available_gap_time:.1f}分鐘)，進貨優先於副倉庫")
            return receiving_tasks + subwarehouse_tasks
        else:
            return subwarehouse_tasks + receiving_tasks