"""
ExceptionHandler - 異常處理模組
負責處理出貨和進貨過程中的各種異常情況
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import random
import uuid

# 導入工作站相關的枚舉類型
try:
    from workstation_task_manager import StationStatus, TaskStatus
except ImportError:
    # 如果無法導入，在這裡定義枚舉以避免錯誤
    class StationStatus(Enum):
        IDLE = "IDLE"
        STARTING_UP = "STARTING_UP"
        BUSY = "BUSY"
        MAINTENANCE = "MAINTENANCE"
        RESERVED = "RESERVED"

class ExceptionType(Enum):
    """異常類型枚舉"""
    INVENTORY_SHORTAGE = "INVENTORY_SHORTAGE"     # 庫存不足
    ITEM_DAMAGE = "ITEM_DAMAGE"                   # 零件破損
    PICKING_ERROR = "PICKING_ERROR"               # 揀貨錯誤
    SYSTEM_ERROR = "SYSTEM_ERROR"                 # 系統錯誤
    QUALITY_ISSUE = "QUALITY_ISSUE"               # 品質問題
    BARCODE_UNREADABLE = "BARCODE_UNREADABLE"     # 條碼無法讀取
    PACKAGING_ERROR = "PACKAGING_ERROR"           # 包裝錯誤
    LOCATION_ERROR = "LOCATION_ERROR"             # 儲位錯誤

class ExceptionPriority(Enum):
    """異常處理優先權"""
    CRITICAL = "CRITICAL"     # 關鍵：影響緊急訂單
    HIGH = "HIGH"            # 高：影響一般訂單
    MEDIUM = "MEDIUM"        # 中：可延後處理
    LOW = "LOW"              # 低：不影響當日作業

class ExceptionStatus(Enum):
    """異常處理狀態"""
    DETECTED = "DETECTED"           # 已檢測
    ASSIGNED = "ASSIGNED"           # 已分配處理
    IN_PROGRESS = "IN_PROGRESS"     # 處理中
    RESOLVED = "RESOLVED"           # 已解決
    ESCALATED = "ESCALATED"         # 已升級
    CANCELLED = "CANCELLED"         # 取消

@dataclass
class ExceptionEvent:
    """異常事件物件"""
    exception_id: str
    exception_type: ExceptionType
    priority: ExceptionPriority
    status: ExceptionStatus = ExceptionStatus.DETECTED
    
    # 相關資訊
    task_id: Optional[str] = None
    order_id: Optional[str] = None
    station_id: Optional[str] = None
    item_info: Optional[Dict] = None
    
    # 時間資訊
    detection_time: Optional[datetime] = None
    assignment_time: Optional[datetime] = None
    start_handling_time: Optional[datetime] = None
    resolution_time: Optional[datetime] = None
    
    # 處理資訊
    assigned_leader: Optional[int] = None
    handling_station: Optional[str] = None
    estimated_handling_time: Optional[float] = None  # 分鐘
    actual_handling_time: Optional[float] = None
    
    # 描述和影響
    description: str = ""
    impact_description: str = ""
    resolution_notes: str = ""
    
    # 元數據
    metadata: Dict[str, Any] = field(default_factory=dict)

class ExceptionHandler:
    def __init__(self, data_manager, workstation_manager):
        """初始化異常處理器"""
        self.logger = logging.getLogger(__name__)
        self.data_manager = data_manager
        self.workstation_manager = workstation_manager
        
        # 載入異常處理參數
        self._load_exception_parameters()
        
        # 初始化異常追蹤
        self.active_exceptions: Dict[str, ExceptionEvent] = {}
        self.resolved_exceptions: List[str] = []
        self.exception_history: List[ExceptionEvent] = []
        
        # 主管資源管理
        self.available_leaders: List[int] = []
        self.busy_leaders: Dict[int, str] = {}  # leader_id -> exception_id
        
        # 初始化主管清單
        self._initialize_leaders()
        
    def _load_exception_parameters(self):
        """載入異常處理相關參數"""
        self.params = {
            # 異常發生機率
            'exception_probability_shipping': self.data_manager.get_parameter_value('exception_probability_shipping', 0.02),
            'exception_probability_receiving': self.data_manager.get_parameter_value('exception_probability_receiving', 0.015),
            
            # 處理時間（分鐘）
            'exception_handling_time_avg': self.data_manager.get_parameter_value('exception_handling_time_avg', 15),
            'exception_handling_time_std': self.data_manager.get_parameter_value('exception_handling_time_std', 5),
            
            # 人員配置
            'leader_count': self.data_manager.get_parameter_value('leader_count', 2),
            
            # 處理政策
            'exception_station_priority': self.data_manager.get_parameter_value('exception_station_priority', 'HIGH'),
            'allow_task_interruption': self.data_manager.get_parameter_value('allow_task_interruption', 'Y'),
            'exception_buffer_time': self.data_manager.get_parameter_value('exception_buffer_time', 10),
            
            # 升級條件
            'escalation_time_threshold': self.data_manager.get_parameter_value('escalation_time_threshold', 30),
            'critical_exception_immediate_escalation': self.data_manager.get_parameter_value('critical_exception_immediate_escalation', 'Y')
        }
        
        # 異常類型處理時間矩陣（分鐘）
        self.exception_handling_times = {
            ExceptionType.INVENTORY_SHORTAGE: {'min': 10, 'avg': 20, 'max': 45},
            ExceptionType.ITEM_DAMAGE: {'min': 5, 'avg': 12, 'max': 30},
            ExceptionType.PICKING_ERROR: {'min': 8, 'avg': 15, 'max': 25},
            ExceptionType.SYSTEM_ERROR: {'min': 15, 'avg': 30, 'max': 60},
            ExceptionType.QUALITY_ISSUE: {'min': 20, 'avg': 35, 'max': 90},
            ExceptionType.BARCODE_UNREADABLE: {'min': 3, 'avg': 8, 'max': 15},
            ExceptionType.PACKAGING_ERROR: {'min': 5, 'avg': 10, 'max': 20},
            ExceptionType.LOCATION_ERROR: {'min': 8, 'avg': 18, 'max': 35}
        }
        
        self.logger.info(f"異常處理參數載入完成: {self.params}")
    
    def _initialize_leaders(self):
        """初始化主管清單"""
        leader_count = self.params['leader_count']
        
        # 假設主管ID從901開始
        self.available_leaders = list(range(901, 901 + leader_count))
        self.busy_leaders = {}
        
        self.logger.info(f"初始化 {leader_count} 名主管: {self.available_leaders}")
    
    def detect_exceptions(self, current_time: datetime, context: Dict = None) -> List[ExceptionEvent]:
        """檢測系統中發生的異常"""
        detected_exceptions = []
        
        # 根據不同情境檢測異常
        if context and 'tasks' in context:
            # 檢測任務執行中的異常
            for task in context['tasks']:
                exception = self._check_task_exceptions(task, current_time)
                if exception:
                    detected_exceptions.append(exception)
        
        # 隨機異常檢測（基於機率）
        random_exceptions = self._generate_random_exceptions(current_time)
        detected_exceptions.extend(random_exceptions)
        
        # 註冊檢測到的異常
        for exception in detected_exceptions:
            self._register_exception(exception)
        
        if detected_exceptions:
            self.logger.info(f"檢測到 {len(detected_exceptions)} 個異常事件")
        
        return detected_exceptions
    
    def _check_task_exceptions(self, task, current_time: datetime) -> Optional[ExceptionEvent]:
        """檢查特定任務是否發生異常"""
        # 基於任務特性計算異常機率
        base_probability = self.params['exception_probability_shipping']
        
        # 調整機率因子
        probability_factors = 1.0
        
        # 高優先權任務異常機率稍低（更小心處理）
        if hasattr(task, 'priority_level') and task.priority_level == 'P1':
            probability_factors *= 0.8
        
        # 再包裝任務異常機率稍高
        if hasattr(task, 'requires_repack') and task.requires_repack:
            probability_factors *= 1.3
        
        adjusted_probability = base_probability * probability_factors
        
        if random.random() < adjusted_probability:
            # 發生異常，隨機選擇異常類型
            exception_type = self._select_random_exception_type()
            priority = self._determine_exception_priority(exception_type, task)
            
            exception = ExceptionEvent(
                exception_id=f"EXC_{current_time.strftime('%Y%m%d_%H%M%S')}_{random.randint(1000, 9999)}",
                exception_type=exception_type,
                priority=priority,
                task_id=getattr(task, 'task_id', None),
                order_id=getattr(task, 'order_id', None),
                station_id=getattr(task, 'assigned_station', None),
                detection_time=current_time,
                description=f"任務執行異常: {exception_type.value}",
                item_info={
                    'frcd': getattr(task, 'frcd', ''),
                    'partno': getattr(task, 'partno', ''),
                    'quantity': getattr(task, 'quantity', 0)
                }
            )
            
            return exception
        
        return None
    
    def _generate_random_exceptions(self, current_time: datetime) -> List[ExceptionEvent]:
        """生成隨機異常事件"""
        exceptions = []
        
        # 系統級異常（較低機率）
        if random.random() < 0.001:  # 0.1% 機率
            exception_type = random.choice([
                ExceptionType.SYSTEM_ERROR,
                ExceptionType.QUALITY_ISSUE
            ])
            
            priority = ExceptionPriority.HIGH if exception_type == ExceptionType.SYSTEM_ERROR else ExceptionPriority.MEDIUM
            
            exception = ExceptionEvent(
                exception_id=f"SYS_{current_time.strftime('%Y%m%d_%H%M%S')}_{random.randint(1000, 9999)}",
                exception_type=exception_type,
                priority=priority,
                detection_time=current_time,
                description=f"系統隨機異常: {exception_type.value}"
            )
            
            exceptions.append(exception)
        
        return exceptions
    
    def _select_random_exception_type(self) -> ExceptionType:
        """隨機選擇異常類型（按真實機率分布）"""
        exception_weights = {
            ExceptionType.PICKING_ERROR: 0.3,        # 30% - 最常見
            ExceptionType.BARCODE_UNREADABLE: 0.2,   # 20%
            ExceptionType.INVENTORY_SHORTAGE: 0.15,  # 15%
            ExceptionType.PACKAGING_ERROR: 0.15,     # 15%
            ExceptionType.ITEM_DAMAGE: 0.1,          # 10%
            ExceptionType.LOCATION_ERROR: 0.05,      # 5%
            ExceptionType.QUALITY_ISSUE: 0.03,       # 3%
            ExceptionType.SYSTEM_ERROR: 0.02         # 2%
        }
        
        exception_types = list(exception_weights.keys())
        weights = list(exception_weights.values())
        
        return random.choices(exception_types, weights=weights)[0]
    
    def _determine_exception_priority(self, exception_type: ExceptionType, task=None) -> ExceptionPriority:
        """決定異常處理優先權"""
        # 基於異常類型的基礎優先權
        base_priority_map = {
            ExceptionType.SYSTEM_ERROR: ExceptionPriority.CRITICAL,
            ExceptionType.INVENTORY_SHORTAGE: ExceptionPriority.HIGH,
            ExceptionType.QUALITY_ISSUE: ExceptionPriority.HIGH,
            ExceptionType.ITEM_DAMAGE: ExceptionPriority.MEDIUM,
            ExceptionType.PICKING_ERROR: ExceptionPriority.MEDIUM,
            ExceptionType.PACKAGING_ERROR: ExceptionPriority.MEDIUM,
            ExceptionType.LOCATION_ERROR: ExceptionPriority.MEDIUM,
            ExceptionType.BARCODE_UNREADABLE: ExceptionPriority.LOW
        }
        
        base_priority = base_priority_map.get(exception_type, ExceptionPriority.MEDIUM)
        
        # 根據任務優先權調整
        if task and hasattr(task, 'priority_level'):
            if task.priority_level == 'P1':  # 緊急訂單
                if base_priority == ExceptionPriority.LOW:
                    base_priority = ExceptionPriority.MEDIUM
                elif base_priority == ExceptionPriority.MEDIUM:
                    base_priority = ExceptionPriority.HIGH
        
        return base_priority
    
    def _register_exception(self, exception: ExceptionEvent):
        """註冊異常事件"""
        self.active_exceptions[exception.exception_id] = exception
        self.exception_history.append(exception)
        
        self.logger.warning(f" 異常註冊: {exception.exception_id} - {exception.exception_type.value} ({exception.priority.value})")
    
    def classify_exception_type(self, exception_event: ExceptionEvent) -> Tuple[ExceptionType, ExceptionPriority]:
        """分類異常類型（已在建立時完成，此方法用於重新評估）"""
        return exception_event.exception_type, exception_event.priority
    
    def create_exception_task(self, exception_event: ExceptionEvent, current_time: datetime) -> Dict:
        """生成異常處理任務"""
        if exception_event.exception_id not in self.active_exceptions:
            return {'success': False, 'error': '異常事件不存在'}
        
        # 估算處理時間
        estimated_time = self.estimate_exception_handling_time(
            exception_event.exception_type, 
            exception_event.priority
        )
        
        exception_event.estimated_handling_time = estimated_time
        
        # 尋找可用主管
        available_leader = self._find_available_leader()
        if not available_leader:
            self.logger.warning(f"沒有可用主管處理異常 {exception_event.exception_id}")
            return {
                'success': False, 
                'error': '沒有可用主管',
                'estimated_time': estimated_time
            }
        
        # 分配主管
        exception_event.assigned_leader = available_leader
        exception_event.assignment_time = current_time
        exception_event.status = ExceptionStatus.ASSIGNED
        
        # 更新主管狀態
        self.available_leaders.remove(available_leader)
        self.busy_leaders[available_leader] = exception_event.exception_id
        
        self.logger.info(f" 異常任務建立: {exception_event.exception_id} 分配給主管 {available_leader}")
        
        return {
            'success': True,
            'exception_id': exception_event.exception_id,
            'assigned_leader': available_leader,
            'estimated_time': estimated_time,
            'priority': exception_event.priority.value
        }
    
    def estimate_exception_handling_time(self, exception_type: ExceptionType, 
                                       priority: ExceptionPriority = None) -> float:
        """估計異常處理時間"""
        time_info = self.exception_handling_times.get(exception_type, {
            'min': 10, 'avg': 20, 'max': 40
        })
        
        # 基礎時間使用正態分布
        base_time = np.random.normal(
            time_info['avg'], 
            (time_info['max'] - time_info['min']) / 4
        )
        
        # 確保在合理範圍內
        base_time = max(time_info['min'], min(time_info['max'], base_time))
        
        # 根據優先權調整（高優先權可能得到更多資源，處理更快）
        if priority == ExceptionPriority.CRITICAL:
            base_time *= 0.8  # 20% 更快
        elif priority == ExceptionPriority.LOW:
            base_time *= 1.2  # 20% 更慢
        
        return round(base_time, 1)
    
    def allocate_station_for_exception(self, exception_event: ExceptionEvent, 
                                     current_time: datetime) -> Dict:
        """為異常處理分配工作站"""
        if exception_event.exception_id not in self.active_exceptions:
            return {'success': False, 'error': '異常事件不存在'}
        
        # 尋找適合的工作站
        suitable_station = self._find_suitable_station_for_exception(exception_event)
        
        if not suitable_station:
            return {
                'success': False, 
                'error': '沒有可用工作站',
                'reason': 'all_stations_busy'
            }
        
        # 檢查是否需要中斷現有任務
        station = self.workstation_manager.workstations[suitable_station]
        interruption_required = False
        interrupted_task_id = None
        
        if (station.current_task and 
            station.current_task.status.value == 'IN_PROGRESS' and
            exception_event.priority in [ExceptionPriority.CRITICAL, ExceptionPriority.HIGH]):
            
            if self.params['allow_task_interruption'] == 'Y':
                # 中斷現有任務
                interrupt_result = self.workstation_manager.interrupt_current_task(
                    suitable_station, 
                    f"異常處理: {exception_event.exception_type.value}"
                )
                
                if interrupt_result['success']:
                    interruption_required = True
                    interrupted_task_id = interrupt_result['interrupted_task']
                    self.logger.warning(f"️ 為異常處理中斷任務: {interrupted_task_id}")
                else:
                    return {
                        'success': False,
                        'error': '無法中斷現有任務',
                        'details': interrupt_result
                    }
        
        # 預留工作站
        reserve_result = self.workstation_manager.reserve_station_for_exception(
            suitable_station, exception_event
        )
        
        if not reserve_result['success']:
            return reserve_result
        
        # 更新異常狀態
        exception_event.handling_station = suitable_station
        exception_event.start_handling_time = current_time
        exception_event.status = ExceptionStatus.IN_PROGRESS
        
        # 計算預期完成時間
        completion_time = current_time + timedelta(minutes=exception_event.estimated_handling_time)
        
        self.logger.info(f" 異常 {exception_event.exception_id} 分配到工作站 {suitable_station}")
        
        return {
            'success': True,
            'allocated_station': suitable_station,
            'estimated_completion': completion_time,
            'interruption_required': interruption_required,
            'interrupted_task': interrupted_task_id,
            'handling_leader': exception_event.assigned_leader
        }
    
    def _find_suitable_station_for_exception(self, exception_event: ExceptionEvent) -> Optional[str]:
        """為異常處理找到合適的工作站"""
        # 優先級順序
        priority_order = []
        
        # 1. 如果異常與特定任務相關，優先使用該任務的工作站
        if exception_event.task_id and exception_event.station_id:
            station = self.workstation_manager.workstations.get(exception_event.station_id)
            if station and not station.reserved_for_exception:
                priority_order.append(exception_event.station_id)
        
        # 2. 找空閒工作站
        for station_id, station in self.workstation_manager.workstations.items():
            if (station.status.value == 'IDLE' and 
                not station.reserved_for_exception and
                station_id not in priority_order):
                priority_order.append(station_id)
        
        # 3. 根據異常優先權決定是否可以佔用忙碌工作站
        if exception_event.priority in [ExceptionPriority.CRITICAL, ExceptionPriority.HIGH]:
            for station_id, station in self.workstation_manager.workstations.items():
                if (station.status.value == 'BUSY' and 
                    not station.reserved_for_exception and
                    station_id not in priority_order):
                    priority_order.append(station_id)
        
        return priority_order[0] if priority_order else None
    
    def _find_available_leader(self) -> Optional[int]:
        """找到可用的主管"""
        return self.available_leaders[0] if self.available_leaders else None
    
    def resolve_exception(self, exception_id: str, current_time: datetime, 
                         resolution_notes: str = "") -> Dict:
        """解決異常"""
        if exception_id not in self.active_exceptions:
            return {'success': False, 'error': '異常事件不存在'}
        
        exception = self.active_exceptions[exception_id]
        
        if exception.status != ExceptionStatus.IN_PROGRESS:
            return {'success': False, 'error': f'異常狀態錯誤: {exception.status.value}'}
        
        # 計算實際處理時間
        if exception.start_handling_time:
            actual_time = (current_time - exception.start_handling_time).total_seconds() / 60
            exception.actual_handling_time = round(actual_time, 1)
        
        # 更新異常狀態
        exception.status = ExceptionStatus.RESOLVED
        exception.resolution_time = current_time
        exception.resolution_notes = resolution_notes
        
        # 釋放資源
        self._release_exception_resources(exception)
        
        # 移到已解決清單
        del self.active_exceptions[exception_id]
        self.resolved_exceptions.append(exception_id)
        
        self.logger.info(f" 異常已解決: {exception_id} (處理時間: {exception.actual_handling_time} 分鐘)")
        
        return {
            'success': True,
            'exception_id': exception_id,
            'actual_handling_time': exception.actual_handling_time,
            'estimated_time': exception.estimated_handling_time,
            'time_variance': exception.actual_handling_time - (exception.estimated_handling_time or 0)
        }
    
    def _release_exception_resources(self, exception: ExceptionEvent):
        """釋放異常處理佔用的資源"""
        # 釋放主管
        if exception.assigned_leader and exception.assigned_leader in self.busy_leaders:
            del self.busy_leaders[exception.assigned_leader]
            self.available_leaders.append(exception.assigned_leader)
            self.logger.debug(f"釋放主管 {exception.assigned_leader}")
        
        # 釋放工作站
        if exception.handling_station:
            station = self.workstation_manager.workstations.get(exception.handling_station)
            if station:
                station.reserved_for_exception = False
                station.status = StationStatus.IDLE
                self.logger.debug(f"釋放工作站 {exception.handling_station}")
    
    def assess_exception_impact(self, exception_event: ExceptionEvent, 
                              current_system_state: Dict) -> Dict:
        """評估異常對系統的影響"""
        impact_assessment = {
            'exception_id': exception_event.exception_id,
            'direct_impact': {},
            'indirect_impact': {},
            'affected_orders': [],
            'delayed_tasks': [],
            'resource_impact': {},
            'estimated_recovery_time': 0
        }
        
        # 直接影響評估
        if exception_event.task_id:
            impact_assessment['direct_impact']['affected_task'] = exception_event.task_id
            impact_assessment['direct_impact']['task_delay'] = exception_event.estimated_handling_time
        
        if exception_event.order_id:
            impact_assessment['affected_orders'].append(exception_event.order_id)
        
        # 間接影響評估
        if exception_event.handling_station:
            # 評估工作站被佔用的影響
            station_impact = self._assess_station_impact(
                exception_event.handling_station, 
                exception_event.estimated_handling_time,
                current_system_state
            )
            impact_assessment['indirect_impact']['station_impact'] = station_impact
        
        # 資源影響
        impact_assessment['resource_impact'] = {
            'occupied_leaders': 1 if exception_event.assigned_leader else 0,
            'occupied_stations': 1 if exception_event.handling_station else 0,
            'leader_utilization_impact': 1.0 / len(self.available_leaders + list(self.busy_leaders.keys())) if self.available_leaders or self.busy_leaders else 0
        }
        
        # 估算恢復時間
        impact_assessment['estimated_recovery_time'] = self._estimate_recovery_time(exception_event)
        
        return impact_assessment
    
    def _assess_station_impact(self, station_id: str, occupation_time: float, 
                             system_state: Dict) -> Dict:
        """評估工作站被佔用的影響"""
        station_impact = {
            'station_id': station_id,
            'occupation_time': occupation_time,
            'queued_tasks': 0,
            'estimated_delay': 0
        }
        
        # 這裡可以加入更複雜的影響評估邏輯
        # 例如：計算排隊任務數量、估算延遲時間等
        
        return station_impact
    
    def _estimate_recovery_time(self, exception_event: ExceptionEvent) -> float:
        """估算系統恢復正常運作的時間"""
        base_recovery = exception_event.estimated_handling_time or 0
        
        # 根據異常類型調整恢復時間
        if exception_event.exception_type == ExceptionType.SYSTEM_ERROR:
            base_recovery *= 1.5  # 系統錯誤需要更多時間恢復
        elif exception_event.exception_type == ExceptionType.QUALITY_ISSUE:
            base_recovery *= 1.3  # 品質問題可能有後續影響
        
        return round(base_recovery, 1)
    
    def calculate_delay_propagation(self, interrupted_tasks: List[str]) -> Dict:
        """計算延遲擴散效應"""
        propagation_analysis = {
            'total_interrupted_tasks': len(interrupted_tasks),
            'task_delays': {},
            'wave_impacts': {},
            'cumulative_delay': 0
        }
        
        total_delay = 0
        
        for task_id in interrupted_tasks:
            if task_id in self.workstation_manager.tasks:
                task = self.workstation_manager.tasks[task_id]
                
                # 估算任務延遲（簡化邏輯）
                estimated_delay = task.estimated_duration * 0.3  # 假設延遲30%
                propagation_analysis['task_delays'][task_id] = estimated_delay
                total_delay += estimated_delay
        
        propagation_analysis['cumulative_delay'] = round(total_delay, 1)
        
        return propagation_analysis
    
    def get_exception_summary(self, current_time: datetime) -> Dict:
        """取得異常處理摘要"""
        summary = {
            'current_time': current_time,
            'active_exceptions_count': len(self.active_exceptions),
            'resolved_exceptions_count': len(self.resolved_exceptions),
            'total_exceptions_today': len(self.exception_history),
            
            # 按狀態統計
            'exceptions_by_status': {},
            
            # 按類型統計
            'exceptions_by_type': {},
            
            # 按優先權統計
            'exceptions_by_priority': {},
            
            # 資源使用狀況
            'resource_utilization': {
                'available_leaders': len(self.available_leaders),
                'busy_leaders': len(self.busy_leaders),
                'leader_utilization_rate': 0.0
            },
            
            # 處理效率
            'handling_efficiency': {
                'avg_handling_time': 0.0,
                'avg_time_variance': 0.0,
                'on_time_resolution_rate': 0.0
            },
            
            # 活躍異常詳情
            'active_exceptions_details': []
        }
        
        # 統計活躍異常
        for exception in self.active_exceptions.values():
            # 按狀態統計
            status = exception.status.value
            summary['exceptions_by_status'][status] = summary['exceptions_by_status'].get(status, 0) + 1
            
            # 按類型統計
            exc_type = exception.exception_type.value
            summary['exceptions_by_type'][exc_type] = summary['exceptions_by_type'].get(exc_type, 0) + 1
            
            # 按優先權統計
            priority = exception.priority.value
            summary['exceptions_by_priority'][priority] = summary['exceptions_by_priority'].get(priority, 0) + 1
            
            # 活躍異常詳情
            exception_detail = {
                'exception_id': exception.exception_id,
                'type': exc_type,
                'priority': priority,
                'status': status,
                'assigned_leader': exception.assigned_leader,
                'handling_station': exception.handling_station,
                'estimated_time': exception.estimated_handling_time
            }
            
            # 計算已用時間
            if exception.start_handling_time:
                elapsed_time = (current_time - exception.start_handling_time).total_seconds() / 60
                exception_detail['elapsed_time'] = round(elapsed_time, 1)
                
                if exception.estimated_handling_time:
                    remaining_time = max(0, exception.estimated_handling_time - elapsed_time)
                    exception_detail['remaining_time'] = round(remaining_time, 1)
            
            summary['active_exceptions_details'].append(exception_detail)
        
        # 計算資源利用率
        total_leaders = len(self.available_leaders) + len(self.busy_leaders)
        if total_leaders > 0:
            summary['resource_utilization']['leader_utilization_rate'] = round(
                len(self.busy_leaders) / total_leaders * 100, 1
            )
        
        # 計算處理效率（基於已解決的異常）
        resolved_exceptions = [exc for exc in self.exception_history if exc.status == ExceptionStatus.RESOLVED]
        
        if resolved_exceptions:
            handling_times = [exc.actual_handling_time for exc in resolved_exceptions if exc.actual_handling_time]
            estimated_times = [exc.estimated_handling_time for exc in resolved_exceptions if exc.estimated_handling_time]
            
            if handling_times:
                summary['handling_efficiency']['avg_handling_time'] = round(np.mean(handling_times), 1)
            
            if handling_times and estimated_times and len(handling_times) == len(estimated_times):
                variances = [actual - estimated for actual, estimated in zip(handling_times, estimated_times)]
                summary['handling_efficiency']['avg_time_variance'] = round(np.mean(variances), 1)
                
                # 計算準時解決率（實際時間<=預估時間*1.1）
                on_time_count = sum(1 for actual, estimated in zip(handling_times, estimated_times) 
                                  if actual <= estimated * 1.1)
                summary['handling_efficiency']['on_time_resolution_rate'] = round(
                    on_time_count / len(handling_times) * 100, 1
                )
        
        return summary
    
    def escalate_exception(self, exception_id: str, current_time: datetime, 
                          escalation_reason: str = "") -> Dict:
        """升級異常處理"""
        if exception_id not in self.active_exceptions:
            return {'success': False, 'error': '異常事件不存在'}
        
        exception = self.active_exceptions[exception_id]
        
        # 更新異常狀態
        original_priority = exception.priority
        exception.status = ExceptionStatus.ESCALATED
        
        # 提升優先權
        if exception.priority == ExceptionPriority.LOW:
            exception.priority = ExceptionPriority.MEDIUM
        elif exception.priority == ExceptionPriority.MEDIUM:
            exception.priority = ExceptionPriority.HIGH
        elif exception.priority == ExceptionPriority.HIGH:
            exception.priority = ExceptionPriority.CRITICAL
        
        # 記錄升級資訊
        exception.metadata['escalation_time'] = current_time
        exception.metadata['escalation_reason'] = escalation_reason
        exception.metadata['original_priority'] = original_priority.value
        
        self.logger.warning(f"️ 異常升級: {exception_id} {original_priority.value} → {exception.priority.value}")
        
        return {
            'success': True,
            'exception_id': exception_id,
            'original_priority': original_priority.value,
            'new_priority': exception.priority.value,
            'escalation_reason': escalation_reason
        }
    
    def check_escalation_conditions(self, current_time: datetime) -> List[str]:
        """檢查需要升級的異常"""
        escalation_candidates = []
        escalation_threshold = self.params['escalation_time_threshold']
        
        for exception_id, exception in self.active_exceptions.items():
            should_escalate = False
            escalation_reason = ""
            
            # 條件1：處理時間超過閾值
            if exception.start_handling_time:
                elapsed_time = (current_time - exception.start_handling_time).total_seconds() / 60
                if elapsed_time > escalation_threshold:
                    should_escalate = True
                    escalation_reason = f"處理時間超過 {escalation_threshold} 分鐘"
            
            # 條件2：關鍵異常立即升級
            if (exception.priority == ExceptionPriority.CRITICAL and 
                self.params['critical_exception_immediate_escalation'] == 'Y' and
                exception.status == ExceptionStatus.ASSIGNED):
                should_escalate = True
                escalation_reason = "關鍵異常立即升級"
            
            # 條件3：等待分配時間過長
            if (exception.status == ExceptionStatus.DETECTED and
                exception.detection_time and
                (current_time - exception.detection_time).total_seconds() / 60 > 10):
                should_escalate = True
                escalation_reason = "等待分配時間過長"
            
            if should_escalate:
                escalation_candidates.append((exception_id, escalation_reason))
        
        # 執行升級
        escalated_exceptions = []
        for exception_id, reason in escalation_candidates:
            result = self.escalate_exception(exception_id, current_time, reason)
            if result['success']:
                escalated_exceptions.append(exception_id)
        
        return escalated_exceptions
    
    def get_exception_performance_metrics(self) -> Dict:
        """取得異常處理績效指標"""
        resolved_exceptions = [exc for exc in self.exception_history if exc.status == ExceptionStatus.RESOLVED]
        
        metrics = {
            'total_exceptions': len(self.exception_history),
            'resolved_count': len(resolved_exceptions),
            'resolution_rate': 0.0,
            'avg_detection_to_resolution_time': 0.0,
            'avg_handling_time': 0.0,
            'exception_frequency_by_type': {},
            'resolution_efficiency_by_type': {},
            'leader_performance': {}
        }
        
        if self.exception_history:
            metrics['resolution_rate'] = round(len(resolved_exceptions) / len(self.exception_history) * 100, 1)
        
        if resolved_exceptions:
            # 計算平均解決時間
            total_resolution_times = []
            handling_times = []
            
            for exc in resolved_exceptions:
                if exc.detection_time and exc.resolution_time:
                    resolution_time = (exc.resolution_time - exc.detection_time).total_seconds() / 60
                    total_resolution_times.append(resolution_time)
                
                if exc.actual_handling_time:
                    handling_times.append(exc.actual_handling_time)
            
            if total_resolution_times:
                metrics['avg_detection_to_resolution_time'] = round(np.mean(total_resolution_times), 1)
            
            if handling_times:
                metrics['avg_handling_time'] = round(np.mean(handling_times), 1)
        
        # 按類型統計
        for exc in self.exception_history:
            exc_type = exc.exception_type.value
            metrics['exception_frequency_by_type'][exc_type] = metrics['exception_frequency_by_type'].get(exc_type, 0) + 1
        
        # 按類型的解決效率
        for exc in resolved_exceptions:
            exc_type = exc.exception_type.value
            if exc.actual_handling_time and exc.estimated_handling_time:
                efficiency = exc.estimated_handling_time / exc.actual_handling_time
                if exc_type not in metrics['resolution_efficiency_by_type']:
                    metrics['resolution_efficiency_by_type'][exc_type] = []
                metrics['resolution_efficiency_by_type'][exc_type].append(efficiency)
        
        # 計算平均效率
        for exc_type, efficiencies in metrics['resolution_efficiency_by_type'].items():
            metrics['resolution_efficiency_by_type'][exc_type] = round(np.mean(efficiencies), 2)
        
        # 主管績效（簡化版）
        leader_counts = {}
        for exc in resolved_exceptions:
            if exc.assigned_leader:
                leader_counts[exc.assigned_leader] = leader_counts.get(exc.assigned_leader, 0) + 1
        
        metrics['leader_performance'] = leader_counts
        
        return metrics
    
    def simulate_exception_scenarios(self, scenario_config: Dict) -> Dict:
        """模擬異常情境（用於What-if分析）"""
        original_params = self.params.copy()
        
        try:
            # 暫時修改參數
            if 'exception_probability_multiplier' in scenario_config:
                multiplier = scenario_config['exception_probability_multiplier']
                self.params['exception_probability_shipping'] *= multiplier
                self.params['exception_probability_receiving'] *= multiplier
            
            if 'leader_count_reduction' in scenario_config:
                reduction = scenario_config['leader_count_reduction']
                original_leader_count = len(self.available_leaders) + len(self.busy_leaders)
                new_leader_count = max(1, original_leader_count - reduction)
                
                # 模擬結果（簡化版）
                impact_multiplier = original_leader_count / new_leader_count
                
                simulation_results = {
                    'scenario': scenario_config,
                    'original_leader_count': original_leader_count,
                    'reduced_leader_count': new_leader_count,
                    'estimated_handling_time_increase': round((impact_multiplier - 1) * 100, 1),
                    'estimated_queue_length_increase': round(impact_multiplier * 1.5, 1)
                }
            else:
                simulation_results = {
                    'scenario': scenario_config,
                    'message': '情境模擬完成'
                }
            
            return simulation_results
            
        finally:
            # 恢復原始參數
            self.params = original_params
    
    def reset_exception_state(self):
        """重置異常處理狀態（用於新的模擬運行）"""
        self.active_exceptions.clear()
        self.resolved_exceptions.clear()
        self.exception_history.clear()
        
        # 重置主管狀態
        self.available_leaders = list(range(901, 901 + self.params['leader_count']))
        self.busy_leaders.clear()
        
        self.logger.info("異常處理狀態已重置")
    
    def export_exception_log(self, start_time: datetime = None, 
                           end_time: datetime = None) -> pd.DataFrame:
        """匯出異常處理記錄"""
        records = []
        
        for exc in self.exception_history:
            # 時間篩選
            if start_time and exc.detection_time and exc.detection_time < start_time:
                continue
            if end_time and exc.detection_time and exc.detection_time > end_time:
                continue
            
            record = {
                'exception_id': exc.exception_id,
                'exception_type': exc.exception_type.value,
                'priority': exc.priority.value,
                'status': exc.status.value,
                'task_id': exc.task_id,
                'order_id': exc.order_id,
                'station_id': exc.station_id,
                'assigned_leader': exc.assigned_leader,
                'handling_station': exc.handling_station,
                'detection_time': exc.detection_time,
                'assignment_time': exc.assignment_time,
                'start_handling_time': exc.start_handling_time,
                'resolution_time': exc.resolution_time,
                'estimated_handling_time': exc.estimated_handling_time,
                'actual_handling_time': exc.actual_handling_time,
                'description': exc.description,
                'resolution_notes': exc.resolution_notes
            }
            
            records.append(record)
        
        return pd.DataFrame(records)