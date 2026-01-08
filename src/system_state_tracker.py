"""
SystemStateTracker - 系統狀態追蹤模組
負責實時追蹤系統各組件狀態
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict, deque

class SystemComponent(Enum):
    """系統組件枚舉"""
    WORKSTATION = "WORKSTATION"
    TASK = "TASK"
    WAVE = "WAVE"
    STAFF = "STAFF"
    EXCEPTION = "EXCEPTION"
    ORDER = "ORDER"

@dataclass
class StateSnapshot:
    """狀態快照"""
    timestamp: datetime
    component_type: SystemComponent
    component_id: str
    state_data: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SystemMetrics:
    """系統指標"""
    timestamp: datetime
    workstation_utilization: float = 0.0
    task_completion_rate: float = 0.0
    wave_progress_avg: float = 0.0
    exception_count: int = 0
    staff_utilization: float = 0.0
    overall_efficiency: float = 0.0

class SystemStateTracker:
    def __init__(self, workstation_manager, wave_manager, exception_handler, staff_schedule_generator):
        """初始化系統狀態追蹤器"""
        self.logger = logging.getLogger(__name__)
        
        # 關聯的管理器
        self.workstation_manager = workstation_manager
        self.wave_manager = wave_manager
        self.exception_handler = exception_handler
        self.staff_schedule_generator = staff_schedule_generator
        
        # 狀態追蹤設定
        self.tracking_enabled = True
        self.snapshot_interval = 60  # 秒
        self.max_history_size = 1000
        
        # 狀態歷史記錄
        self.state_history: Dict[SystemComponent, deque] = {
            component: deque(maxlen=self.max_history_size) 
            for component in SystemComponent
        }
        
        # 即時狀態快取
        self.current_state: Dict[SystemComponent, Dict[str, Any]] = {
            component: {} for component in SystemComponent
        }
        
        # 系統指標歷史
        self.metrics_history: deque = deque(maxlen=self.max_history_size)
        
        # 狀態變更事件追蹤
        self.state_changes: deque = deque(maxlen=500)
        
        # 最後更新時間
        self.last_update_time: Optional[datetime] = None
        self.last_snapshot_time: Optional[datetime] = None
        
        self.logger.info("SystemStateTracker 初始化完成")
    
    def update_system_state(self, current_time: datetime, force_update: bool = False):
        """更新整個系統狀態"""
        if not self.tracking_enabled:
            return
        
        # 檢查是否需要更新
        if not force_update and self.last_update_time:
            time_since_update = (current_time - self.last_update_time).total_seconds()
            if time_since_update < 10:  # 最小更新間隔10秒
                return
        
        try:
            # 更新各組件狀態
            self._update_workstation_states(current_time)
            self._update_task_states(current_time)
            self._update_wave_states(current_time)
            self._update_staff_states(current_time)
            self._update_exception_states(current_time)
            
            # 計算系統指標
            metrics = self._calculate_system_metrics(current_time)
            self.metrics_history.append(metrics)
            
            # 檢查是否需要創建快照
            if self._should_create_snapshot(current_time):
                self._create_system_snapshot(current_time)
            
            self.last_update_time = current_time
            
        except Exception as e:
            self.logger.error(f"更新系統狀態時發生錯誤: {str(e)}")
    
    def _update_workstation_states(self, current_time: datetime):
        """更新工作站狀態"""
        workstation_states = {}
        
        for station_id, station in self.workstation_manager.workstations.items():
            # 取得工作站進度資訊
            progress_info = self.workstation_manager.monitor_station_progress(station_id, current_time)
            
            state_data = {
                'station_id': station_id,
                'status': station.status.value,
                'floor': station.floor,
                'is_fixed': station.is_fixed,
                'assigned_staff': station.assigned_staff,
                'reserved_for_exception': station.reserved_for_exception,
                'current_task_id': station.current_task.task_id if station.current_task else None,
                'available_time': station.available_time,
                'startup_time': station.startup_time,
                'progress_info': progress_info,
                'last_updated': current_time
            }
            
            workstation_states[station_id] = state_data
            
            # 檢查狀態變更
            self._check_state_change(SystemComponent.WORKSTATION, station_id, state_data, current_time)
        
        self.current_state[SystemComponent.WORKSTATION] = workstation_states
    
    def _update_task_states(self, current_time: datetime):
        """更新任務狀態"""
        task_states = {}
        
        for task_id, task in self.workstation_manager.tasks.items():
            state_data = {
                'task_id': task_id,
                'order_id': task.order_id,
                'status': task.status.value,
                'priority_level': task.priority_level,
                'assigned_station': task.assigned_station,
                'assigned_staff': task.assigned_staff,
                'estimated_duration': task.estimated_duration,
                'start_time': task.start_time,
                'estimated_completion': task.estimated_completion,
                'actual_completion': task.actual_completion,
                'floor': task.floor,
                'requires_repack': task.requires_repack,
                'item_info': {
                    'frcd': task.frcd,
                    'partno': task.partno,
                    'quantity': task.quantity
                },
                'last_updated': current_time
            }
            
            # 計算進度百分比
            if task.start_time and task.estimated_completion and task.status.value == 'IN_PROGRESS':
                total_duration = (task.estimated_completion - task.start_time).total_seconds()
                elapsed_duration = (current_time - task.start_time).total_seconds()
                
                if total_duration > 0:
                    progress_percent = min(100, (elapsed_duration / total_duration) * 100)
                    state_data['progress_percent'] = round(progress_percent, 1)
                    
                    remaining_seconds = max(0, total_duration - elapsed_duration)
                    state_data['remaining_minutes'] = round(remaining_seconds / 60, 1)
                else:
                    state_data['progress_percent'] = 100
                    state_data['remaining_minutes'] = 0
            else:
                state_data['progress_percent'] = 0 if task.status.value != 'COMPLETED' else 100
                state_data['remaining_minutes'] = task.estimated_duration if task.status.value == 'PENDING' else 0
            
            task_states[task_id] = state_data
            
            # 檢查狀態變更
            self._check_state_change(SystemComponent.TASK, task_id, state_data, current_time)
        
        self.current_state[SystemComponent.TASK] = task_states
    
    def _update_wave_states(self, current_time: datetime):
        """更新波次狀態"""
        wave_states = {}
        
        # 活躍波次
        for wave_id in self.wave_manager.active_waves:
            if wave_id in self.wave_manager.waves:
                wave = self.wave_manager.waves[wave_id]
                progress_info = self.wave_manager.track_wave_progress(wave_id, current_time)
                
                state_data = {
                    'wave_id': wave_id,
                    'status': wave.status.value,
                    'wave_type': wave.wave_type.value,
                    'priority_level': wave.priority_level,
                    'route_codes': wave.route_codes,
                    'total_tasks': wave.total_tasks,
                    'completed_tasks': wave.completed_tasks,
                    'assigned_workstations': wave.assigned_workstations,
                    'planned_start_time': wave.planned_start_time,
                    'actual_start_time': wave.actual_start_time,
                    'estimated_completion_time': wave.estimated_completion_time,
                    'actual_completion_time': wave.actual_completion_time,
                    'progress_info': progress_info,
                    'last_updated': current_time
                }
                
                wave_states[wave_id] = state_data
                
                # 檢查狀態變更
                self._check_state_change(SystemComponent.WAVE, wave_id, state_data, current_time)
        
        # 最近完成的波次（保留最近10個）
        recent_completed = self.wave_manager.wave_history[-10:] if len(self.wave_manager.wave_history) > 10 else self.wave_manager.wave_history
        
        for wave_id in recent_completed:
            if wave_id in self.wave_manager.waves:
                wave = self.wave_manager.waves[wave_id]
                
                state_data = {
                    'wave_id': wave_id,
                    'status': wave.status.value,
                    'wave_type': wave.wave_type.value,
                    'priority_level': wave.priority_level,
                    'total_tasks': wave.total_tasks,
                    'completed_tasks': wave.completed_tasks,
                    'actual_start_time': wave.actual_start_time,
                    'actual_completion_time': wave.actual_completion_time,
                    'is_completed': True,
                    'last_updated': current_time
                }
                
                wave_states[f"{wave_id}_completed"] = state_data
        
        self.current_state[SystemComponent.WAVE] = wave_states
    
    def _update_staff_states(self, current_time: datetime):
        """更新員工狀態"""
        staff_states = {}
        
        # 這裡需要從排班資料和工作站分配狀況推斷員工狀態
        # 簡化版本：基於工作站分配推斷員工狀態
        
        for station_id, station in self.workstation_manager.workstations.items():
            if station.assigned_staff:
                staff_id = station.assigned_staff
                
                state_data = {
                    'staff_id': staff_id,
                    'assigned_station': station_id,
                    'station_status': station.status.value,
                    'floor': station.floor,
                    'current_task_id': station.current_task.task_id if station.current_task else None,
                    'is_busy': station.status.value in ['BUSY', 'STARTING_UP'],
                    'last_updated': current_time
                }
                
                # 從員工主檔取得額外資訊
                staff_info = self.staff_schedule_generator.get_staff_info(staff_id)
                if staff_info:
                    state_data.update({
                        'staff_name': staff_info.get('staff_name', f'Staff_{staff_id}'),
                        'skill_level': staff_info.get('skill_level', 3),
                        'capacity_multiplier': staff_info.get('capacity_multiplier', 1.0)
                    })
                
                staff_states[staff_id] = state_data
                
                # 檢查狀態變更
                self._check_state_change(SystemComponent.STAFF, staff_id, state_data, current_time)
        
        # 主管狀態
        for leader_id in self.exception_handler.available_leaders:
            state_data = {
                'staff_id': leader_id,
                'role': 'leader',
                'is_available': True,
                'assigned_exception': None,
                'last_updated': current_time
            }
            
            staff_states[f"leader_{leader_id}"] = state_data
        
        for leader_id, exception_id in self.exception_handler.busy_leaders.items():
            state_data = {
                'staff_id': leader_id,
                'role': 'leader',
                'is_available': False,
                'assigned_exception': exception_id,
                'last_updated': current_time
            }
            
            staff_states[f"leader_{leader_id}"] = state_data
        
        self.current_state[SystemComponent.STAFF] = staff_states
    
    def _update_exception_states(self, current_time: datetime):
        """更新異常狀態"""
        exception_states = {}
        
        # 活躍異常
        for exception_id, exception in self.exception_handler.active_exceptions.items():
            state_data = {
                'exception_id': exception_id,
                'exception_type': exception.exception_type.value,
                'priority': exception.priority.value,
                'status': exception.status.value,
                'task_id': exception.task_id,
                'order_id': exception.order_id,
                'station_id': exception.station_id,
                'assigned_leader': exception.assigned_leader,
                'handling_station': exception.handling_station,
                'detection_time': exception.detection_time,
                'assignment_time': exception.assignment_time,
                'start_handling_time': exception.start_handling_time,
                'estimated_handling_time': exception.estimated_handling_time,
                'description': exception.description,
                'last_updated': current_time
            }
            
            # 計算處理進度
            if exception.start_handling_time and exception.estimated_handling_time:
                elapsed_time = (current_time - exception.start_handling_time).total_seconds() / 60
                progress_percent = min(100, (elapsed_time / exception.estimated_handling_time) * 100)
                remaining_time = max(0, exception.estimated_handling_time - elapsed_time)
                
                state_data.update({
                    'elapsed_time': round(elapsed_time, 1),
                    'progress_percent': round(progress_percent, 1),
                    'remaining_time': round(remaining_time, 1)
                })
            
            exception_states[exception_id] = state_data
            
            # 檢查狀態變更
            self._check_state_change(SystemComponent.EXCEPTION, exception_id, state_data, current_time)
        
        self.current_state[SystemComponent.EXCEPTION] = exception_states
    
    def _calculate_system_metrics(self, current_time: datetime) -> SystemMetrics:
        """計算系統整體指標"""
        metrics = SystemMetrics(timestamp=current_time)
        
        # 工作站利用率
        if self.workstation_manager.workstations:
            busy_stations = sum(1 for station in self.workstation_manager.workstations.values() 
                              if station.status.value in ['BUSY', 'STARTING_UP'])
            metrics.workstation_utilization = round(busy_stations / len(self.workstation_manager.workstations) * 100, 1)
        
        # 任務完成率
        if self.workstation_manager.tasks:
            completed_tasks = sum(1 for task in self.workstation_manager.tasks.values() 
                                if task.status.value == 'COMPLETED')
            metrics.task_completion_rate = round(completed_tasks / len(self.workstation_manager.tasks) * 100, 1)
        
        # 波次平均進度
        if self.wave_manager.active_waves:
            total_progress = 0
            valid_waves = 0
            
            for wave_id in self.wave_manager.active_waves:
                if wave_id in self.wave_manager.waves:
                    wave = self.wave_manager.waves[wave_id]
                    if wave.total_tasks > 0:
                        progress = (wave.completed_tasks / wave.total_tasks) * 100
                        total_progress += progress
                        valid_waves += 1
            
            if valid_waves > 0:
                metrics.wave_progress_avg = round(total_progress / valid_waves, 1)
        
        # 異常數量
        metrics.exception_count = len(self.exception_handler.active_exceptions)
        
        # 員工利用率
        total_staff = len(self.current_state[SystemComponent.STAFF])
        if total_staff > 0:
            busy_staff = sum(1 for staff_data in self.current_state[SystemComponent.STAFF].values() 
                           if staff_data.get('is_busy', False))
            metrics.staff_utilization = round(busy_staff / total_staff * 100, 1)
        
        # 整體效率（綜合指標）
        efficiency_factors = [
            metrics.workstation_utilization,
            metrics.task_completion_rate,
            metrics.wave_progress_avg,
            100 - min(100, metrics.exception_count * 10)  # 異常越多效率越低
        ]
        
        valid_factors = [f for f in efficiency_factors if f > 0]
        if valid_factors:
            metrics.overall_efficiency = round(sum(valid_factors) / len(valid_factors), 1)
        
        return metrics
    
    def _check_state_change(self, component_type: SystemComponent, component_id: str, 
                           new_state: Dict, current_time: datetime):
        """檢查狀態變更並記錄"""
        if component_type not in self.current_state:
            return
        
        old_state = self.current_state[component_type].get(component_id, {})
        
        # 比較關鍵狀態欄位
        key_fields = self._get_key_fields_for_component(component_type)
        
        changes = {}
        for field in key_fields:
            old_value = old_state.get(field)
            new_value = new_state.get(field)
            
            if old_value != new_value:
                changes[field] = {'old': old_value, 'new': new_value}
        
        if changes:
            change_event = {
                'timestamp': current_time,
                'component_type': component_type.value,
                'component_id': component_id,
                'changes': changes
            }
            
            self.state_changes.append(change_event)
            
            # 記錄重要狀態變更
            self._log_important_state_changes(component_type, component_id, changes, current_time)
    
    def _get_key_fields_for_component(self, component_type: SystemComponent) -> List[str]:
        """取得各組件的關鍵狀態欄位"""
        key_fields_map = {
            SystemComponent.WORKSTATION: ['status', 'current_task_id', 'assigned_staff', 'reserved_for_exception'],
            SystemComponent.TASK: ['status', 'assigned_station', 'progress_percent'],
            SystemComponent.WAVE: ['status', 'completed_tasks', 'progress_percent'],
            SystemComponent.STAFF: ['assigned_station', 'is_busy', 'assigned_exception'],
            SystemComponent.EXCEPTION: ['status', 'assigned_leader', 'handling_station', 'progress_percent']
        }
        
        return key_fields_map.get(component_type, [])
    
    def _log_important_state_changes(self, component_type: SystemComponent, component_id: str, 
                                   changes: Dict, current_time: datetime):
        """記錄重要的狀態變更"""
        for field, change in changes.items():
            old_val, new_val = change['old'], change['new']
            
            # 工作站狀態變更
            if component_type == SystemComponent.WORKSTATION and field == 'status':
                if new_val == 'BUSY':
                    self.logger.info(f" 工作站 {component_id} 開始工作")
                elif new_val == 'IDLE':
                    self.logger.info(f" 工作站 {component_id} 完成作業")
                elif new_val == 'RESERVED':
                    self.logger.warning(f"️ 工作站 {component_id} 被異常處理預留")
            
            # 任務狀態變更
            elif component_type == SystemComponent.TASK and field == 'status':
                if new_val == 'IN_PROGRESS':
                    self.logger.info(f" 任務 {component_id} 開始執行")
                elif new_val == 'COMPLETED':
                    self.logger.info(f" 任務 {component_id} 完成")
                elif new_val == 'PAUSED':
                    self.logger.warning(f"⏸️ 任務 {component_id} 被暫停")
            
            # 波次狀態變更
            elif component_type == SystemComponent.WAVE and field == 'status':
                if new_val == 'IN_PROGRESS':
                    self.logger.info(f" 波次 {component_id} 開始執行")
                elif new_val == 'COMPLETED':
                    self.logger.info(f" 波次 {component_id} 完成")
            
            # 異常狀態變更
            elif component_type == SystemComponent.EXCEPTION and field == 'status':
                if new_val == 'IN_PROGRESS':
                    self.logger.warning(f" 異常 {component_id} 開始處理")
                elif new_val == 'RESOLVED':
                    self.logger.info(f" 異常 {component_id} 已解決")
    
    def _should_create_snapshot(self, current_time: datetime) -> bool:
        """判斷是否需要創建系統快照"""
        if not self.last_snapshot_time:
            return True
        
        time_since_snapshot = (current_time - self.last_snapshot_time).total_seconds()
        return time_since_snapshot >= self.snapshot_interval
    
    def _create_system_snapshot(self, current_time: datetime):
        """創建系統狀態快照"""
        snapshot_data = {
            'timestamp': current_time,
            'workstations': dict(self.current_state[SystemComponent.WORKSTATION]),
            'tasks': dict(self.current_state[SystemComponent.TASK]),
            'waves': dict(self.current_state[SystemComponent.WAVE]),
            'staff': dict(self.current_state[SystemComponent.STAFF]),
            'exceptions': dict(self.current_state[SystemComponent.EXCEPTION]),
            'metrics': self.metrics_history[-1].__dict__ if self.metrics_history else {}
        }
        
        # 為每個組件類型創建快照
        for component_type in SystemComponent:
            snapshot = StateSnapshot(
                timestamp=current_time,
                component_type=component_type,
                component_id="SYSTEM_WIDE",
                state_data=snapshot_data[component_type.value.lower() + 's'],
                metadata={'snapshot_type': 'periodic', 'system_wide': True}
            )
            
            self.state_history[component_type].append(snapshot)
        
        self.last_snapshot_time = current_time
        self.logger.debug(f"系統快照已創建: {current_time}")
    
    def track_station_status(self, current_time: datetime) -> Dict[str, Any]:
        """追蹤工作站狀態"""
        station_summary = {
            'timestamp': current_time,
            'total_stations': len(self.workstation_manager.workstations),
            'status_distribution': defaultdict(int),
            'floor_distribution': defaultdict(int),
            'utilization_by_floor': {},
            'active_stations': [],
            'idle_stations': [],
            'reserved_stations': [],
            'station_details': {}
        }
        
        floor_stats = defaultdict(lambda: {'total': 0, 'busy': 0})
        
        for station_id, station_data in self.current_state[SystemComponent.WORKSTATION].items():
            status = station_data['status']
            floor = station_data['floor']
            
            # 統計狀態分布
            station_summary['status_distribution'][status] += 1
            station_summary['floor_distribution'][floor] += 1
            
            # 樓層統計
            floor_stats[floor]['total'] += 1
            if status in ['BUSY', 'STARTING_UP']:
                floor_stats[floor]['busy'] += 1
            
            # 分類工作站
            if status == 'BUSY':
                station_summary['active_stations'].append(station_id)
            elif status == 'IDLE':
                station_summary['idle_stations'].append(station_id)
            elif status == 'RESERVED':
                station_summary['reserved_stations'].append(station_id)
            
            # 詳細資訊
            station_summary['station_details'][station_id] = {
                'status': status,
                'floor': floor,
                'current_task': station_data.get('current_task_id'),
                'assigned_staff': station_data.get('assigned_staff'),
                'progress': station_data.get('progress_info', {}).get('current_task', {}).get('progress_percent', 0),
                'is_fixed': station_data.get('is_fixed', True)
            }
        
        # 計算各樓層利用率
        for floor, stats in floor_stats.items():
            if stats['total'] > 0:
                utilization = round(stats['busy'] / stats['total'] * 100, 1)
                station_summary['utilization_by_floor'][floor] = utilization
        
        return station_summary
    
    def track_task_progress(self, task_id: str, current_time: datetime) -> Dict[str, Any]:
        """追蹤單一任務進度"""
        if task_id not in self.current_state[SystemComponent.TASK]:
            return {'error': f'任務 {task_id} 不存在'}
        
        task_data = self.current_state[SystemComponent.TASK][task_id]
        
        progress_info = {
            'task_id': task_id,
            'timestamp': current_time,
            'basic_info': {
                'order_id': task_data['order_id'],
                'status': task_data['status'],
                'priority': task_data['priority_level'],
                'floor': task_data['floor']
            },
            'assignment_info': {
                'assigned_station': task_data['assigned_station'],
                'assigned_staff': task_data['assigned_staff']
            },
            'timing_info': {
                'estimated_duration': task_data['estimated_duration'],
                'start_time': task_data['start_time'],
                'estimated_completion': task_data['estimated_completion'],
                'actual_completion': task_data['actual_completion']
            },
            'progress': {
                'progress_percent': task_data.get('progress_percent', 0),
                'remaining_minutes': task_data.get('remaining_minutes', 0)
            },
            'item_details': task_data['item_info']
        }
        
        # 計算時間統計
        if task_data['start_time'] and task_data['status'] == 'IN_PROGRESS':
            elapsed_time = (current_time - task_data['start_time']).total_seconds() / 60
            progress_info['timing_info']['elapsed_minutes'] = round(elapsed_time, 1)
            
            if task_data['estimated_completion']:
                remaining_time = (task_data['estimated_completion'] - current_time).total_seconds() / 60
                progress_info['timing_info']['remaining_minutes'] = max(0, round(remaining_time, 1))
        
        # 狀態歷史
        task_history = [
            snapshot for snapshot in self.state_history[SystemComponent.TASK]
            if task_id in snapshot.state_data
        ]
        
        if task_history:
            progress_info['status_history'] = [
                {
                    'timestamp': snapshot.timestamp,
                    'status': snapshot.state_data[task_id].get('status'),
                    'progress_percent': snapshot.state_data[task_id].get('progress_percent', 0)
                }
                for snapshot in task_history[-5:]  # 最近5個快照
            ]
        
        return progress_info
    
    def capture_system_snapshot(self, current_time: datetime) -> Dict[str, Any]:
        """捕獲完整的系統狀況快照"""
        snapshot = {
            'timestamp': current_time,
            'system_overview': self._get_system_overview(current_time),
            'performance_metrics': self._get_current_metrics(),
            'workstation_summary': self.track_station_status(current_time),
            'wave_summary': self._get_wave_summary(current_time),
            'exception_summary': self._get_exception_summary(current_time),
            'staff_summary': self._get_staff_summary(current_time),
            'recent_state_changes': list(self.state_changes)[-20:],  # 最近20個狀態變更
            'system_health': self._assess_system_health(current_time)
        }
        
        return snapshot
    
    def _get_system_overview(self, current_time: datetime) -> Dict[str, Any]:
        """取得系統概覽"""
        overview = {
            'timestamp': current_time,
            'total_workstations': len(self.workstation_manager.workstations),
            'total_tasks': len(self.workstation_manager.tasks),
            'active_waves': len(self.wave_manager.active_waves),
            'active_exceptions': len(self.exception_handler.active_exceptions),
            'system_uptime_minutes': 0,  # 需要從模擬開始時間計算
            'last_update': self.last_update_time
        }
        
        # 狀態統計
        overview['task_status_counts'] = defaultdict(int)
        for task_data in self.current_state[SystemComponent.TASK].values():
            overview['task_status_counts'][task_data['status']] += 1
        
        overview['workstation_status_counts'] = defaultdict(int)
        for station_data in self.current_state[SystemComponent.WORKSTATION].values():
            overview['workstation_status_counts'][station_data['status']] += 1
        
        return overview
    
    def _get_current_metrics(self) -> Dict[str, Any]:
        """取得當前性能指標"""
        if not self.metrics_history:
            return {}
        
        current_metrics = self.metrics_history[-1]
        return current_metrics.__dict__
    
    def _get_wave_summary(self, current_time: datetime) -> Dict[str, Any]:
        """取得波次摘要"""
        summary = {
            'active_waves_count': len(self.wave_manager.active_waves),
            'completed_waves_count': len(self.wave_manager.wave_history),
            'waves_by_priority': defaultdict(int),
            'waves_by_type': defaultdict(int),
            'average_progress': 0,
            'active_wave_details': []
        }
        
        if self.current_state[SystemComponent.WAVE]:
            total_progress = 0
            active_count = 0
            
            for wave_data in self.current_state[SystemComponent.WAVE].values():
                if not wave_data.get('is_completed', False):
                    priority = wave_data['priority_level']
                    wave_type = wave_data['wave_type']
                    
                    summary['waves_by_priority'][priority] += 1
                    summary['waves_by_type'][wave_type] += 1
                    
                    progress_info = wave_data.get('progress_info', {})
                    progress = progress_info.get('progress_percent', 0)
                    total_progress += progress
                    active_count += 1
                    
                    summary['active_wave_details'].append({
                        'wave_id': wave_data['wave_id'],
                        'type': wave_type,
                        'priority': priority,
                        'progress': progress,
                        'tasks': f"{wave_data['completed_tasks']}/{wave_data['total_tasks']}"
                    })
            
            if active_count > 0:
                summary['average_progress'] = round(total_progress / active_count, 1)
        
        return summary
    
    def _get_exception_summary(self, current_time: datetime) -> Dict[str, Any]:
        """取得異常摘要"""
        summary = {
            'active_exceptions_count': len(self.exception_handler.active_exceptions),
            'exceptions_by_type': defaultdict(int),
            'exceptions_by_priority': defaultdict(int),
            'exceptions_by_status': defaultdict(int),
            'average_handling_time': 0,
            'leader_utilization': 0,
            'active_exception_details': []
        }
        
        if self.current_state[SystemComponent.EXCEPTION]:
            handling_times = []
            
            for exc_data in self.current_state[SystemComponent.EXCEPTION].values():
                exc_type = exc_data['exception_type']
                priority = exc_data['priority']
                status = exc_data['status']
                
                summary['exceptions_by_type'][exc_type] += 1
                summary['exceptions_by_priority'][priority] += 1
                summary['exceptions_by_status'][status] += 1
                
                if exc_data.get('elapsed_time'):
                    handling_times.append(exc_data['elapsed_time'])
                
                summary['active_exception_details'].append({
                    'exception_id': exc_data['exception_id'],
                    'type': exc_type,
                    'priority': priority,
                    'status': status,
                    'assigned_leader': exc_data.get('assigned_leader'),
                    'progress': exc_data.get('progress_percent', 0)
                })
            
            if handling_times:
                summary['average_handling_time'] = round(np.mean(handling_times), 1)
        
        # 主管利用率
        total_leaders = len(self.exception_handler.available_leaders) + len(self.exception_handler.busy_leaders)
        if total_leaders > 0:
            summary['leader_utilization'] = round(len(self.exception_handler.busy_leaders) / total_leaders * 100, 1)
        
        return summary
    
    def _get_staff_summary(self, current_time: datetime) -> Dict[str, Any]:
        """取得員工摘要"""
        summary = {
            'total_staff': 0,
            'active_staff': 0,
            'idle_staff': 0,
            'staff_by_floor': defaultdict(int),
            'active_by_floor': defaultdict(int),
            'utilization_rate': 0,
            'leaders_available': len(self.exception_handler.available_leaders),
            'leaders_busy': len(self.exception_handler.busy_leaders)
        }
        
        if self.current_state[SystemComponent.STAFF]:
            for staff_data in self.current_state[SystemComponent.STAFF].values():
                if staff_data.get('role') != 'leader':  # 排除主管
                    summary['total_staff'] += 1
                    
                    floor = staff_data.get('floor', 'unknown')
                    summary['staff_by_floor'][floor] += 1
                    
                    if staff_data.get('is_busy', False):
                        summary['active_staff'] += 1
                        summary['active_by_floor'][floor] += 1
                    else:
                        summary['idle_staff'] += 1
            
            if summary['total_staff'] > 0:
                summary['utilization_rate'] = round(summary['active_staff'] / summary['total_staff'] * 100, 1)
        
        return summary
    
    def _assess_system_health(self, current_time: datetime) -> Dict[str, Any]:
        """評估系統健康狀況"""
        health = {
            'overall_status': 'HEALTHY',
            'score': 100,
            'issues': [],
            'warnings': [],
            'recommendations': []
        }
        
        # 檢查工作站利用率
        if self.metrics_history:
            current_metrics = self.metrics_history[-1]
            
            # 工作站利用率過低
            if current_metrics.workstation_utilization < 30:
                health['warnings'].append('工作站利用率偏低')
                health['score'] -= 10
            
            # 工作站利用率過高
            elif current_metrics.workstation_utilization > 90:
                health['warnings'].append('工作站利用率過高，可能造成瓶頸')
                health['score'] -= 15
            
            # 異常數量過多
            if current_metrics.exception_count > 5:
                health['issues'].append('活躍異常數量過多')
                health['score'] -= 20
                health['recommendations'].append('檢查異常處理流程')
            
            # 整體效率過低
            if current_metrics.overall_efficiency < 60:
                health['issues'].append('系統整體效率偏低')
                health['score'] -= 25
                health['recommendations'].append('分析效率瓶頸原因')
        
        # 檢查資源狀況
        total_leaders = len(self.exception_handler.available_leaders) + len(self.exception_handler.busy_leaders)
        if len(self.exception_handler.busy_leaders) / total_leaders > 0.8:
            health['warnings'].append('主管資源緊張')
            health['score'] -= 10
        
        # 根據分數決定整體狀態
        if health['score'] >= 80:
            health['overall_status'] = 'HEALTHY'
        elif health['score'] >= 60:
            health['overall_status'] = 'WARNING'
        else:
            health['overall_status'] = 'CRITICAL'
        
        return health
    
    def get_component_state(self, component_type: SystemComponent, component_id: str = None) -> Dict[str, Any]:
        """取得特定組件狀態"""
        if component_type not in self.current_state:
            return {'error': f'組件類型 {component_type.value} 不存在'}
        
        component_states = self.current_state[component_type]
        
        if component_id:
            if component_id in component_states:
                return component_states[component_id]
            else:
                return {'error': f'組件 {component_id} 不存在'}
        else:
            return component_states
    
    def get_state_history(self, component_type: SystemComponent, limit: int = 50) -> List[StateSnapshot]:
        """取得狀態歷史"""
        if component_type not in self.state_history:
            return []
        
        history = list(self.state_history[component_type])
        return history[-limit:] if limit else history
    
    def get_recent_state_changes(self, limit: int = 20) -> List[Dict]:
        """取得最近的狀態變更"""
        changes = list(self.state_changes)
        return changes[-limit:] if limit else changes
    
    def get_metrics_trend(self, metric_name: str, duration_minutes: int = 60) -> Dict[str, Any]:
        """取得指標趨勢"""
        if not self.metrics_history:
            return {'error': '沒有指標歷史資料'}
        
        cutoff_time = datetime.now() - timedelta(minutes=duration_minutes)
        recent_metrics = [
            metrics for metrics in self.metrics_history
            if metrics.timestamp >= cutoff_time
        ]
        
        if not recent_metrics:
            return {'error': f'沒有最近 {duration_minutes} 分鐘的資料'}
        
        if not hasattr(recent_metrics[0], metric_name):
            return {'error': f'指標 {metric_name} 不存在'}
        
        values = [getattr(metrics, metric_name) for metrics in recent_metrics]
        timestamps = [metrics.timestamp for metrics in recent_metrics]
        
        trend_analysis = {
            'metric_name': metric_name,
            'duration_minutes': duration_minutes,
            'data_points': len(values),
            'current_value': values[-1] if values else 0,
            'min_value': min(values) if values else 0,
            'max_value': max(values) if values else 0,
            'avg_value': round(np.mean(values), 2) if values else 0,
            'trend_direction': 'stable',
            'values': list(zip(timestamps, values))
        }
        
        # 分析趨勢方向
        if len(values) >= 3:
            first_half = values[:len(values)//2]
            second_half = values[len(values)//2:]
            
            first_avg = np.mean(first_half)
            second_avg = np.mean(second_half)
            
            if second_avg > first_avg * 1.1:
                trend_analysis['trend_direction'] = 'increasing'
            elif second_avg < first_avg * 0.9:
                trend_analysis['trend_direction'] = 'decreasing'
        
        return trend_analysis
    
    def enable_tracking(self):
        """啟用狀態追蹤"""
        self.tracking_enabled = True
        self.logger.info("系統狀態追蹤已啟用")
    
    def disable_tracking(self):
        """停用狀態追蹤"""
        self.tracking_enabled = False
        self.logger.info("系統狀態追蹤已停用")
    
    def reset_tracking_data(self):
        """重置追蹤資料"""
        for component_history in self.state_history.values():
            component_history.clear()
        
        for component_state in self.current_state.values():
            component_state.clear()
        
        self.metrics_history.clear()
        self.state_changes.clear()
        
        self.last_update_time = None
        self.last_snapshot_time = None
        
        self.logger.info("系統狀態追蹤資料已重置")
    
    def export_tracking_data(self, start_time: datetime = None, end_time: datetime = None) -> Dict[str, pd.DataFrame]:
        """匯出追蹤資料為DataFrame"""
        export_data = {}
        
        # 匯出指標歷史
        if self.metrics_history:
            metrics_records = []
            for metrics in self.metrics_history:
                if start_time and metrics.timestamp < start_time:
                    continue
                if end_time and metrics.timestamp > end_time:
                    continue
                
                metrics_records.append(metrics.__dict__)
            
            if metrics_records:
                export_data['metrics'] = pd.DataFrame(metrics_records)
        
        # 匯出狀態變更
        if self.state_changes:
            change_records = []
            for change in self.state_changes:
                if start_time and change['timestamp'] < start_time:
                    continue
                if end_time and change['timestamp'] > end_time:
                    continue
                
                # 展開變更記錄
                for field, change_detail in change['changes'].items():
                    change_records.append({
                        'timestamp': change['timestamp'],
                        'component_type': change['component_type'],
                        'component_id': change['component_id'],
                        'field': field,
                        'old_value': change_detail['old'],
                        'new_value': change_detail['new']
                    })
            
            if change_records:
                export_data['state_changes'] = pd.DataFrame(change_records)
        
        return export_data