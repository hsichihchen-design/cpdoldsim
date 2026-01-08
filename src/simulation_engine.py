"""
SimulationEngine - 模擬執行引擎 (修改版：支援進貨任務和加班邏輯)
負責執行離散事件模擬，整合所有模組
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import heapq
import uuid
from collections import defaultdict
import time

class EventType(Enum):
    """🔧 修改：事件類型枚舉（新增進貨和加班相關）"""
    SIMULATION_START = "SIMULATION_START"
    SIMULATION_END = "SIMULATION_END"
    
    # 任務相關事件
    TASK_START = "TASK_START"
    TASK_COMPLETE = "TASK_COMPLETE"
    TASK_ASSIGN = "TASK_ASSIGN"
    
    # 🆕 新增：進貨相關事件
    RECEIVING_LOAD = "RECEIVING_LOAD"           # 載入進貨資料
    RECEIVING_TASK_ASSIGN = "RECEIVING_TASK_ASSIGN"  # 分配進貨任務
    RECEIVING_DEADLINE_CHECK = "RECEIVING_DEADLINE_CHECK"  # 檢查進貨期限
    
    # 🆕 新增：加班相關事件
    OVERTIME_EVALUATION = "OVERTIME_EVALUATION"  # 評估加班需求
    OVERTIME_START = "OVERTIME_START"           # 開始加班
    OVERTIME_END = "OVERTIME_END"               # 結束加班
    
    # 工作站相關事件
    STATION_STARTUP_COMPLETE = "STATION_STARTUP_COMPLETE"
    STATION_BECOME_IDLE = "STATION_BECOME_IDLE"
    
    # 波次相關事件
    WAVE_START = "WAVE_START"
    WAVE_COMPLETE = "WAVE_COMPLETE"
    
    # 異常相關事件
    EXCEPTION_DETECTED = "EXCEPTION_DETECTED"
    EXCEPTION_RESOLVED = "EXCEPTION_RESOLVED"
    
    # 系統相關事件
    SYSTEM_STATUS_UPDATE = "SYSTEM_STATUS_UPDATE"
    DAILY_SCHEDULE_GENERATE = "DAILY_SCHEDULE_GENERATE"
    
    # 🆕 新增：日終處理事件
    END_OF_DAY_PROCESSING = "END_OF_DAY_PROCESSING"
    
    # 自定義事件
    CUSTOM_EVENT = "CUSTOM_EVENT"

class SimulationState(Enum):
    """模擬狀態枚舉"""
    INITIALIZED = "INITIALIZED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"

@dataclass
class SimulationEvent:
    """模擬事件物件"""
    event_id: str
    event_type: EventType
    scheduled_time: datetime
    priority: int = 0  # 0 = 最高優先權
    event_data: Dict[str, Any] = field(default_factory=dict)
    handler_function: Optional[Callable] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __lt__(self, other):
        """用於優先隊列排序"""
        if self.scheduled_time != other.scheduled_time:
            return self.scheduled_time < other.scheduled_time
        return self.priority < other.priority

@dataclass 
class SimulationConfig:
    """模擬配置"""
    start_date: str
    end_date: str
    time_step_seconds: int = 60  # 時間步長（秒）
    random_seed: Optional[int] = None
    
    # 模擬控制參數
    max_events_per_step: int = 100
    status_update_interval: int = 300  # 狀態更新間隔（秒）
    snapshot_interval: int = 600  # 快照間隔（秒）
    
    # 🆕 新增：進貨和加班相關參數
    enable_receiving_simulation: bool = True
    enable_overtime_simulation: bool = True
    overtime_evaluation_interval: int = 3600  # 加班評估間隔（秒）
    
    # 性能參數
    enable_detailed_logging: bool = True
    enable_progress_tracking: bool = True
    max_simulation_duration_hours: int = 72  # 最大模擬時間
    
    # 驗證參數
    validate_events: bool = True
    check_consistency: bool = True

@dataclass
class SimulationResults:
    """🔧 修改：模擬結果（新增進貨和加班統計）"""
    simulation_id: str
    config: SimulationConfig
    
    # 時間資訊
    start_time: datetime
    end_time: Optional[datetime] = None
    simulation_duration_seconds: Optional[float] = None
    simulated_time_range: Optional[Tuple[datetime, datetime]] = None
    
    # 事件統計
    total_events_processed: int = 0
    events_by_type: Dict[str, int] = field(default_factory=dict)
    
    # 性能指標
    final_metrics: Optional[Dict] = None
    peak_workstation_utilization: float = 0.0
    total_tasks_completed: int = 0
    total_waves_completed: int = 0
    total_exceptions_handled: int = 0
    
    # 🆕 新增：進貨和加班統計
    total_shipping_tasks: int = 0
    total_receiving_tasks: int = 0
    completed_shipping_tasks: int = 0
    completed_receiving_tasks: int = 0
    overdue_receiving_tasks: int = 0
    overtime_sessions: int = 0
    total_overtime_hours: float = 0.0
    
    # 錯誤和警告
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # 詳細結果
    detailed_logs: List[Dict] = field(default_factory=list)
    performance_timeline: List[Dict] = field(default_factory=list)

class SimulationEngine:
    def __init__(self, data_manager, staff_schedule_generator, order_priority_manager, 
                 workstation_task_manager, wave_manager, exception_handler, system_state_tracker,
                 receiving_manager=None):  # 🆕 新增進貨管理器
        """🔧 修改：初始化模擬執行引擎（新增進貨管理器）"""
        self.logger = logging.getLogger(__name__)
        
        # 關聯的管理器
        self.data_manager = data_manager
        self.staff_schedule_generator = staff_schedule_generator
        self.order_priority_manager = order_priority_manager
        self.workstation_task_manager = workstation_task_manager
        self.wave_manager = wave_manager
        self.exception_handler = exception_handler
        self.system_state_tracker = system_state_tracker
        self.receiving_manager = receiving_manager  # 🆕 新增
        
        # 模擬狀態
        self.simulation_state = SimulationState.INITIALIZED
        self.current_simulation_time: Optional[datetime] = None
        self.simulation_config: Optional[SimulationConfig] = None
        self.simulation_results: Optional[SimulationResults] = None
        
        # 事件隊列（優先隊列）
        self.event_queue: List[SimulationEvent] = []
        self.processed_events: List[SimulationEvent] = []
        
        # 事件處理器映射
        self.event_handlers: Dict[EventType, Callable] = {}
        self._register_default_event_handlers()
        
        # 統計追蹤
        self.event_statistics = defaultdict(int)
        self.performance_samples = []
        
        # 🆕 新增：進貨和加班相關追蹤
        self.daily_receiving_data: Dict[str, pd.DataFrame] = {}  # 按日期存放進貨資料
        self.overtime_sessions: List[Dict] = []  # 加班時段記錄
        self.current_overtime_schedule: Optional[pd.DataFrame] = None
        
        # 模擬控制
        self.should_continue = True
        self.pause_requested = False
        self.step_mode = False

        self.workstation_task_manager.wave_manager = wave_manager

        # 🆕 讓 WorkstationTaskManager 能存取 WaveManager
        self.logger.info("SimulationEngine 初始化完成")
    
    def _register_default_event_handlers(self):
        """🔧 修改：註冊預設事件處理器（新增進貨和加班處理器）"""
        self.event_handlers = {
            EventType.SIMULATION_START: self._handle_simulation_start,
            EventType.SIMULATION_END: self._handle_simulation_end,
            EventType.TASK_START: self._handle_task_start,
            EventType.TASK_COMPLETE: self._handle_task_complete,
            EventType.TASK_ASSIGN: self._handle_task_assign,
            
            # 🆕 新增：進貨相關處理器
            EventType.RECEIVING_LOAD: self._handle_receiving_load,
            EventType.RECEIVING_TASK_ASSIGN: self._handle_receiving_task_assign,
            EventType.RECEIVING_DEADLINE_CHECK: self._handle_receiving_deadline_check,
            
            # 🆕 新增：加班相關處理器
            EventType.OVERTIME_EVALUATION: self._handle_overtime_evaluation,
            EventType.OVERTIME_START: self._handle_overtime_start,
            EventType.OVERTIME_END: self._handle_overtime_end,
            
            EventType.STATION_STARTUP_COMPLETE: self._handle_station_startup_complete,
            EventType.STATION_BECOME_IDLE: self._handle_station_become_idle,
            EventType.WAVE_START: self._handle_wave_start,
            EventType.WAVE_COMPLETE: self._handle_wave_complete,
            EventType.EXCEPTION_DETECTED: self._handle_exception_detected,
            EventType.EXCEPTION_RESOLVED: self._handle_exception_resolved,
            EventType.SYSTEM_STATUS_UPDATE: self._handle_system_status_update,
            EventType.DAILY_SCHEDULE_GENERATE: self._handle_daily_schedule_generate,
            EventType.END_OF_DAY_PROCESSING: self._handle_end_of_day_processing,  # 🆕
            EventType.CUSTOM_EVENT: self._handle_custom_event
        }
    
    def _create_initial_events(self, start_time: datetime, end_time: datetime):
        """🔧 修改：建立初始事件（新增進貨和加班相關事件）"""
        # 模擬開始事件
        self._schedule_event(
            EventType.SIMULATION_START,
            start_time,
            priority=0,
            event_data={'message': '模擬開始'}
        )
        
        # 模擬結束事件
        self._schedule_event(
            EventType.SIMULATION_END,
            end_time,
            priority=0,
            event_data={'message': '模擬結束'}
        )
        
        # 定期系統狀態更新事件
        current_time = start_time
        status_interval = timedelta(seconds=self.simulation_config.status_update_interval)
        
        while current_time < end_time:
            self._schedule_event(
                EventType.SYSTEM_STATUS_UPDATE,
                current_time,
                priority=10,
                event_data={'update_type': 'periodic'}
            )
            current_time += status_interval
        
        # 每日排班生成事件
        current_date = start_time.date()
        end_date = end_time.date()
        
        while current_date <= end_date:
            
            # 🆕 跳過週末
            if not self.data_manager.is_workday(current_date):
                current_date += timedelta(days=1)
                continue
            
            # 每日6點生成排班
            schedule_time = datetime.combine(current_date, datetime.min.time().replace(hour=6))
            
            if schedule_time >= start_time:
                self._schedule_event(
                    EventType.DAILY_SCHEDULE_GENERATE,
                    schedule_time,
                    priority=5,
                    event_data={'date': current_date.strftime('%Y-%m-%d')}
                )
            
            # 🆕 新增：每日8點載入進貨資料
            if self.simulation_config.enable_receiving_simulation and self.receiving_manager:
                receiving_time = datetime.combine(current_date, datetime.min.time().replace(hour=8))
                if receiving_time >= start_time:
                    self._schedule_event(
                        EventType.RECEIVING_LOAD,
                        receiving_time,
                        priority=3,
                        event_data={'date': current_date.strftime('%Y-%m-%d')}
                    )
            
            # 🆕 新增：每日多次進貨期限檢查
            for hour in [10, 14, 16]:  # 每天檢查3次
                check_time = datetime.combine(current_date, datetime.min.time().replace(hour=hour))
                if check_time >= start_time:
                    self._schedule_event(
                        EventType.RECEIVING_DEADLINE_CHECK,
                        check_time,
                        priority=6,
                        event_data={'date': current_date.strftime('%Y-%m-%d')}
                    )
            
            # 🆕 新增：日終處理事件（17:00）
            end_of_day_time = datetime.combine(current_date, datetime.min.time().replace(hour=17))
            if end_of_day_time >= start_time:
                self._schedule_event(
                    EventType.END_OF_DAY_PROCESSING,
                    end_of_day_time,
                    priority=2,
                    event_data={'date': current_date.strftime('%Y-%m-%d')}
                )
            
            current_date += timedelta(days=1)
        
        # 🆕 新增：定期加班評估事件
        if self.simulation_config.enable_overtime_simulation:
            current_time = start_time
            overtime_interval = timedelta(seconds=self.simulation_config.overtime_evaluation_interval)
            
            while current_time < end_time:
                # 只在工作時間內進行加班評估
                if 8 <= current_time.hour <= 20:  # 上午8點到晚上8點
                    self._schedule_event(
                        EventType.OVERTIME_EVALUATION,
                        current_time,
                        priority=7,
                        event_data={'evaluation_type': 'periodic'}
                    )
                current_time += overtime_interval
    
    # ===================
    # 🆕 新增：進貨相關事件處理器
    # ===================
    
    def _handle_receiving_load(self, event: SimulationEvent, current_time: datetime):
        """🆕 處理載入進貨資料事件"""
        date_str = event.event_data.get('date')
        current_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        
        self.logger.info(f"📦 載入 {date_str} 的進貨資料...")
        
        # 從transaction data中載入進貨資料
        if 'historical_receiving' in self.data_manager.transaction_data:
            receiving_df = self.data_manager.transaction_data['historical_receiving']
            
            # 篩選當日到貨的進貨資料
            if 'DATE' in receiving_df.columns:
                daily_receiving = receiving_df[
                    pd.to_datetime(receiving_df['DATE']).dt.date == current_date
                ]
                
                if len(daily_receiving) > 0:
                    # 使用進貨管理器處理
                    processed_receiving = self.receiving_manager.process_receiving_batch(
                        daily_receiving, current_date
                    )
                    
                    # 存放當日進貨資料
                    self.daily_receiving_data[date_str] = processed_receiving
                    
                    # 建立進貨任務
                    receiving_tasks = self.workstation_task_manager.create_tasks_from_receiving(
                        processed_receiving, current_date
                    )
                    
                    # 排程進貨任務分配
                    assign_time = current_time + timedelta(minutes=30)  # 30分鐘後分配
                    self._schedule_event(
                        EventType.RECEIVING_TASK_ASSIGN,
                        assign_time,
                        priority=4,
                        event_data={
                            'date': date_str,
                            'task_count': len(receiving_tasks)
                        }
                    )
                    
                    self.logger.info(f"✅ 載入 {len(daily_receiving)} 筆進貨資料，建立 {len(receiving_tasks)} 個任務")
                else:
                    self.logger.info(f"📦 {date_str} 無進貨資料")
        else:
            self.logger.warning("找不到 historical_receiving 資料")
    
    def _handle_receiving_task_assign(self, event: SimulationEvent, current_time: datetime):
        """🆕 處理分配進貨任務事件"""
        date_str = event.event_data.get('date')
        task_count = event.event_data.get('task_count', 0)
        
        self.logger.info(f"📋 開始分配 {date_str} 的 {task_count} 個進貨任務...")
        
        # 取得當日的進貨任務
        receiving_tasks = [
            task for task in self.workstation_task_manager.tasks.values()
            if (task.task_type.value == 'RECEIVING' and 
                task.status.value == 'PENDING' and
                task.arrival_date and 
                task.arrival_date.strftime('%Y-%m-%d') == date_str)
        ]
        
        if receiving_tasks:
            # 取得當日員工排班
            current_schedule = self._get_current_staff_schedule(current_time)
            
            if current_schedule is not None and len(current_schedule) > 0:
                # 分配進貨任務到工作站
                assignment_result = self.workstation_task_manager.assign_tasks_to_stations(
                    receiving_tasks, current_schedule, current_time
                )
                
                # 為已分配的任務安排開始時間
                for task_id in assignment_result['assigned']:
                    start_time = current_time + timedelta(minutes=random.randint(10, 60))
                    self._schedule_event(
                        EventType.TASK_START,
                        start_time,
                        priority=5,
                        event_data={'task_id': task_id, 'task_type': 'RECEIVING'}
                    )
                
                self.logger.info(f"✅ 進貨任務分配完成: 已分配 {len(assignment_result['assigned'])}, 需加班 {len(assignment_result.get('overtime_required', []))}")
                
                # 處理需要加班的任務
                if assignment_result.get('overtime_required'):
                    self._handle_overtime_requirements(assignment_result['overtime_required'], current_time)
            else:
                self.logger.warning(f"找不到 {date_str} 的員工排班資料")
    
    def _handle_receiving_deadline_check(self, event: SimulationEvent, current_time: datetime):
        """🆕 處理進貨期限檢查事件"""
        date_str = event.event_data.get('date')
        current_date = current_time.date()
        
        # 檢查所有進貨任務的期限狀況
        overdue_tasks = self.workstation_task_manager.get_overdue_receiving_tasks(current_date)
        due_today_tasks = self.workstation_task_manager.get_due_today_tasks(current_date)
        
        if overdue_tasks:
            self.logger.warning(f"🚨 發現 {len(overdue_tasks)} 個逾期進貨任務")
            
            # 立即安排加班處理逾期任務
            self._schedule_immediate_overtime_for_tasks(overdue_tasks, current_time, "逾期進貨")
        
        if due_today_tasks:
            self.logger.info(f"⏰ 今天有 {len(due_today_tasks)} 個進貨任務截止")
            
            # 檢查是否需要安排加班
            incomplete_due_today = [
                task for task in due_today_tasks 
                if task.status.value not in ['COMPLETED', 'IN_PROGRESS']
            ]
            
            if incomplete_due_today and current_time.hour >= 15:  # 下午3點後檢查
                self._schedule_immediate_overtime_for_tasks(incomplete_due_today, current_time, "今日截止進貨")
    
    # ===================
    # 🆕 新增：加班相關事件處理器
    # ===================
    
    def _handle_overtime_evaluation(self, event: SimulationEvent, current_time: datetime):
        """🆕 處理加班評估事件"""
        self.logger.debug(f"🕒 進行加班評估: {current_time}")
        
        # 取得需要加班的任務
        overtime_tasks = self.workstation_task_manager.get_tasks_requiring_overtime(current_time)
        
        if overtime_tasks:
            self.logger.info(f"🕒 發現 {len(overtime_tasks)} 個任務需要加班")
            
            # 計算加班需求
            overtime_requirements = self.staff_schedule_generator.calculate_overtime_requirements(
                overtime_tasks, current_time
            )
            
            if overtime_requirements:
                # 安排加班
                self._schedule_overtime_session(overtime_requirements, current_time)
    
    def _handle_overtime_start(self, event: SimulationEvent, current_time: datetime):
        """🆕 處理開始加班事件"""
        overtime_info = event.event_data.get('overtime_info', {})
        session_id = event.event_data.get('session_id')
        
        self.logger.info(f"🕐 開始加班時段: {session_id}")
        
        # 生成加班排班
        base_schedule = self._get_current_staff_schedule(current_time)
        if base_schedule is not None:
            overtime_schedule = self.staff_schedule_generator.generate_overtime_schedule(
                base_schedule, overtime_info
            )
            
            self.current_overtime_schedule = overtime_schedule
            
            # 創建加班任務
            overtime_tasks = self.workstation_task_manager.create_overtime_tasks(overtime_info)
            
            # 為加班任務安排開始時間
            for task in overtime_tasks:
                start_time = current_time + timedelta(minutes=5)  # 5分鐘後開始
                self._schedule_event(
                    EventType.TASK_START,
                    start_time,
                    priority=1,  # 高優先權
                    event_data={'task_id': task.task_id, 'task_type': 'OVERTIME'}
                )
            
            # 計算加班結束時間
            max_overtime_hours = max(
                req.get('required_hours', 1.0) for req in overtime_info.values()
            )
            overtime_end_time = current_time + timedelta(hours=max_overtime_hours)
            
            # 排程加班結束事件
            self._schedule_event(
                EventType.OVERTIME_END,
                overtime_end_time,
                priority=3,
                event_data={'session_id': session_id}
            )
            
            # 記錄加班時段
            overtime_session = {
                'session_id': session_id,
                'start_time': current_time,
                'end_time': overtime_end_time,
                'stations': list(overtime_info.keys()),
                'total_hours': max_overtime_hours,
                'reason': overtime_info.get(list(overtime_info.keys())[0], {}).get('reason', 'unknown')
            }
            
            self.overtime_sessions.append(overtime_session)
            
            self.logger.info(f"✅ 加班安排完成: {len(overtime_tasks)} 個任務，預計 {max_overtime_hours:.1f} 小時")
    
    def _handle_overtime_end(self, event: SimulationEvent, current_time: datetime):
        """🆕 處理結束加班事件"""
        session_id = event.event_data.get('session_id')
        
        self.logger.info(f"🕕 結束加班時段: {session_id}")
        
        # 清空加班排班
        self.current_overtime_schedule = None
        
        # 更新加班記錄
        for session in self.overtime_sessions:
            if session['session_id'] == session_id:
                session['actual_end_time'] = current_time
                actual_duration = (current_time - session['start_time']).total_seconds() / 3600
                session['actual_hours'] = round(actual_duration, 1)
                break
        
        # 檢查未完成的加班任務
        incomplete_overtime_tasks = [
            task for task in self.workstation_task_manager.overtime_tasks.values()
            if task.status.value not in ['COMPLETED', 'CANCELLED']
        ]
        
        if incomplete_overtime_tasks:
            self.logger.warning(f"⚠️ 加班結束時仍有 {len(incomplete_overtime_tasks)} 個任務未完成")
            
            # 強制完成或取消未完成的加班任務
            for task in incomplete_overtime_tasks:
                if task.status.value == 'IN_PROGRESS':
                    # 正在進行的任務強制完成
                    self.workstation_task_manager.complete_task(task.task_id, current_time)
                    self.logger.info(f"🚧 強制完成加班任務: {task.task_id}")
                else:
                    # 其他任務取消
                    task.status = task.status.CANCELLED
                    self.logger.info(f"❌ 取消未開始的加班任務: {task.task_id}")
    
    def _handle_end_of_day_processing(self, event: SimulationEvent, current_time: datetime):
        """🆕 處理日終處理事件"""
        date_str = event.event_data.get('date')
        
        self.logger.info(f"🏁 執行 {date_str} 日終處理...")
        
        # 檢查未完成的副倉庫出貨任務
        incomplete_sub_warehouse = [
            task for task in self.workstation_task_manager.tasks.values()
            if (task.task_type.value == 'SHIPPING' and
                task.route_code in ['SDTC', 'SDHN'] and
                task.status.value not in ['COMPLETED', 'CANCELLED'])
        ]
        
        if incomplete_sub_warehouse:
            self.logger.warning(f"🚨 發現 {len(incomplete_sub_warehouse)} 個副倉庫出貨任務未完成，需要加班")
            self._schedule_immediate_overtime_for_tasks(incomplete_sub_warehouse, current_time, "副倉庫出貨")
        
        # 檢查今天截止的進貨任務
        current_date = current_time.date()
        due_today_receiving = [
            task for task in self.workstation_task_manager.tasks.values()
            if (task.task_type.value == 'RECEIVING' and
                task.deadline_date == current_date and
                task.status.value not in ['COMPLETED', 'CANCELLED'])
        ]
        
        if due_today_receiving:
            self.logger.warning(f"🚨 發現 {len(due_today_receiving)} 個進貨任務今天截止未完成，需要加班")
            self._schedule_immediate_overtime_for_tasks(due_today_receiving, current_time, "進貨期限")
        
        # 統計當日完成情況
        daily_summary = self._generate_daily_summary(current_time.date())
        self.logger.info(f"📊 {date_str} 當日總結: {daily_summary}")
    
    # ===================
    # 🔧 修改：原有事件處理器
    # ===================
    
    def _handle_simulation_start(self, event: SimulationEvent, current_time: datetime):
        """🔧 修改：處理模擬開始事件（支援進貨載入）"""
        self.logger.info(f"🎯 模擬開始: {current_time}")
        
        # 載入交易資料
        start_date = self.simulation_config.start_date
        end_date = self.simulation_config.end_date
        
        transaction_data = self.data_manager.load_transaction_data(start_date, end_date)
        
        # 處理出貨訂單
        if 'historical_orders' in transaction_data:
            orders_df = transaction_data['historical_orders']
            
            # 處理訂單優先權
            processed_orders = self.order_priority_manager.process_orders_batch(orders_df)
            
            # 建立出貨任務
            shipping_tasks = self.workstation_task_manager.create_tasks_from_orders(processed_orders)
            
            # 為每個出貨任務安排處理時間
            self._schedule_task_processing(shipping_tasks, current_time, 'SHIPPING')
            
            self.logger.info(f"📦 載入 {len(processed_orders)} 筆出貨訂單，建立 {len(shipping_tasks)} 個出貨任務")
        
        # 🆕 新增：進貨資料預載入統計
        if 'historical_receiving' in transaction_data and self.receiving_manager:
            receiving_df = transaction_data['historical_receiving']
            self.logger.info(f"📋 載入 {len(receiving_df)} 筆進貨資料，將按日期自動處理")
    
    def _handle_task_start(self, event: SimulationEvent, current_time: datetime):
        """🔧 修改：處理任務開始事件（支援任務類型）"""
        task_id = event.event_data.get('task_id')
        task_type = event.event_data.get('task_type', 'SHIPPING')
        
        # 從正常任務或加班任務中查找
        task = None
        if task_id in self.workstation_task_manager.tasks:
            task = self.workstation_task_manager.tasks[task_id]
        elif task_id in self.workstation_task_manager.overtime_tasks:
            task = self.workstation_task_manager.overtime_tasks[task_id]
        
        if task:
            # 檢查異常
            exceptions = self.exception_handler.detect_exceptions(
                current_time, 
                context={'tasks': [task]}
            )
            
            if exceptions:
                for exception in exceptions:
                    self._schedule_event(
                        EventType.EXCEPTION_DETECTED,
                        current_time,
                        priority=1,
                        event_data={'exception_id': exception.exception_id}
                    )
            else:
                # 正常開始任務
                self._start_task_execution(task, current_time)
        else:
            self.logger.warning(f"找不到任務: {task_id}")
    
    def _handle_task_complete(self, event: SimulationEvent, current_time: datetime):
            """🔧 修改：處理任務完成事件（檢查波次完成）"""
            task_id = event.event_data.get('task_id')
            
            if task_id:
                # 完成任務
                success = False
                task = None
                
                if task_id in self.workstation_task_manager.tasks:
                    success = self.workstation_task_manager.complete_task(task_id, current_time)
                    task = self.workstation_task_manager.tasks[task_id]
                elif task_id in self.workstation_task_manager.overtime_tasks:
                    task = self.workstation_task_manager.overtime_tasks[task_id]
                    task.status = task.status.COMPLETED
                    task.actual_completion = current_time
                    success = True
                
                if success and task:
                    # 更新統計
                    if task.task_type.value == 'SHIPPING':
                        self.simulation_results.completed_shipping_tasks += 1
                    elif task.task_type.value == 'RECEIVING':
                        self.simulation_results.completed_receiving_tasks += 1
                    
                    # 🆕 檢查是否需要檢查波次完成
                    if hasattr(task, 'assigned_wave') and task.assigned_wave:
                        # 延遲檢查波次完成（讓其他同波次任務有機會完成）
                        self._schedule_event(
                            EventType.CUSTOM_EVENT,
                            current_time + timedelta(seconds=1),
                            priority=2,
                            event_data={
                                'event_type': 'wave_completion_check',
                                'wave_id': task.assigned_wave
                            },
                            handler_function=self._handle_wave_completion_check
                        )
                    
                    # 工作站變為可用
                    if task.assigned_station:
                        self._schedule_event(
                            EventType.STATION_BECOME_IDLE,
                            current_time,
                            priority=4,
                            event_data={'station_id': task.assigned_station}
                        )
                    
                    task_type_str = "進貨" if task.task_type.value == 'RECEIVING' else "出貨"
                    self.logger.info(f"✅ {task_type_str}任務 {task_id} 完成")
    
    def _handle_simulation_end(self, event: SimulationEvent, current_time: datetime):
        """處理模擬結束事件"""
        self.logger.info(f"🏁 模擬結束: {current_time}")
        
        # 停止模擬
        self.should_continue = False
        
        # 計算最終統計
        self._calculate_final_statistics()
        
        # 更新模擬結果
        if self.simulation_results:
            self.simulation_results.end_time = datetime.now()
            self.simulation_results.simulation_duration_seconds = (
                self.simulation_results.end_time - self.simulation_results.start_time
            ).total_seconds()
        
        self.logger.info("模擬結束事件處理完成")

    # ===================
    # 🆕 新增：輔助方法
    # ===================
    
    def _get_current_staff_schedule(self, current_time: datetime) -> Optional[pd.DataFrame]:
        """取得當前的員工排班"""
        date_str = current_time.strftime('%Y-%m-%d')
        
        try:
            # 生成當日排班（如果尚未生成）
            daily_schedule = self.staff_schedule_generator.generate_daily_schedule(date_str)
            return daily_schedule
        except Exception as e:
            self.logger.error(f"取得員工排班失敗: {str(e)}")
            return None
    
    def _schedule_task_processing(self, tasks: List, current_time: datetime, task_type: str):
        """🔧 修改：排程任務處理（支援任務類型）"""
        for task in tasks:
            # 基於任務優先權決定處理時間
            if task.priority_level == 'P1':
                delay_minutes = random.randint(5, 15)  # 緊急任務快速處理
            elif task.priority_level == 'P2':
                delay_minutes = random.randint(15, 45)  # 一般任務
            else:
                delay_minutes = random.randint(30, 90)  # 低優先權任務
            
            process_time = current_time + timedelta(minutes=delay_minutes)
            
            self._schedule_event(
                EventType.TASK_ASSIGN,
                process_time,
                priority=4,
                event_data={'task_id': task.task_id, 'task_type': task_type}
            )
    
    def _handle_overtime_requirements(self, task_ids: List[str], current_time: datetime):
        """🆕 處理加班需求"""
        if not task_ids:
            return
        
        self.logger.info(f"🕒 處理 {len(task_ids)} 個任務的加班需求...")
        
        # 收集需要加班的任務
        overtime_tasks = []
        for task_id in task_ids:
            if task_id in self.workstation_task_manager.tasks:
                overtime_tasks.append(self.workstation_task_manager.tasks[task_id])
        
        if overtime_tasks:
            # 計算加班需求
            overtime_requirements = self.staff_schedule_generator.calculate_overtime_requirements(
                overtime_tasks, current_time
            )
            
            if overtime_requirements:
                # 立即安排加班（加班開始時間為當前時間+10分鐘）
                overtime_start_time = current_time + timedelta(minutes=10)
                self._schedule_overtime_session(overtime_requirements, overtime_start_time)
    
    def _schedule_immediate_overtime_for_tasks(self, tasks: List, current_time: datetime, reason: str):
        """🆕 為指定任務立即安排加班"""
        if not tasks:
            return
        
        self.logger.warning(f"🚨 立即安排加班處理 {len(tasks)} 個任務（原因: {reason}）")
        
        # 計算加班需求
        overtime_requirements = {}
        for task in tasks:
            if task.assigned_station:
                station_id = task.assigned_station
            else:
                # 為未分配工作站的任務找一個適合的工作站
                floor_stations = [
                    s.station_id for s in self.workstation_task_manager.workstations.values()
                    if s.floor == task.floor and not s.reserved_for_exception
                ]
                station_id = floor_stations[0] if floor_stations else f"ST{task.floor}F01"
            
            overtime_requirements[station_id] = {
                'task_id': task.task_id,
                'required_hours': max(1.0, task.estimated_duration / 60),
                'reason': reason,
                'current_hours': 8.0
            }
        
        # 立即開始加班
        self._schedule_overtime_session(overtime_requirements, current_time)
    
    def _schedule_overtime_session(self, overtime_requirements: Dict, start_time: datetime):
        """🆕 排程加班時段"""
        session_id = f"OT_{start_time.strftime('%Y%m%d_%H%M')}_{uuid.uuid4().hex[:8]}"
        
        self._schedule_event(
            EventType.OVERTIME_START,
            start_time,
            priority=1,
            event_data={
                'session_id': session_id,
                'overtime_info': overtime_requirements
            }
        )
    
    def _generate_daily_summary(self, target_date: date) -> Dict:
        """🆕 生成當日總結"""
        
        # 統計當日任務
        daily_shipping = [
            task for task in self.workstation_task_manager.tasks.values()
            if task.task_type.value == 'SHIPPING'
        ]
        
        daily_receiving = [
            task for task in self.workstation_task_manager.tasks.values()
            if (task.task_type.value == 'RECEIVING' and 
                task.arrival_date == target_date)
        ]
        
        # 統計完成情況
        completed_shipping = sum(1 for task in daily_shipping if task.status.value == 'COMPLETED')
        completed_receiving = sum(1 for task in daily_receiving if task.status.value == 'COMPLETED')
        
        # 統計加班情況
        daily_overtime = [
            session for session in self.overtime_sessions
            if session['start_time'].date() == target_date
        ]
        
        total_overtime_hours = sum(session.get('actual_hours', session.get('total_hours', 0)) 
                                  for session in daily_overtime)
        
        return {
            'date': target_date.strftime('%Y-%m-%d'),
            'shipping_tasks': {'total': len(daily_shipping), 'completed': completed_shipping},
            'receiving_tasks': {'total': len(daily_receiving), 'completed': completed_receiving},
            'overtime_sessions': len(daily_overtime),
            'total_overtime_hours': round(total_overtime_hours, 1)
        }
    
    def _calculate_final_statistics(self):
        """🔧 修改：計算最終統計數據（新增進貨和加班統計）"""
        if not self.simulation_results:
            return
        
        # 工作站利用率峰值
        if self.system_state_tracker.metrics_history:
            utilizations = [m.workstation_utilization for m in self.system_state_tracker.metrics_history]
            self.simulation_results.peak_workstation_utilization = max(utilizations) if utilizations else 0
        
        # 任務完成統計
        shipping_tasks = [task for task in self.workstation_task_manager.tasks.values() 
                         if task.task_type.value == 'SHIPPING']
        receiving_tasks = [task for task in self.workstation_task_manager.tasks.values() 
                          if task.task_type.value == 'RECEIVING']
        
        self.simulation_results.total_shipping_tasks = len(shipping_tasks)
        self.simulation_results.total_receiving_tasks = len(receiving_tasks)
        
        self.simulation_results.completed_shipping_tasks = sum(
            1 for task in shipping_tasks if task.status.value == 'COMPLETED'
        )
        self.simulation_results.completed_receiving_tasks = sum(
            1 for task in receiving_tasks if task.status.value == 'COMPLETED'
        )
        
        # 逾期進貨統計
        self.simulation_results.overdue_receiving_tasks = sum(
            1 for task in receiving_tasks if task.is_overdue
        )
        
        # 加班統計
        self.simulation_results.overtime_sessions = len(self.overtime_sessions)
        self.simulation_results.total_overtime_hours = sum(
            session.get('actual_hours', session.get('total_hours', 0)) 
            for session in self.overtime_sessions
        )
        
        # 總任務數
        self.simulation_results.total_tasks_completed = (
            self.simulation_results.completed_shipping_tasks + 
            self.simulation_results.completed_receiving_tasks
        )
        
        # 完成波次數
        self.simulation_results.total_waves_completed = len(self.wave_manager.wave_history)
        
        # 處理異常數
        self.simulation_results.total_exceptions_handled = len(self.exception_handler.resolved_exceptions)
    
    # === 保留其他原有方法 ===
    def initialize_simulation(self, config: SimulationConfig) -> Dict[str, Any]:
        """初始化模擬"""
        try:
            self.simulation_config = config
            self.simulation_state = SimulationState.INITIALIZED
            
            # 設定隨機種子
            if config.random_seed:
                np.random.seed(config.random_seed)
                random.seed(config.random_seed)
            
            # 解析時間範圍
            start_datetime = datetime.strptime(config.start_date, '%Y-%m-%d')
            end_datetime = datetime.strptime(config.end_date, '%Y-%m-%d')
            
            if start_datetime >= end_datetime:
                raise ValueError("開始日期必須早於結束日期")
            
            self.current_simulation_time = start_datetime
            
            # 創建模擬結果物件
            simulation_id = f"SIM_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
            self.simulation_results = SimulationResults(
                simulation_id=simulation_id,
                config=config,
                start_time=datetime.now(),
                simulated_time_range=(start_datetime, end_datetime)
            )
            
            # 重置所有模組狀態
            self._reset_all_modules()
            
            # 清空事件隊列
            self.event_queue.clear()
            self.processed_events.clear()
            self.event_statistics.clear()
            self.performance_samples.clear()
            
            # 🆕 清空進貨和加班相關追蹤
            self.daily_receiving_data.clear()
            self.overtime_sessions.clear()
            self.current_overtime_schedule = None
            
            # 建立初始事件
            self._create_initial_events(start_datetime, end_datetime)
            
            self.logger.info(f"✅ 模擬初始化完成: {simulation_id}")
            self.logger.info(f"  時間範圍: {config.start_date} 到 {config.end_date}")
            self.logger.info(f"  初始事件數量: {len(self.event_queue)}")
            
            return {
                'success': True,
                'simulation_id': simulation_id,
                'simulated_time_range': (start_datetime, end_datetime),
                'initial_events': len(self.event_queue)
            }
            
        except Exception as e:
            self.simulation_state = SimulationState.ERROR
            error_msg = f"模擬初始化失敗: {str(e)}"
            self.logger.error(error_msg)
            
            if self.simulation_results:
                self.simulation_results.errors.append(error_msg)
            
            return {'success': False, 'error': error_msg}
    
    # === 保留所有其他原有方法 ===
    def run_simulation(self) -> SimulationResults:
        """執行模擬主循環"""
        if self.simulation_state != SimulationState.INITIALIZED:
            raise RuntimeError("模擬未正確初始化")
        
        try:
            self.simulation_state = SimulationState.RUNNING
            self.should_continue = True
            
            self.logger.info("🚀 開始執行模擬...")
            
            # 主模擬循環
            while self.should_continue and self.event_queue:
                if self.pause_requested:
                    self._handle_pause()
                    continue
                
                # 處理下一個事件
                self._process_next_event()
                
                # 檢查模擬條件
                if not self._check_simulation_conditions():
                    break
                
                # 步進模式處理
                if self.step_mode:
                    self._handle_step_mode()
            
            # 完成模擬
            self._finalize_simulation()
            
            self.logger.info("🏁 模擬執行完成")
            
        except Exception as e:
            self.simulation_state = SimulationState.ERROR
            error_msg = f"模擬執行錯誤: {str(e)}"
            self.logger.error(error_msg)
            
            if self.simulation_results:
                self.simulation_results.errors.append(error_msg)
                self.simulation_results.end_time = datetime.now()
        
        return self.simulation_results
    
    def _start_task_execution(self, task, current_time: datetime):
        """🔧 修改：開始任務執行（使用隨機時間計算）"""
        
        # 取得員工技能資訊
        staff_skill_info = None
        if task.assigned_staff:
            staff_skill_info = self.workstation_task_manager._get_staff_skill_info(task.assigned_staff)
        
        # 🆕 計算實際執行時間（包含隨機性）
        actual_duration = self.workstation_task_manager.calculate_actual_duration_with_randomness(
            task, staff_skill_info
        )
        
        # 更新任務資訊
        task.actual_duration = actual_duration
        task.actual_start_time = current_time
        task.status = task.status.IN_PROGRESS
        
        # 計算實際完成時間
        actual_completion_time = current_time + timedelta(minutes=actual_duration)
        
        # 排程任務完成事件（使用實際時間）
        self._schedule_event(
            EventType.TASK_COMPLETE,
            actual_completion_time,
            priority=3,
            event_data={'task_id': task.task_id}
        )
        
        # 更新工作站狀態
        if task.assigned_station:
            station = self.workstation_task_manager.workstations[task.assigned_station]
            station.status = StationStatus.BUSY
            station.available_time = actual_completion_time
        
        task_type_str = "進貨" if task.task_type.value == 'RECEIVING' else "出貨"
        self.logger.info(f"▶️ {task_type_str}任務 {task.task_id} 開始執行（實際時間: {actual_duration:.1f}分鐘）")


    # 🆕 新增：波次實際完成檢查事件處理
    def _handle_wave_completion_check(self, event: SimulationEvent, current_time: datetime):
        """🆕 新增：檢查波次是否實際完成"""
        wave_id = event.event_data.get('wave_id')
        
        if wave_id:
            completion_result = self.wave_manager.check_wave_actual_completion(wave_id, current_time)
            
            if completion_result['completed']:
                self.logger.info(f"🏁 波次 {wave_id} 實際完成")
                
                # 釋放該波次的工作站，允許接受其他任務（非下一波次一般出貨）
                wave = self.wave_manager.waves[wave_id]
                for station_id in wave.assigned_workstations:
                    self._schedule_event(
                        EventType.STATION_BECOME_IDLE,
                        current_time,
                        priority=4,
                        event_data={'station_id': station_id, 'wave_completed': True}
                    )
    

    def _handle_task_assign(self, event: SimulationEvent, current_time: datetime):
            """處理任務分配事件"""
            task_id = event.event_data.get('task_id')
            task_type = event.event_data.get('task_type', 'SHIPPING')
            
            self.logger.info(f"📋 分配任務: {task_id} ({task_type})")
            # 這裡可以添加任務分配邏輯，或者簡單記錄即可

    def _handle_station_startup_complete(self, event: SimulationEvent, current_time: datetime):
        """處理工作站啟動完成事件"""
        station_id = event.event_data.get('station_id')
        self.logger.info(f"🔧 工作站 {station_id} 啟動完成")

    def _handle_station_become_idle(self, event: SimulationEvent, current_time: datetime):
        """處理工作站變為空閒事件"""
        station_id = event.event_data.get('station_id')
        self.logger.info(f"💤 工作站 {station_id} 變為空閒")

    def _handle_wave_start(self, event: SimulationEvent, current_time: datetime):
        """處理波次開始事件"""
        wave_id = event.event_data.get('wave_id')
        self.logger.info(f"🌊 波次 {wave_id} 開始")

    def _handle_wave_complete(self, event: SimulationEvent, current_time: datetime):
        """處理波次完成事件"""
        wave_id = event.event_data.get('wave_id')
        self.logger.info(f"🏁 波次 {wave_id} 完成")

    def _handle_exception_detected(self, event: SimulationEvent, current_time: datetime):
        """處理異常檢測事件"""
        exception_id = event.event_data.get('exception_id')
        self.logger.warning(f"⚠️ 檢測到異常: {exception_id}")

    def _handle_exception_resolved(self, event: SimulationEvent, current_time: datetime):
        """處理異常解決事件"""
        exception_id = event.event_data.get('exception_id')
        self.logger.info(f"✅ 異常已解決: {exception_id}")

    def _handle_system_status_update(self, event: SimulationEvent, current_time: datetime):
        """處理系統狀態更新事件"""
        self.logger.debug(f"📊 系統狀態更新: {current_time}")

    def _handle_daily_schedule_generate(self, event: SimulationEvent, current_time: datetime):
        """處理每日排班生成事件"""
        date_str = event.event_data.get('date')
        self.logger.info(f"📅 生成 {date_str} 排班")

    def _handle_custom_event(self, event: SimulationEvent, current_time: datetime):
        """處理自定義事件"""
        if event.handler_function:
            event.handler_function(event, current_time)
        else:
            self.logger.info(f"🔧 處理自定義事件: {event.event_data}")

    def _schedule_event(self, event_type: EventType, scheduled_time: datetime, 
                       priority: int = 0, event_data: Dict = None, handler_function: Callable = None):
        """排程事件"""
        event_id = f"{event_type.value}_{scheduled_time.strftime('%Y%m%d_%H%M%S')}_{len(self.event_queue)}"
        
        event = SimulationEvent(
            event_id=event_id,
            event_type=event_type,
            scheduled_time=scheduled_time,
            priority=priority,
            event_data=event_data or {},
            handler_function=handler_function
        )
        
        heapq.heappush(self.event_queue, event)

    def _process_next_event(self):
        """處理下一個事件"""
        if not self.event_queue:
            return False
        
        event = heapq.heappop(self.event_queue)
        self.current_simulation_time = event.scheduled_time
        
        try:
            handler = self.event_handlers.get(event.event_type)
            if handler:
                handler(event, self.current_simulation_time)
            else:
                self.logger.warning(f"找不到事件處理器: {event.event_type}")
            
            self.processed_events.append(event)
            self.event_statistics[event.event_type.value] += 1
            
        except Exception as e:
            self.logger.error(f"處理事件 {event.event_id} 時發生錯誤: {str(e)}")
        
        return True

    def _check_simulation_conditions(self) -> bool:
        """檢查模擬條件"""
        return self.should_continue

    def _handle_pause(self):
        """處理暫停"""
        self.simulation_state = SimulationState.PAUSED
        while self.pause_requested:
            time.sleep(0.1)
        self.simulation_state = SimulationState.RUNNING

    def _handle_step_mode(self):
        """處理步進模式"""
        input("按 Enter 繼續下一步...")

    def _finalize_simulation(self):
        """完成模擬"""
        self.simulation_state = SimulationState.COMPLETED
        self._calculate_final_statistics()

    def _reset_all_modules(self):
        """重置所有模組狀態"""
        # 重置異常處理器
        if hasattr(self.exception_handler, 'reset_exception_state'):
            self.exception_handler.reset_exception_state()
        
        # 重置其他模組狀態
        self.logger.info("所有模組狀態已重置")