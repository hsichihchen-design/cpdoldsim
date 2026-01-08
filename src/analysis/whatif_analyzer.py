"""
WhatIfAnalyzer - What-if分析模組
負責執行各種假設情境分析
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import copy
import json
from collections import defaultdict
import itertools

class ScenarioType(Enum):
    """情境類型枚舉"""
    SPEED_REDUCTION = "SPEED_REDUCTION"           # 速度減慢
    STAFF_REDUCTION = "STAFF_REDUCTION"           # 人員減少
    WORKSTATION_REDUCTION = "WORKSTATION_REDUCTION"  # 工作站減少
    EXCEPTION_INCREASE = "EXCEPTION_INCREASE"     # 異常增加
    ORDER_VOLUME_CHANGE = "ORDER_VOLUME_CHANGE"   # 訂單量變化
    PRIORITY_SHIFT = "PRIORITY_SHIFT"             # 優先權調整
    CAPACITY_OPTIMIZATION = "CAPACITY_OPTIMIZATION"  # 產能最佳化
    SKILL_ENHANCEMENT = "SKILL_ENHANCEMENT"       # 技能提升
    EQUIPMENT_FAILURE = "EQUIPMENT_FAILURE"       # 設備故障
    SEASONAL_VARIATION = "SEASONAL_VARIATION"     # 季節性變化

class ImpactLevel(Enum):
    """影響程度枚舉"""
    MINIMAL = "MINIMAL"       # 最小影響 (<5%)
    LOW = "LOW"              # 低影響 (5-15%)
    MODERATE = "MODERATE"    # 中等影響 (15-30%)
    HIGH = "HIGH"            # 高影響 (30-50%)
    SEVERE = "SEVERE"        # 嚴重影響 (>50%)

@dataclass
class ScenarioConfig:
    """情境配置"""
    scenario_id: str
    scenario_type: ScenarioType
    description: str
    
    # 參數調整
    parameter_changes: Dict[str, Any] = field(default_factory=dict)
    
    # 測試範圍
    test_duration_days: int = 7
    warm_up_days: int = 1
    
    # 測試條件
    baseline_comparison: bool = True
    multiple_runs: int = 3
    confidence_level: float = 0.95
    
    # 元數據
    created_by: str = "system"
    created_at: Optional[datetime] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ScenarioResult:
    """情境分析結果"""
    scenario_id: str
    scenario_config: ScenarioConfig
    
    # 執行資訊
    execution_time: Optional[datetime] = None
    simulation_duration_seconds: float = 0.0
    
    # 基準對比
    baseline_metrics: Dict[str, float] = field(default_factory=dict)
    scenario_metrics: Dict[str, float] = field(default_factory=dict)
    
    # 影響分析
    impact_summary: Dict[str, Any] = field(default_factory=dict)
    impact_level: Optional[ImpactLevel] = None
    critical_thresholds_breached: List[str] = field(default_factory=list)
    
    # 詳細結果
    daily_results: List[Dict] = field(default_factory=list)
    performance_metrics: Dict[str, List[float]] = field(default_factory=dict)
    
    # 統計分析
    statistical_summary: Dict[str, Any] = field(default_factory=dict)
    
    # 建議
    recommendations: List[str] = field(default_factory=list)
    risk_assessment: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ThresholdDefinition:
    """臨界值定義"""
    metric_name: str
    critical_threshold: float
    warning_threshold: float
    direction: str = "upper"  # "upper", "lower", "both"
    description: str = ""

class WhatIfAnalyzer:
    def __init__(self, simulation_engine, data_manager, system_state_tracker, 
                 daily_workload_manager, exception_handler):
        """初始化What-if分析器"""
        self.logger = logging.getLogger(__name__)
        
        # 關聯的管理器
        self.simulation_engine = simulation_engine
        self.data_manager = data_manager
        self.system_state_tracker = system_state_tracker
        self.daily_workload_manager = daily_workload_manager
        self.exception_handler = exception_handler
        
        # 分析結果
        self.scenario_results: Dict[str, ScenarioResult] = {}
        self.baseline_results: Optional[Dict] = None
        
        # 臨界值定義
        self.critical_thresholds = self._define_critical_thresholds()
        
        # 預定義情境模板
        self.scenario_templates = self._create_scenario_templates()
        
        self.logger.info("WhatIfAnalyzer 初始化完成")
    
    def _define_critical_thresholds(self) -> List[ThresholdDefinition]:
        """定義系統臨界值"""
        return [
            ThresholdDefinition(
                metric_name="workstation_utilization",
                critical_threshold=95.0,
                warning_threshold=85.0,
                direction="upper",
                description="工作站利用率過高"
            ),
            ThresholdDefinition(
                metric_name="task_completion_rate",
                critical_threshold=70.0,
                warning_threshold=80.0,
                direction="lower",
                description="任務完成率過低"
            ),
            ThresholdDefinition(
                metric_name="overtime_hours_per_day",
                critical_threshold=32.0,  # 8人 * 4小時
                warning_threshold=20.0,
                direction="upper",
                description="每日加班時數過高"
            ),
            ThresholdDefinition(
                metric_name="exception_count_per_day",
                critical_threshold=15.0,
                warning_threshold=10.0,
                direction="upper",
                description="每日異常數量過多"
            ),
            ThresholdDefinition(
                metric_name="wave_completion_delay_hours",
                critical_threshold=4.0,
                warning_threshold=2.0,
                direction="upper",
                description="波次完成延遲過長"
            ),
            ThresholdDefinition(
                metric_name="staff_utilization",
                critical_threshold=95.0,
                warning_threshold=90.0,
                direction="upper",
                description="人員利用率過高"
            )
        ]
    
    def _create_scenario_templates(self) -> Dict[str, ScenarioConfig]:
        """創建預定義情境模板"""
        templates = {}
        
        # 速度減慢情境
        templates['speed_reduction_light'] = ScenarioConfig(
            scenario_id='speed_reduction_light',
            scenario_type=ScenarioType.SPEED_REDUCTION,
            description='輕度速度減慢（10%）',
            parameter_changes={'speed_reduction_factor': 0.1},
            tags=['performance', 'light_impact']
        )
        
        templates['speed_reduction_moderate'] = ScenarioConfig(
            scenario_id='speed_reduction_moderate',
            scenario_type=ScenarioType.SPEED_REDUCTION,
            description='中度速度減慢（25%）',
            parameter_changes={'speed_reduction_factor': 0.25},
            tags=['performance', 'moderate_impact']
        )
        
        templates['speed_reduction_severe'] = ScenarioConfig(
            scenario_id='speed_reduction_severe',
            scenario_type=ScenarioType.SPEED_REDUCTION,
            description='嚴重速度減慢（50%）',
            parameter_changes={'speed_reduction_factor': 0.5},
            tags=['performance', 'severe_impact']
        )
        
        # 人員減少情境
        templates['staff_reduction_10pct'] = ScenarioConfig(
            scenario_id='staff_reduction_10pct',
            scenario_type=ScenarioType.STAFF_REDUCTION,
            description='人員減少10%',
            parameter_changes={'staff_reduction_percentage': 0.1},
            tags=['staffing', 'light_impact']
        )
        
        templates['staff_reduction_25pct'] = ScenarioConfig(
            scenario_id='staff_reduction_25pct',
            scenario_type=ScenarioType.STAFF_REDUCTION,
            description='人員減少25%',
            parameter_changes={'staff_reduction_percentage': 0.25},
            tags=['staffing', 'moderate_impact']
        )
        
        # 異常增加情境
        templates['exception_increase_2x'] = ScenarioConfig(
            scenario_id='exception_increase_2x',
            scenario_type=ScenarioType.EXCEPTION_INCREASE,
            description='異常頻率增加2倍',
            parameter_changes={'exception_frequency_multiplier': 2.0},
            tags=['exceptions', 'moderate_impact']
        )
        
        templates['exception_increase_5x'] = ScenarioConfig(
            scenario_id='exception_increase_5x',
            scenario_type=ScenarioType.EXCEPTION_INCREASE,
            description='異常頻率增加5倍',
            parameter_changes={'exception_frequency_multiplier': 5.0},
            tags=['exceptions', 'severe_impact']
        )
        
        # 訂單量變化情境
        templates['order_volume_increase_50pct'] = ScenarioConfig(
            scenario_id='order_volume_increase_50pct',
            scenario_type=ScenarioType.ORDER_VOLUME_CHANGE,
            description='訂單量增加50%',
            parameter_changes={'order_volume_multiplier': 1.5},
            tags=['volume', 'high_impact']
        )
        
        templates['order_volume_peak_season'] = ScenarioConfig(
            scenario_id='order_volume_peak_season',
            scenario_type=ScenarioType.ORDER_VOLUME_CHANGE,
            description='旺季訂單量（200%）',
            parameter_changes={'order_volume_multiplier': 2.0},
            tags=['volume', 'seasonal', 'severe_impact']
        )
        
        return templates
    
    def run_scenario_analysis(self, scenario_config: ScenarioConfig, 
                            baseline_data: Dict = None) -> ScenarioResult:
        """執行情境分析"""
        self.logger.info(f"開始執行情境分析: {scenario_config.scenario_id}")
        
        start_time = datetime.now()
        
        # 創建結果物件
        result = ScenarioResult(
            scenario_id=scenario_config.scenario_id,
            scenario_config=scenario_config,
            execution_time=start_time
        )
        
        try:
            # 1. 準備基準數據
            if baseline_data is None and scenario_config.baseline_comparison:
                baseline_data = self._run_baseline_simulation(scenario_config)
                result.baseline_metrics = baseline_data
            
            # 2. 執行情境模擬
            scenario_data = self._run_scenario_simulation(scenario_config)
            result.scenario_metrics = scenario_data
            
            # 3. 計算影響分析
            if baseline_data:
                result.impact_summary = self._calculate_impact_analysis(
                    baseline_data, scenario_data
                )
                result.impact_level = self._determine_impact_level(result.impact_summary)
            
            # 4. 檢查臨界值
            result.critical_thresholds_breached = self._check_critical_thresholds(scenario_data)
            
            # 5. 生成建議
            result.recommendations = self._generate_recommendations(
                scenario_config, result.impact_summary, result.critical_thresholds_breached
            )
            
            # 6. 風險評估
            result.risk_assessment = self._assess_risks(scenario_config, result)
            
            # 記錄執行時間
            result.simulation_duration_seconds = (datetime.now() - start_time).total_seconds()
            
            # 儲存結果
            self.scenario_results[scenario_config.scenario_id] = result
            
            self.logger.info(f" 情境分析完成: {scenario_config.scenario_id}")
            
        except Exception as e:
            error_msg = f"情境分析執行錯誤: {str(e)}"
            self.logger.error(error_msg)
            result.impact_summary['error'] = error_msg
        
        return result
    
    def _run_baseline_simulation(self, scenario_config: ScenarioConfig) -> Dict[str, float]:
        """執行基準模擬"""
        self.logger.info("執行基準模擬...")
        
        # 使用原始參數執行模擬
        original_config = copy.deepcopy(self.simulation_engine.simulation_config)
        
        # 設定模擬時間範圍
        start_date = datetime.now().strftime('%Y-%m-%d')
        end_date = (datetime.now() + timedelta(days=scenario_config.test_duration_days)).strftime('%Y-%m-%d')
        
        from simulation_engine import SimulationConfig
        sim_config = SimulationConfig(
            start_date=start_date,
            end_date=end_date,
            random_seed=12345  # 固定種子確保可重現性
        )
        
        # 執行基準模擬
        self.simulation_engine.initialize_simulation(sim_config)
        baseline_results = self.simulation_engine.run_simulation()
        
        # 提取關鍵指標
        baseline_metrics = self._extract_key_metrics(baseline_results)
        
        self.baseline_results = baseline_metrics
        
        return baseline_metrics
    
    def _run_scenario_simulation(self, scenario_config: ScenarioConfig) -> Dict[str, float]:
        """執行情境模擬"""
        self.logger.info(f"執行情境模擬: {scenario_config.scenario_type.value}")
        
        # 備份原始參數
        original_params = self._backup_system_parameters()
        
        try:
            # 應用情境參數
            self._apply_scenario_parameters(scenario_config)
            
            # 設定模擬時間範圍
            start_date = datetime.now().strftime('%Y-%m-%d')
            end_date = (datetime.now() + timedelta(days=scenario_config.test_duration_days)).strftime('%Y-%m-%d')
            
            from simulation_engine import SimulationConfig
            sim_config = SimulationConfig(
                start_date=start_date,
                end_date=end_date,
                random_seed=12345  # 使用相同種子確保可比較性
            )
            
            # 執行情境模擬
            self.simulation_engine.initialize_simulation(sim_config)
            scenario_results = self.simulation_engine.run_simulation()
            
            # 提取關鍵指標
            scenario_metrics = self._extract_key_metrics(scenario_results)
            
            return scenario_metrics
            
        finally:
            # 恢復原始參數
            self._restore_system_parameters(original_params)
    
    def _backup_system_parameters(self) -> Dict[str, Any]:
        """備份系統參數"""
        backup = {}
        
        # 備份各管理器的關鍵參數
        if hasattr(self.simulation_engine.workstation_task_manager, 'params'):
            backup['workstation_params'] = copy.deepcopy(
                self.simulation_engine.workstation_task_manager.params
            )
        
        if hasattr(self.simulation_engine.staff_schedule_generator, 'params'):
            backup['staff_params'] = copy.deepcopy(
                self.simulation_engine.staff_schedule_generator.params
            )
        
        if hasattr(self.exception_handler, 'params'):
            backup['exception_params'] = copy.deepcopy(
                self.exception_handler.params
            )
        
        return backup
    
    def _restore_system_parameters(self, backup: Dict[str, Any]):
        """恢復系統參數"""
        if 'workstation_params' in backup:
            self.simulation_engine.workstation_task_manager.params = backup['workstation_params']
        
        if 'staff_params' in backup:
            self.simulation_engine.staff_schedule_generator.params = backup['staff_params']
        
        if 'exception_params' in backup:
            self.exception_handler.params = backup['exception_params']
    
    def _apply_scenario_parameters(self, scenario_config: ScenarioConfig):
        """應用情境參數"""
        changes = scenario_config.parameter_changes
        
        if scenario_config.scenario_type == ScenarioType.SPEED_REDUCTION:
            # 調整作業速度
            if 'speed_reduction_factor' in changes:
                reduction_factor = changes['speed_reduction_factor']
                
                # 增加所有作業時間
                if hasattr(self.simulation_engine.workstation_task_manager, 'params'):
                    params = self.simulation_engine.workstation_task_manager.params
                    params['picking_base_time_repack'] *= (1 + reduction_factor)
                    params['picking_base_time_no_repack'] *= (1 + reduction_factor)
                    params['station_startup_time_minutes'] *= (1 + reduction_factor)
        
        elif scenario_config.scenario_type == ScenarioType.STAFF_REDUCTION:
            # 減少人員數量
            if 'staff_reduction_percentage' in changes:
                reduction_pct = changes['staff_reduction_percentage']
                
                if hasattr(self.simulation_engine.staff_schedule_generator, 'params'):
                    params = self.simulation_engine.staff_schedule_generator.params
                    for floor in [2, 3, 4]:
                        param_key = f'planned_staff_{floor}f'
                        if param_key in params:
                            original_count = params[param_key]
                            reduced_count = max(1, int(original_count * (1 - reduction_pct)))
                            params[param_key] = reduced_count
        
        elif scenario_config.scenario_type == ScenarioType.EXCEPTION_INCREASE:
            # 增加異常頻率
            if 'exception_frequency_multiplier' in changes:
                multiplier = changes['exception_frequency_multiplier']
                
                if hasattr(self.exception_handler, 'params'):
                    params = self.exception_handler.params
                    params['exception_probability_shipping'] *= multiplier
                    params['exception_probability_receiving'] *= multiplier
        
        elif scenario_config.scenario_type == ScenarioType.ORDER_VOLUME_CHANGE:
            # 調整訂單量（這個需要在數據載入時處理）
            if 'order_volume_multiplier' in changes:
                # 這個會在數據載入時被處理
                pass
        
        # 記錄參數變更
        self.logger.info(f"已應用情境參數: {changes}")
    
    def _extract_key_metrics(self, simulation_results) -> Dict[str, float]:
        """提取關鍵指標"""
        metrics = {}
        
        if simulation_results and simulation_results.final_metrics:
            final_metrics = simulation_results.final_metrics
            
            # 系統性能指標
            if 'system_overview' in final_metrics:
                overview = final_metrics['system_overview']
                metrics['total_tasks'] = overview.get('total_tasks', 0)
                metrics['total_workstations'] = overview.get('total_workstations', 0)
                
                # 計算任務完成率
                task_counts = overview.get('task_status_counts', {})
                completed = task_counts.get('COMPLETED', 0)
                total = sum(task_counts.values()) if task_counts else 1
                metrics['task_completion_rate'] = (completed / total) * 100 if total > 0 else 0
            
            # 工作站利用率
            if 'workstation_summary' in final_metrics:
                ws_summary = final_metrics['workstation_summary']
                metrics['workstation_utilization'] = ws_summary.get('utilization_by_floor', {}).get('average', 0)
            
            # 波次性能
            if 'wave_summary' in final_metrics:
                wave_summary = final_metrics['wave_summary']
                metrics['completed_waves'] = wave_summary.get('completed_waves_count', 0)
                metrics['average_wave_progress'] = wave_summary.get('average_progress', 0)
            
            # 異常統計
            if 'exception_summary' in final_metrics:
                exc_summary = final_metrics['exception_summary']
                metrics['exception_count_per_day'] = exc_summary.get('active_exceptions_count', 0)
                metrics['exception_handling_time'] = exc_summary.get('average_handling_time', 0)
            
            # 人員利用率
            if 'staff_summary' in final_metrics:
                staff_summary = final_metrics['staff_summary']
                metrics['staff_utilization'] = staff_summary.get('utilization_rate', 0)
        
        # 從系統狀態追蹤器取得額外指標
        if hasattr(self.system_state_tracker, 'metrics_history') and self.system_state_tracker.metrics_history:
            latest_metrics = self.system_state_tracker.metrics_history[-1]
            metrics['overall_efficiency'] = latest_metrics.overall_efficiency
        
        # 從工作量管理器取得指標
        if hasattr(self.daily_workload_manager, 'daily_workloads'):
            total_overtime = 0
            total_days = 0
            
            for daily_workload in self.daily_workload_manager.daily_workloads.values():
                if daily_workload.overtime_required:
                    total_overtime += daily_workload.overtime_hours
                total_days += 1
            
            metrics['overtime_hours_per_day'] = total_overtime / max(1, total_days)
            metrics['overtime_frequency'] = sum(1 for dw in self.daily_workload_manager.daily_workloads.values() 
                                               if dw.overtime_required) / max(1, total_days) * 100
        
        return metrics
    
    def _calculate_impact_analysis(self, baseline_metrics: Dict[str, float], 
                                 scenario_metrics: Dict[str, float]) -> Dict[str, Any]:
        """計算影響分析"""
        impact_analysis = {
            'metric_comparisons': {},
            'overall_impact_score': 0.0,
            'degraded_metrics': [],
            'improved_metrics': [],
            'critical_impacts': []
        }
        
        total_impact_score = 0.0
        metric_count = 0
        
        for metric_name in baseline_metrics:
            if metric_name in scenario_metrics:
                baseline_value = baseline_metrics[metric_name]
                scenario_value = scenario_metrics[metric_name]
                
                # 計算變化百分比
                if baseline_value != 0:
                    change_percent = ((scenario_value - baseline_value) / baseline_value) * 100
                else:
                    change_percent = 0.0
                
                # 判斷影響方向（某些指標數值越高越好，某些越低越好）
                positive_metrics = ['task_completion_rate', 'overall_efficiency', 'completed_waves']
                negative_metrics = ['exception_count_per_day', 'overtime_hours_per_day', 'exception_handling_time']
                
                if metric_name in positive_metrics:
                    impact_score = -change_percent  # 降低是負面影響
                elif metric_name in negative_metrics:
                    impact_score = change_percent   # 增加是負面影響
                else:
                    impact_score = abs(change_percent)  # 任何變化都是影響
                
                comparison = {
                    'baseline_value': round(baseline_value, 3),
                    'scenario_value': round(scenario_value, 3),
                    'absolute_change': round(scenario_value - baseline_value, 3),
                    'percentage_change': round(change_percent, 2),
                    'impact_score': round(impact_score, 2)
                }
                
                impact_analysis['metric_comparisons'][metric_name] = comparison
                
                # 分類影響
                if abs(change_percent) > 10:  # 變化超過10%
                    if change_percent > 0:
                        if metric_name in positive_metrics:
                            impact_analysis['improved_metrics'].append(metric_name)
                        else:
                            impact_analysis['degraded_metrics'].append(metric_name)
                    else:
                        if metric_name in positive_metrics:
                            impact_analysis['degraded_metrics'].append(metric_name)
                        else:
                            impact_analysis['improved_metrics'].append(metric_name)
                
                # 檢查關鍵影響
                if abs(change_percent) > 30:
                    impact_analysis['critical_impacts'].append({
                        'metric': metric_name,
                        'change_percent': round(change_percent, 2),
                        'severity': 'HIGH' if abs(change_percent) > 50 else 'MODERATE'
                    })
                
                total_impact_score += abs(impact_score)
                metric_count += 1
        
        # 計算整體影響分數
        if metric_count > 0:
            impact_analysis['overall_impact_score'] = round(total_impact_score / metric_count, 2)
        
        return impact_analysis
    
    def _determine_impact_level(self, impact_analysis: Dict[str, Any]) -> ImpactLevel:
        """判斷影響程度"""
        impact_score = impact_analysis.get('overall_impact_score', 0)
        
        if impact_score < 5:
            return ImpactLevel.MINIMAL
        elif impact_score < 15:
            return ImpactLevel.LOW
        elif impact_score < 30:
            return ImpactLevel.MODERATE
        elif impact_score < 50:
            return ImpactLevel.HIGH
        else:
            return ImpactLevel.SEVERE
    
    def _check_critical_thresholds(self, metrics: Dict[str, float]) -> List[str]:
        """檢查臨界值突破"""
        breached_thresholds = []
        
        for threshold in self.critical_thresholds:
            metric_name = threshold.metric_name
            
            if metric_name in metrics:
                value = metrics[metric_name]
                
                # 檢查臨界值
                if threshold.direction == "upper" and value > threshold.critical_threshold:
                    breached_thresholds.append(f"{metric_name} 超過臨界值 {threshold.critical_threshold} (實際: {value:.2f})")
                elif threshold.direction == "lower" and value < threshold.critical_threshold:
                    breached_thresholds.append(f"{metric_name} 低於臨界值 {threshold.critical_threshold} (實際: {value:.2f})")
                elif threshold.direction == "both":
                    if value > threshold.critical_threshold or value < threshold.warning_threshold:
                        breached_thresholds.append(f"{metric_name} 超出正常範圍 ({threshold.warning_threshold}-{threshold.critical_threshold})")
        
        return breached_thresholds
    
    def _generate_recommendations(self, scenario_config: ScenarioConfig, 
                                impact_analysis: Dict[str, Any], 
                                breached_thresholds: List[str]) -> List[str]:
        """生成建議"""
        recommendations = []
        
        # 基於情境類型的建議
        if scenario_config.scenario_type == ScenarioType.SPEED_REDUCTION:
            if impact_analysis.get('overall_impact_score', 0) > 20:
                recommendations.append("考慮投資自動化設備或改善流程以提升作業效率")
                recommendations.append("評估是否需要增加人員或工作站來補償速度下降")
        
        elif scenario_config.scenario_type == ScenarioType.STAFF_REDUCTION:
            if 'overtime_hours_per_day' in impact_analysis.get('degraded_metrics', []):
                recommendations.append("人員減少導致加班增加，建議優化排班或考慮臨時人力")
                recommendations.append("評估跨樓層支援的可行性")
        
        elif scenario_config.scenario_type == ScenarioType.EXCEPTION_INCREASE:
            if breached_thresholds:
                recommendations.append("異常增加影響系統穩定性，建議加強預防措施")
                recommendations.append("考慮增加主管人員或改善異常處理流程")
        
        # 基於臨界值突破的建議
        if breached_thresholds:
            recommendations.append("系統存在臨界值突破風險，需要制定應急計劃")
            
            for breach in breached_thresholds:
                if "工作站利用率" in breach:
                    recommendations.append("考慮增加工作站數量或優化工作站分配")
                elif "任務完成率" in breach:
                    recommendations.append("檢討任務分配邏輯和人員技能匹配")
                elif "加班時數" in breach:
                    recommendations.append("重新評估產能規劃和人力配置")
        
        # 基於影響程度的建議
        impact_level = self._determine_impact_level(impact_analysis)
        
        if impact_level in [ImpactLevel.HIGH, ImpactLevel.SEVERE]:
            recommendations.append("影響程度較高，建議制定詳細的風險緩解計劃")
            recommendations.append("考慮分階段實施變更，並建立監控機制")
        
        return recommendations
    
    def _assess_risks(self, scenario_config: ScenarioConfig, result: ScenarioResult) -> Dict[str, Any]:
        """評估風險"""
        risk_assessment = {
            'overall_risk_level': 'LOW',
            'risk_factors': [],
            'mitigation_strategies': [],
            'monitoring_requirements': []
        }
        
        # 基於影響程度評估風險
        if result.impact_level in [ImpactLevel.HIGH, ImpactLevel.SEVERE]:
            risk_assessment['overall_risk_level'] = 'HIGH'
            risk_assessment['risk_factors'].append('系統性能大幅下降')
        elif result.impact_level == ImpactLevel.MODERATE:
            risk_assessment['overall_risk_level'] = 'MEDIUM'
            risk_assessment['risk_factors'].append('系統性能中度影響')
        
        # 基於臨界值突破評估風險
        if result.critical_thresholds_breached:
            risk_assessment['overall_risk_level'] = 'HIGH'
            risk_assessment['risk_factors'].extend(result.critical_thresholds_breached)
        
        # 制定緩解策略
        if scenario_config.scenario_type == ScenarioType.STAFF_REDUCTION:
            risk_assessment['mitigation_strategies'].append('建立人員備援計劃')
            risk_assessment['mitigation_strategies'].append('實施靈活排班制度')
        
        if scenario_config.scenario_type == ScenarioType.EXCEPTION_INCREASE:
            risk_assessment['mitigation_strategies'].append('加強員工訓練')
            risk_assessment['mitigation_strategies'].append('改善設備維護計劃')
        
        # 監控需求
        risk_assessment['monitoring_requirements'] = [
            '即時工作站利用率監控',
            '每日任務完成率追蹤',
            '異常事件頻率監控',
            '加班時數統計'
        ]
        
        return risk_assessment
    
    def test_speed_reduction_impact(self, speed_reduction_percentage: float, 
                                  test_duration_days: int = 7) -> ScenarioResult:
        """測試速度減慢影響"""
        scenario_config = ScenarioConfig(
            scenario_id=f"speed_reduction_{speed_reduction_percentage*100:.0f}pct",
            scenario_type=ScenarioType.SPEED_REDUCTION,
            description=f"作業速度減慢 {speed_reduction_percentage*100:.0f}%",
            parameter_changes={'speed_reduction_factor': speed_reduction_percentage},
            test_duration_days=test_duration_days,
            tags=['automated_test', 'speed_impact']
        )
        
        return self.run_scenario_analysis(scenario_config)
    
    def test_staffing_reduction_impact(self, staff_reduction_count: int = None, 
                                     staff_reduction_percentage: float = None,
                                     test_duration_days: int = 7) -> ScenarioResult:
        """測試人員減少影響"""
        if staff_reduction_percentage is None and staff_reduction_count is not None:
            # 假設總人員數為24人（3樓層*8人）
            total_staff = 24
            staff_reduction_percentage = staff_reduction_count / total_staff
        elif staff_reduction_percentage is None:
            staff_reduction_percentage = 0.1  # 預設10%
        
        scenario_config = ScenarioConfig(
            scenario_id=f"staff_reduction_{staff_reduction_percentage*100:.0f}pct",
            scenario_type=ScenarioType.STAFF_REDUCTION,
            description=f"人員減少 {staff_reduction_percentage*100:.0f}%",
            parameter_changes={'staff_reduction_percentage': staff_reduction_percentage},
            test_duration_days=test_duration_days,
            tags=['automated_test', 'staffing_impact']
        )
        
        return self.run_scenario_analysis(scenario_config)
    
    def find_critical_thresholds(self, parameter_name: str, test_range: Tuple[float, float], 
                               step_size: float = 0.05, target_metric: str = None) -> Dict[str, Any]:
        """找出系統臨界值"""
        min_val, max_val = test_range
        current_val = min_val
        
        threshold_analysis = {
            'parameter_name': parameter_name,
            'test_range': test_range,
            'step_size': step_size,
            'target_metric': target_metric,
            'test_results': [],
            'critical_point': None,
            'warning_point': None,
            'stability_point': None
        }
        
        while current_val <= max_val:
            self.logger.info(f"測試 {parameter_name} = {current_val:.3f}")
            
            # 創建測試情境
            if 'speed' in parameter_name.lower():
                scenario_type = ScenarioType.SPEED_REDUCTION
                param_changes = {'speed_reduction_factor': current_val}
            elif 'staff' in parameter_name.lower():
                scenario_type = ScenarioType.STAFF_REDUCTION
                param_changes = {'staff_reduction_percentage': current_val}
            elif 'exception' in parameter_name.lower():
                scenario_type = ScenarioType.EXCEPTION_INCREASE
                param_changes = {'exception_frequency_multiplier': current_val}
            else:
                param_changes = {parameter_name: current_val}
                scenario_type = ScenarioType.CAPACITY_OPTIMIZATION
            
            scenario_config = ScenarioConfig(
                scenario_id=f"threshold_test_{parameter_name}_{current_val:.3f}",
                scenario_type=scenario_type,
                description=f"臨界值測試: {parameter_name} = {current_val:.3f}",
                parameter_changes=param_changes,
                test_duration_days=3,  # 較短的測試時間
                baseline_comparison=False,  # 不需要基準比較
                tags=['threshold_test']
            )
            
            # 執行測試
            result = self.run_scenario_analysis(scenario_config)
            
            # 記錄結果
            test_point = {
                'parameter_value': current_val,
                'metrics': result.scenario_metrics,
                'thresholds_breached': len(result.critical_thresholds_breached),
                'impact_level': result.impact_level.value if result.impact_level else 'UNKNOWN'
            }
            
            threshold_analysis['test_results'].append(test_point)
            
            # 檢查是否達到臨界點
            if not threshold_analysis['warning_point'] and test_point['thresholds_breached'] > 0:
                threshold_analysis['warning_point'] = current_val
            
            if not threshold_analysis['critical_point'] and test_point['impact_level'] in ['HIGH', 'SEVERE']:
                threshold_analysis['critical_point'] = current_val
            
            current_val += step_size
        
        # 分析結果
        if threshold_analysis['test_results']:
            # 找出穩定運作的最後一點
            for test_point in threshold_analysis['test_results']:
                if test_point['thresholds_breached'] == 0 and test_point['impact_level'] in ['MINIMAL', 'LOW']:
                    threshold_analysis['stability_point'] = test_point['parameter_value']
        
        self.logger.info(f"臨界值分析完成: {parameter_name}")
        self.logger.info(f"  穩定點: {threshold_analysis['stability_point']}")
        self.logger.info(f"  警告點: {threshold_analysis['warning_point']}")
        self.logger.info(f"  臨界點: {threshold_analysis['critical_point']}")
        
        return threshold_analysis
    
    def run_comprehensive_analysis(self, test_scenarios: List[str] = None) -> Dict[str, Any]:
        """執行綜合分析"""
        if test_scenarios is None:
            test_scenarios = [
                'speed_reduction_light',
                'speed_reduction_moderate', 
                'staff_reduction_10pct',
                'staff_reduction_25pct',
                'exception_increase_2x'
            ]
        
        comprehensive_results = {
            'analysis_timestamp': datetime.now(),
            'tested_scenarios': test_scenarios,
            'scenario_results': {},
            'comparative_analysis': {},
            'risk_matrix': {},
            'recommendations_summary': []
        }
        
        # 執行所有測試情境
        for scenario_name in test_scenarios:
            if scenario_name in self.scenario_templates:
                self.logger.info(f"執行綜合分析: {scenario_name}")
                
                scenario_config = self.scenario_templates[scenario_name]
                result = self.run_scenario_analysis(scenario_config)
                comprehensive_results['scenario_results'][scenario_name] = result
        
        # 比較分析
        comprehensive_results['comparative_analysis'] = self._perform_comparative_analysis(
            comprehensive_results['scenario_results']
        )
        
        # 風險矩陣
        comprehensive_results['risk_matrix'] = self._create_risk_matrix(
            comprehensive_results['scenario_results']
        )
        
        # 綜合建議
        comprehensive_results['recommendations_summary'] = self._generate_comprehensive_recommendations(
            comprehensive_results
        )
        
        return comprehensive_results
    
    def _perform_comparative_analysis(self, scenario_results: Dict[str, ScenarioResult]) -> Dict[str, Any]:
        """執行比較分析"""
        analysis = {
            'scenario_ranking': [],
            'metric_sensitivities': {},
            'common_issues': [],
            'best_case_scenario': None,
            'worst_case_scenario': None
        }
        
        # 按影響程度排序情境
        scenario_impacts = []
        for scenario_name, result in scenario_results.items():
            if result.impact_summary:
                impact_score = result.impact_summary.get('overall_impact_score', 0)
                scenario_impacts.append({
                    'scenario_name': scenario_name,
                    'impact_score': impact_score,
                    'impact_level': result.impact_level.value if result.impact_level else 'UNKNOWN'
                })
        
        # 排序
        scenario_impacts.sort(key=lambda x: x['impact_score'], reverse=True)
        analysis['scenario_ranking'] = scenario_impacts
        
        if scenario_impacts:
            analysis['worst_case_scenario'] = scenario_impacts[0]['scenario_name']
            analysis['best_case_scenario'] = scenario_impacts[-1]['scenario_name']
        
        # 找出共同問題
        common_metrics = defaultdict(int)
        for result in scenario_results.values():
            if result.impact_summary and 'degraded_metrics' in result.impact_summary:
                for metric in result.impact_summary['degraded_metrics']:
                    common_metrics[metric] += 1
        
        # 出現在多個情境中的問題
        total_scenarios = len(scenario_results)
        for metric, count in common_metrics.items():
            if count > total_scenarios * 0.5:  # 超過一半的情境都有問題
                analysis['common_issues'].append({
                    'metric': metric,
                    'affected_scenarios': count,
                    'frequency': round(count / total_scenarios * 100, 1)
                })
        
        return analysis
    
    def _create_risk_matrix(self, scenario_results: Dict[str, ScenarioResult]) -> Dict[str, Any]:
        """創建風險矩陣"""
        risk_matrix = {
            'high_risk_scenarios': [],
            'medium_risk_scenarios': [],
            'low_risk_scenarios': [],
            'risk_summary': {}
        }
        
        for scenario_name, result in scenario_results.items():
            risk_level = 'LOW'
            
            if result.impact_level in [ImpactLevel.HIGH, ImpactLevel.SEVERE]:
                risk_level = 'HIGH'
                risk_matrix['high_risk_scenarios'].append(scenario_name)
            elif result.impact_level == ImpactLevel.MODERATE:
                risk_level = 'MEDIUM'
                risk_matrix['medium_risk_scenarios'].append(scenario_name)
            else:
                risk_matrix['low_risk_scenarios'].append(scenario_name)
            
            risk_matrix['risk_summary'][scenario_name] = {
                'risk_level': risk_level,
                'impact_level': result.impact_level.value if result.impact_level else 'UNKNOWN',
                'thresholds_breached': len(result.critical_thresholds_breached),
                'overall_impact_score': result.impact_summary.get('overall_impact_score', 0) if result.impact_summary else 0
            }
        
        return risk_matrix
    
    def _generate_comprehensive_recommendations(self, comprehensive_results: Dict[str, Any]) -> List[str]:
        """生成綜合建議"""
        recommendations = []
        
        # 基於風險矩陣的建議
        risk_matrix = comprehensive_results.get('risk_matrix', {})
        high_risk_scenarios = risk_matrix.get('high_risk_scenarios', [])
        
        if high_risk_scenarios:
            recommendations.append(f"高風險情境 {len(high_risk_scenarios)} 個，需要重點關注和準備應對措施")
            recommendations.append("建議建立針對高風險情境的詳細應急計劃")
        
        # 基於比較分析的建議
        comparative = comprehensive_results.get('comparative_analysis', {})
        common_issues = comparative.get('common_issues', [])
        
        if common_issues:
            recommendations.append("發現多個情境中的共同弱點，建議優先改善:")
            for issue in common_issues[:3]:  # 只列出前3個
                recommendations.append(f"  - {issue['metric']} (影響 {issue['frequency']:.1f}% 的測試情境)")
        
        # 基於最差情境的建議
        worst_case = comparative.get('worst_case_scenario')
        if worst_case:
            recommendations.append(f"最嚴重影響情境: {worst_case}，建議制定專門的應對策略")
        
        return recommendations
    
    def get_scenario_result(self, scenario_id: str) -> Optional[ScenarioResult]:
        """取得情境分析結果"""
        return self.scenario_results.get(scenario_id)
    
    def list_available_templates(self) -> List[Dict[str, Any]]:
        """列出可用的情境模板"""
        templates = []
        
        for template_id, config in self.scenario_templates.items():
            templates.append({
                'template_id': template_id,
                'scenario_type': config.scenario_type.value,
                'description': config.description,
                'tags': config.tags,
                'parameter_changes': config.parameter_changes
            })
        
        return templates
    
    def export_analysis_report(self, scenario_ids: List[str] = None) -> pd.DataFrame:
        """匯出分析報告"""
        if scenario_ids is None:
            scenario_ids = list(self.scenario_results.keys())
        
        records = []
        
        for scenario_id in scenario_ids:
            if scenario_id in self.scenario_results:
                result = self.scenario_results[scenario_id]
                
                record = {
                    'scenario_id': scenario_id,
                    'scenario_type': result.scenario_config.scenario_type.value,
                    'description': result.scenario_config.description,
                    'execution_time': result.execution_time,
                    'simulation_duration_seconds': result.simulation_duration_seconds,
                    'impact_level': result.impact_level.value if result.impact_level else 'UNKNOWN',
                    'overall_impact_score': result.impact_summary.get('overall_impact_score', 0) if result.impact_summary else 0,
                    'thresholds_breached_count': len(result.critical_thresholds_breached),
                    'recommendations_count': len(result.recommendations)
                }
                
                # 添加關鍵指標
                if result.scenario_metrics:
                    for metric_name, value in result.scenario_metrics.items():
                        record[f'metric_{metric_name}'] = value
                
                records.append(record)
        
        return pd.DataFrame(records)