"""
ReportGenerator - 報告生成模組
負責生成分析報告和視覺化
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta, time
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, field
from enum import Enum
import json
import base64
from io import BytesIO
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from pathlib import Path

class ReportType(Enum):
    """報告類型枚舉"""
    WAVE_COMPLETION = "WAVE_COMPLETION"           # 波次完成報告
    REALTIME_DASHBOARD = "REALTIME_DASHBOARD"     # 實時狀態儀表板
    WHATIF_SUMMARY = "WHATIF_SUMMARY"             # What-if分析摘要
    VALIDATION_REPORT = "VALIDATION_REPORT"       # 驗證報告
    PERFORMANCE_ANALYSIS = "PERFORMANCE_ANALYSIS" # 性能分析報告
    EXCEPTION_ANALYSIS = "EXCEPTION_ANALYSIS"     # 異常分析報告
    WORKLOAD_REPORT = "WORKLOAD_REPORT"           # 工作量報告
    STAFF_UTILIZATION = "STAFF_UTILIZATION"       # 人員利用率報告
    SYSTEM_HEALTH = "SYSTEM_HEALTH"               # 系統健康報告
    COMPREHENSIVE = "COMPREHENSIVE"               # 綜合報告

class ReportFormat(Enum):
    """報告格式枚舉"""
    HTML = "HTML"
    PDF = "PDF"
    EXCEL = "EXCEL"
    JSON = "JSON"
    CSV = "CSV"

class ChartType(Enum):
    """圖表類型枚舉"""
    LINE_CHART = "LINE_CHART"
    BAR_CHART = "BAR_CHART"
    PIE_CHART = "PIE_CHART"
    SCATTER_PLOT = "SCATTER_PLOT"
    HEATMAP = "HEATMAP"
    HISTOGRAM = "HISTOGRAM"
    BOX_PLOT = "BOX_PLOT"
    AREA_CHART = "AREA_CHART"

@dataclass
class ReportConfig:
    """報告配置"""
    report_type: ReportType
    report_format: ReportFormat
    title: str
    subtitle: str = ""
    
    # 時間範圍
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    
    # 內容設定
    include_charts: bool = True
    include_tables: bool = True
    include_summary: bool = True
    include_recommendations: bool = True
    
    # 圖表設定
    chart_style: str = "seaborn"
    color_palette: str = "viridis"
    figure_size: Tuple[int, int] = (12, 8)
    dpi: int = 300
    
    # 其他設定
    language: str = "zh-TW"
    timezone: str = "Asia/Taipei"
    decimal_places: int = 2
    
    # 自定義參數
    custom_parameters: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ReportElement:
    """報告元素"""
    element_id: str
    element_type: str  # 'text', 'table', 'chart', 'image'
    title: str
    content: Any
    description: str = ""
    order: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class GeneratedReport:
    """生成的報告"""
    report_id: str
    config: ReportConfig
    
    # 報告內容
    elements: List[ReportElement] = field(default_factory=list)
    raw_data: Dict[str, Any] = field(default_factory=dict)
    
    # 元數據
    generation_time: datetime = field(default_factory=datetime.now)
    generation_duration_seconds: float = 0.0
    file_path: Optional[str] = None
    file_size_bytes: Optional[int] = None
    
    # 統計
    total_elements: int = 0
    chart_count: int = 0
    table_count: int = 0

class ReportGenerator:
    def __init__(self, simulation_engine, data_manager, system_state_tracker, 
                 wave_manager, exception_handler, daily_workload_manager, 
                 whatif_analyzer, validation_engine):
        """初始化報告生成器"""
        self.logger = logging.getLogger(__name__)
        
        # 關聯的管理器
        self.simulation_engine = simulation_engine
        self.data_manager = data_manager
        self.system_state_tracker = system_state_tracker
        self.wave_manager = wave_manager
        self.exception_handler = exception_handler
        self.daily_workload_manager = daily_workload_manager
        self.whatif_analyzer = whatif_analyzer
        self.validation_engine = validation_engine
        
        # 報告記錄
        self.generated_reports: Dict[str, GeneratedReport] = {}
        
        # 設定視覺化樣式
        self._setup_visualization_style()
        
        # 建立輸出目錄
        self.output_dir = Path("reports")
        self.output_dir.mkdir(exist_ok=True)
        
        self.logger.info("ReportGenerator 初始化完成")
    
    def _setup_visualization_style(self):
        """設定視覺化樣式"""
        plt.style.use('seaborn-v0_8')
        sns.set_palette("viridis")
        
        # 設定中文字體
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
        # 設定預設圖表大小
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['figure.dpi'] = 100
    
    def generate_report(self, config: ReportConfig) -> GeneratedReport:
        """生成報告"""
        start_time = datetime.now()
        report_id = f"{config.report_type.value}_{start_time.strftime('%Y%m%d_%H%M%S')}"
        
        self.logger.info(f"開始生成報告: {config.title}")
        
        # 創建報告物件
        report = GeneratedReport(
            report_id=report_id,
            config=config,
            generation_time=start_time
        )
        
        try:
            # 根據報告類型生成對應內容
            if config.report_type == ReportType.WAVE_COMPLETION:
                self._generate_wave_completion_report(report)
            elif config.report_type == ReportType.REALTIME_DASHBOARD:
                self._generate_realtime_dashboard(report)
            elif config.report_type == ReportType.WHATIF_SUMMARY:
                self._generate_whatif_summary(report)
            elif config.report_type == ReportType.VALIDATION_REPORT:
                self._generate_validation_report(report)
            elif config.report_type == ReportType.PERFORMANCE_ANALYSIS:
                self._generate_performance_analysis(report)
            elif config.report_type == ReportType.EXCEPTION_ANALYSIS:
                self._generate_exception_analysis(report)
            elif config.report_type == ReportType.WORKLOAD_REPORT:
                self._generate_workload_report(report)
            elif config.report_type == ReportType.STAFF_UTILIZATION:
                self._generate_staff_utilization_report(report)
            elif config.report_type == ReportType.SYSTEM_HEALTH:
                self._generate_system_health_report(report)
            elif config.report_type == ReportType.COMPREHENSIVE:
                self._generate_comprehensive_report(report)
            else:
                raise ValueError(f"不支援的報告類型: {config.report_type.value}")
            
            # 計算統計資訊
            report.total_elements = len(report.elements)
            report.chart_count = sum(1 for e in report.elements if e.element_type == 'chart')
            report.table_count = sum(1 for e in report.elements if e.element_type == 'table')
            
            # 生成報告檔案
            if config.report_format != ReportFormat.JSON:
                self._export_report(report)
            
            # 計算生成時間
            report.generation_duration_seconds = (datetime.now() - start_time).total_seconds()
            
            # 儲存報告
            self.generated_reports[report_id] = report
            
            self.logger.info(f" 報告生成完成: {config.title} ({report.total_elements} 個元素)")
            
        except Exception as e:
            self.logger.error(f"報告生成失敗: {str(e)}")
            raise
        
        return report
    
    def _generate_wave_completion_report(self, report: GeneratedReport):
        """生成波次完成報告"""
        config = report.config
        
        # 1. 報告摘要
        if config.include_summary:
            summary_data = self._collect_wave_summary_data()
            report.elements.append(ReportElement(
                element_id="wave_summary",
                element_type="text",
                title="波次完成摘要",
                content=self._format_wave_summary(summary_data),
                order=1
            ))
        
        # 2. 波次完成趨勢圖
        if config.include_charts:
            chart_data = self._prepare_wave_completion_chart_data()
            chart_content = self._create_wave_completion_chart(chart_data, config)
            report.elements.append(ReportElement(
                element_id="wave_completion_trend",
                element_type="chart",
                title="波次完成趨勢",
                content=chart_content,
                order=2
            ))
        
        # 3. 波次詳細資料表
        if config.include_tables:
            table_data = self._prepare_wave_details_table()
            report.elements.append(ReportElement(
                element_id="wave_details_table",
                element_type="table",
                title="波次詳細資料",
                content=table_data,
                order=3
            ))
        
        # 4. 建議
        if config.include_recommendations:
            recommendations = self._generate_wave_recommendations()
            report.elements.append(ReportElement(
                element_id="wave_recommendations",
                element_type="text",
                title="改善建議",
                content=recommendations,
                order=4
            ))
    
    def _generate_realtime_dashboard(self, report: GeneratedReport):
        """生成實時狀態儀表板"""
        config = report.config
        current_time = datetime.now()
        
        # 1. 系統概覽
        system_snapshot = self.system_state_tracker.capture_system_snapshot(current_time)
        report.elements.append(ReportElement(
            element_id="system_overview",
            element_type="text",
            title="系統概覽",
            content=self._format_system_overview(system_snapshot),
            order=1
        ))
        
        # 2. 即時指標儀表板
        if config.include_charts:
            dashboard_charts = self._create_dashboard_charts(system_snapshot, config)
            for i, (chart_id, chart_content) in enumerate(dashboard_charts.items()):
                report.elements.append(ReportElement(
                    element_id=f"dashboard_{chart_id}",
                    element_type="chart",
                    title=chart_content['title'],
                    content=chart_content['chart'],
                    order=2 + i
                ))
        
        # 3. 活躍項目表
        if config.include_tables:
            active_items_table = self._prepare_active_items_table(current_time)
            report.elements.append(ReportElement(
                element_id="active_items",
                element_type="table",
                title="活躍項目",
                content=active_items_table,
                order=10
            ))
    
    def _generate_whatif_summary(self, report: GeneratedReport):
        """生成What-if分析摘要"""
        config = report.config
        
        # 1. 分析摘要
        if config.include_summary:
            whatif_summary = self._collect_whatif_summary_data()
            report.elements.append(ReportElement(
                element_id="whatif_summary",
                element_type="text",
                title="What-if 分析摘要",
                content=self._format_whatif_summary(whatif_summary),
                order=1
            ))
        
        # 2. 情境比較圖表
        if config.include_charts:
            comparison_chart = self._create_scenario_comparison_chart(config)
            report.elements.append(ReportElement(
                element_id="scenario_comparison",
                element_type="chart",
                title="情境影響比較",
                content=comparison_chart,
                order=2
            ))
        
        # 3. 風險矩陣
        if config.include_charts:
            risk_matrix_chart = self._create_risk_matrix_chart(config)
            report.elements.append(ReportElement(
                element_id="risk_matrix",
                element_type="chart",
                title="風險矩陣",
                content=risk_matrix_chart,
                order=3
            ))
        
        # 4. 詳細結果表
        if config.include_tables:
            whatif_results_table = self._prepare_whatif_results_table()
            report.elements.append(ReportElement(
                element_id="whatif_results",
                element_type="table",
                title="What-if 分析結果",
                content=whatif_results_table,
                order=4
            ))
    
    def _generate_validation_report(self, report: GeneratedReport):
        """生成驗證報告"""
        config = report.config
        
        # 1. 驗證摘要
        if config.include_summary:
            validation_summary = self._collect_validation_summary_data()
            report.elements.append(ReportElement(
                element_id="validation_summary",
                element_type="text",
                title="驗證測試摘要",
                content=self._format_validation_summary(validation_summary),
                order=1
            ))
        
        # 2. 測試結果圖表
        if config.include_charts:
            test_results_chart = self._create_validation_results_chart(config)
            report.elements.append(ReportElement(
                element_id="test_results_chart",
                element_type="chart",
                title="測試結果分布",
                content=test_results_chart,
                order=2
            ))
        
        # 3. 信心度分析
        if config.include_charts:
            confidence_chart = self._create_confidence_analysis_chart(config)
            report.elements.append(ReportElement(
                element_id="confidence_analysis",
                element_type="chart",
                title="驗證信心度分析",
                content=confidence_chart,
                order=3
            ))
        
        # 4. 詳細測試結果
        if config.include_tables:
            validation_details_table = self._prepare_validation_details_table()
            report.elements.append(ReportElement(
                element_id="validation_details",
                element_type="table",
                title="詳細測試結果",
                content=validation_details_table,
                order=4
            ))
    
    def _generate_performance_analysis(self, report: GeneratedReport):
        """生成性能分析報告"""
        config = report.config
        
        # 1. 性能摘要
        if config.include_summary:
            performance_summary = self._collect_performance_summary_data()
            report.elements.append(ReportElement(
                element_id="performance_summary",
                element_type="text",
                title="性能分析摘要",
                content=self._format_performance_summary(performance_summary),
                order=1
            ))
        
        # 2. 關鍵指標趨勢
        if config.include_charts:
            metrics_trend_chart = self._create_metrics_trend_chart(config)
            report.elements.append(ReportElement(
                element_id="metrics_trend",
                element_type="chart",
                title="關鍵指標趨勢",
                content=metrics_trend_chart,
                order=2
            ))
        
        # 3. 利用率分析
        if config.include_charts:
            utilization_chart = self._create_utilization_analysis_chart(config)
            report.elements.append(ReportElement(
                element_id="utilization_analysis",
                element_type="chart",
                title="資源利用率分析",
                content=utilization_chart,
                order=3
            ))
        
        # 4. 性能統計表
        if config.include_tables:
            performance_stats_table = self._prepare_performance_statistics_table()
            report.elements.append(ReportElement(
                element_id="performance_statistics",
                element_type="table",
                title="性能統計數據",
                content=performance_stats_table,
                order=4
            ))
    
    def _generate_exception_analysis(self, report: GeneratedReport):
        """生成異常分析報告"""
        config = report.config
        
        # 1. 異常摘要
        if config.include_summary:
            exception_summary = self._collect_exception_summary_data()
            report.elements.append(ReportElement(
                element_id="exception_summary",
                element_type="text",
                title="異常分析摘要",
                content=self._format_exception_summary(exception_summary),
                order=1
            ))
        
        # 2. 異常類型分布
        if config.include_charts:
            exception_type_chart = self._create_exception_type_chart(config)
            report.elements.append(ReportElement(
                element_id="exception_types",
                element_type="chart",
                title="異常類型分布",
                content=exception_type_chart,
                order=2
            ))
        
        # 3. 異常處理時間分析
        if config.include_charts:
            handling_time_chart = self._create_exception_handling_time_chart(config)
            report.elements.append(ReportElement(
                element_id="handling_time_analysis",
                element_type="chart",
                title="異常處理時間分析",
                content=handling_time_chart,
                order=3
            ))
        
        # 4. 異常詳細記錄
        if config.include_tables:
            exception_details_table = self._prepare_exception_details_table()
            report.elements.append(ReportElement(
                element_id="exception_details",
                element_type="table",
                title="異常詳細記錄",
                content=exception_details_table,
                order=4
            ))
    
    def _generate_workload_report(self, report: GeneratedReport):
        """生成工作量報告"""
        config = report.config
        
        # 1. 工作量摘要
        if config.include_summary:
            workload_summary = self._collect_workload_summary_data()
            report.elements.append(ReportElement(
                element_id="workload_summary",
                element_type="text",
                title="工作量分析摘要",
                content=self._format_workload_summary(workload_summary),
                order=1
            ))
        
        # 2. 每日工作量趨勢
        if config.include_charts:
            workload_trend_chart = self._create_workload_trend_chart(config)
            report.elements.append(ReportElement(
                element_id="workload_trend",
                element_type="chart",
                title="每日工作量趨勢",
                content=workload_trend_chart,
                order=2
            ))
        
        # 3. 產能利用率分析
        if config.include_charts:
            capacity_utilization_chart = self._create_capacity_utilization_chart(config)
            report.elements.append(ReportElement(
                element_id="capacity_utilization",
                element_type="chart",
                title="產能利用率分析",
                content=capacity_utilization_chart,
                order=3
            ))
        
        # 4. 加班分析
        if config.include_charts:
            overtime_analysis_chart = self._create_overtime_analysis_chart(config)
            report.elements.append(ReportElement(
                element_id="overtime_analysis",
                element_type="chart",
                title="加班需求分析",
                content=overtime_analysis_chart,
                order=4
            ))
    
    def _generate_staff_utilization_report(self, report: GeneratedReport):
        """生成人員利用率報告"""
        config = report.config
        
        # 1. 人員利用率摘要
        if config.include_summary:
            staff_summary = self._collect_staff_utilization_summary()
            report.elements.append(ReportElement(
                element_id="staff_summary",
                element_type="text",
                title="人員利用率摘要",
                content=self._format_staff_summary(staff_summary),
                order=1
            ))
        
        # 2. 樓層別人員利用率
        if config.include_charts:
            floor_utilization_chart = self._create_floor_utilization_chart(config)
            report.elements.append(ReportElement(
                element_id="floor_utilization",
                element_type="chart",
                title="樓層別人員利用率",
                content=floor_utilization_chart,
                order=2
            ))
        
        # 3. 技能分析
        if config.include_charts:
            skill_analysis_chart = self._create_skill_analysis_chart(config)
            report.elements.append(ReportElement(
                element_id="skill_analysis",
                element_type="chart",
                title="技能效率分析",
                content=skill_analysis_chart,
                order=3
            ))
    
    def _generate_system_health_report(self, report: GeneratedReport):
        """生成系統健康報告"""
        config = report.config
        current_time = datetime.now()
        
        # 1. 系統健康摘要
        if config.include_summary:
            health_assessment = self.system_state_tracker._assess_system_health(current_time)
            report.elements.append(ReportElement(
                element_id="health_summary",
                element_type="text",
                title="系統健康狀況",
                content=self._format_health_summary(health_assessment),
                order=1
            ))
        
        # 2. 健康指標儀表板
        if config.include_charts:
            health_dashboard = self._create_health_dashboard(health_assessment, config)
            report.elements.append(ReportElement(
                element_id="health_dashboard",
                element_type="chart",
                title="系統健康指標",
                content=health_dashboard,
                order=2
            ))
        
        # 3. 問題分析
        if config.include_tables:
            issues_table = self._prepare_system_issues_table(health_assessment)
            report.elements.append(ReportElement(
                element_id="system_issues",
                element_type="table",
                title="系統問題分析",
                content=issues_table,
                order=3
            ))
    
    def _generate_comprehensive_report(self, report: GeneratedReport):
        """生成綜合報告"""
        config = report.config
        
        # 綜合報告包含所有主要分析
        self.logger.info("生成綜合報告...")
        
        # 1. 執行摘要
        executive_summary = self._create_executive_summary()
        report.elements.append(ReportElement(
            element_id="executive_summary",
            element_type="text",
            title="執行摘要",
            content=executive_summary,
            order=1
        ))
        
        # 2. 系統概覽
        if config.include_charts:
            system_overview_chart = self._create_system_overview_chart(config)
            report.elements.append(ReportElement(
                element_id="system_overview_chart",
                element_type="chart",
                title="系統整體概覽",
                content=system_overview_chart,
                order=2
            ))
        
        # 3. 關鍵績效指標
        if config.include_charts:
            kpi_dashboard = self._create_kpi_dashboard(config)
            report.elements.append(ReportElement(
                element_id="kpi_dashboard",
                element_type="chart",
                title="關鍵績效指標",
                content=kpi_dashboard,
                order=3
            ))
        
        # 4. 詳細分析結果（簡化版）
        analysis_results = self._prepare_comprehensive_analysis_table()
        report.elements.append(ReportElement(
            element_id="analysis_results",
            element_type="table",
            title="詳細分析結果",
            content=analysis_results,
            order=4
        ))
        
        # 5. 整體建議
        if config.include_recommendations:
            comprehensive_recommendations = self._generate_comprehensive_recommendations()
            report.elements.append(ReportElement(
                element_id="comprehensive_recommendations",
                element_type="text",
                title="整體改善建議",
                content=comprehensive_recommendations,
                order=5
            ))
    
    # === 數據收集方法 ===
    
    def _collect_wave_summary_data(self) -> Dict[str, Any]:
        """收集波次摘要數據"""
        summary = self.wave_manager.get_active_waves_summary(datetime.now())
        history = self.wave_manager.get_wave_history_summary()
        
        return {
            'active_summary': summary,
            'history_summary': history,
            'total_waves_processed': len(self.wave_manager.wave_history),
            'current_active_waves': len(self.wave_manager.active_waves)
        }
    
    def _collect_whatif_summary_data(self) -> Dict[str, Any]:
        """收集What-if分析摘要數據"""
        return {
            'total_scenarios_tested': len(self.whatif_analyzer.scenario_results),
            'scenario_results': self.whatif_analyzer.scenario_results,
            'available_templates': len(self.whatif_analyzer.scenario_templates)
        }
    
    def _collect_validation_summary_data(self) -> Dict[str, Any]:
        """收集驗證摘要數據"""
        return {
            'total_tests_run': len(self.validation_engine.validation_reports),
            'validation_reports': self.validation_engine.validation_reports,
            'available_tests': len(self.validation_engine.predefined_tests)
        }
    
    def _collect_performance_summary_data(self) -> Dict[str, Any]:
        """收集性能摘要數據"""
        metrics_history = self.system_state_tracker.metrics_history
        
        if not metrics_history:
            return {'error': '沒有性能數據'}
        
        latest_metrics = metrics_history[-1]
        
        return {
            'latest_metrics': latest_metrics.__dict__,
            'metrics_count': len(metrics_history),
            'time_span_hours': (metrics_history[-1].timestamp - metrics_history[0].timestamp).total_seconds() / 3600
        }
    
    def _collect_exception_summary_data(self) -> Dict[str, Any]:
        """收集異常摘要數據"""
        return self.exception_handler.get_exception_summary(datetime.now())
    
    def _collect_workload_summary_data(self) -> Dict[str, Any]:
        """收集工作量摘要數據"""
        workload_data = {
            'daily_workloads_count': len(self.daily_workload_manager.daily_workloads),
            'historical_workloads_count': len(self.daily_workload_manager.historical_workloads)
        }
        
        if self.daily_workload_manager.daily_workloads:
            latest_date = max(self.daily_workload_manager.daily_workloads.keys())
            latest_workload = self.daily_workload_manager.daily_workloads[latest_date]
            workload_data['latest_workload'] = {
                'date': latest_date,
                'total_hours': latest_workload.total_workload_hours,
                'utilization': latest_workload.capacity_utilization,
                'overtime_required': latest_workload.overtime_required
            }
        
        return workload_data
    
    def _collect_staff_utilization_summary(self) -> Dict[str, Any]:
        """收集人員利用率摘要"""
        current_time = datetime.now()
        staff_summary = self.system_state_tracker._get_staff_summary(current_time)
        
        return staff_summary
    
    # === 圖表創建方法 ===
    
    def _create_wave_completion_chart(self, chart_data: Dict, config: ReportConfig) -> str:
        """創建波次完成趨勢圖"""
        fig, ax = plt.subplots(figsize=config.figure_size)
        
        # 模擬數據（實際使用時應從 chart_data 取得）
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        completed_waves = np.cumsum(np.random.poisson(3, 10))
        
        ax.plot(dates, completed_waves, marker='o', linewidth=2, markersize=6)
        ax.set_title('波次完成趨勢', fontsize=16, fontweight='bold')
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel('累計完成波次數', fontsize=12)
        ax.grid(True, alpha=0.3)
        
        # 格式化日期軸
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_dashboard_charts(self, system_snapshot: Dict, config: ReportConfig) -> Dict[str, Dict]:
        """創建儀表板圖表"""
        charts = {}
        
        # 1. 工作站利用率圓餅圖
        fig1, ax1 = plt.subplots(figsize=(8, 6))
        
        ws_summary = system_snapshot.get('workstation_summary', {})
        status_dist = ws_summary.get('status_distribution', {})
        
        if status_dist:
            labels = list(status_dist.keys())
            values = list(status_dist.values())
            colors = plt.cm.Set3(np.linspace(0, 1, len(labels)))
            
            wedges, texts, autotexts = ax1.pie(values, labels=labels, autopct='%1.1f%%', 
                                              colors=colors, startangle=90)
            ax1.set_title('工作站狀態分布', fontsize=14, fontweight='bold')
        
        charts['workstation_status'] = {
            'title': '工作站狀態分布',
            'chart': self._chart_to_base64(fig1)
        }
        
        # 2. 系統性能指標
        fig2, ((ax2_1, ax2_2), (ax2_3, ax2_4)) = plt.subplots(2, 2, figsize=config.figure_size)
        
        # 模擬指標數據
        metrics = ['工作站利用率', '任務完成率', '人員利用率', '系統效率']
        values = [75, 85, 68, 82]  # 實際使用時應從系統數據取得
        
        for ax, metric, value in zip([ax2_1, ax2_2, ax2_3, ax2_4], metrics, values):
            ax.bar([metric], [value], color='skyblue', alpha=0.7)
            ax.set_ylim(0, 100)
            ax.set_ylabel('百分比 (%)')
            ax.set_title(f'{metric}: {value}%')
            
            # 添加目標線
            ax.axhline(y=80, color='red', linestyle='--', alpha=0.7, label='目標')
            ax.legend()
        
        plt.tight_layout()
        charts['performance_metrics'] = {
            'title': '系統性能指標',
            'chart': self._chart_to_base64(fig2)
        }
        
        return charts
    
    def _create_scenario_comparison_chart(self, config: ReportConfig) -> str:
        """創建情境比較圖表"""
        fig, ax = plt.subplots(figsize=config.figure_size)
        
        # 從 whatif_analyzer 取得結果
        scenario_results = self.whatif_analyzer.scenario_results
        
        if scenario_results:
            scenarios = list(scenario_results.keys())[:5]  # 最多顯示5個情境
            impact_scores = []
            
            for scenario_id in scenarios:
                result = scenario_results[scenario_id]
                if result.impact_summary:
                    impact_scores.append(result.impact_summary.get('overall_impact_score', 0))
                else:
                    impact_scores.append(0)
            
            bars = ax.bar(scenarios, impact_scores, color='coral', alpha=0.7)
            ax.set_title('情境影響分數比較', fontsize=16, fontweight='bold')
            ax.set_xlabel('測試情境', fontsize=12)
            ax.set_ylabel('影響分數', fontsize=12)
            ax.tick_params(axis='x', rotation=45)
            
            # 添加數值標籤
            for bar, score in zip(bars, impact_scores):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                       f'{score:.1f}', ha='center', va='bottom')
        else:
            ax.text(0.5, 0.5, '無What-if分析數據', ha='center', va='center', 
                   transform=ax.transAxes, fontsize=14)
            ax.set_title('情境影響分數比較', fontsize=16, fontweight='bold')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_risk_matrix_chart(self, config: ReportConfig) -> str:
        """創建風險矩陣圖表"""
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # 模擬風險矩陣數據
        risk_levels = ['低風險', '中風險', '高風險']
        scenario_counts = [8, 3, 2]  # 實際應從 whatif_analyzer 取得
        
        colors = ['green', 'orange', 'red']
        bars = ax.bar(risk_levels, scenario_counts, color=colors, alpha=0.7)
        
        ax.set_title('情境風險分布矩陣', fontsize=16, fontweight='bold')
        ax.set_xlabel('風險等級', fontsize=12)
        ax.set_ylabel('情境數量', fontsize=12)
        
        # 添加數值標籤
        for bar, count in zip(bars, scenario_counts):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                   str(count), ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_validation_results_chart(self, config: ReportConfig) -> str:
        """創建驗證結果圖表"""
        fig, ax = plt.subplots(figsize=config.figure_size)
        
        # 從驗證引擎取得結果
        validation_reports = self.validation_engine.validation_reports
        
        if validation_reports:
            result_counts = defaultdict(int)
            for report in validation_reports.values():
                result_counts[report.result.value] += 1
            
            results = list(result_counts.keys())
            counts = list(result_counts.values())
            colors = {'PASS': 'green', 'FAIL': 'red', 'WARNING': 'orange', 'INCONCLUSIVE': 'gray'}
            chart_colors = [colors.get(result, 'blue') for result in results]
            
            bars = ax.bar(results, counts, color=chart_colors, alpha=0.7)
            ax.set_title('驗證測試結果分布', fontsize=16, fontweight='bold')
            ax.set_xlabel('測試結果', fontsize=12)
            ax.set_ylabel('測試數量', fontsize=12)
            
            # 添加數值標籤
            for bar, count in zip(bars, counts):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                       str(count), ha='center', va='bottom', fontweight='bold')
        else:
            ax.text(0.5, 0.5, '無驗證測試數據', ha='center', va='center', 
                   transform=ax.transAxes, fontsize=14)
            ax.set_title('驗證測試結果分布', fontsize=16, fontweight='bold')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_confidence_analysis_chart(self, config: ReportConfig) -> str:
        """創建信心度分析圖表"""
        fig, ax = plt.subplots(figsize=config.figure_size)
        
        validation_reports = self.validation_engine.validation_reports
        
        if validation_reports:
            confidence_scores = [report.confidence_score for report in validation_reports.values()]
            test_names = [f"Test {i+1}" for i in range(len(confidence_scores))]
            
            bars = ax.bar(test_names, confidence_scores, color='lightblue', alpha=0.7)
            ax.set_title('驗證測試信心度分析', fontsize=16, fontweight='bold')
            ax.set_xlabel('測試項目', fontsize=12)
            ax.set_ylabel('信心度分數', fontsize=12)
            ax.set_ylim(0, 1)
            ax.tick_params(axis='x', rotation=45)
            
            # 添加信心度閾值線
            ax.axhline(y=0.8, color='green', linestyle='--', alpha=0.7, label='高信心度 (0.8)')
            ax.axhline(y=0.6, color='orange', linestyle='--', alpha=0.7, label='中信心度 (0.6)')
            ax.legend()
            
            # 添加數值標籤
            for bar, score in zip(bars, confidence_scores):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                       f'{score:.2f}', ha='center', va='bottom')
        else:
            ax.text(0.5, 0.5, '無驗證信心度數據', ha='center', va='center', 
                   transform=ax.transAxes, fontsize=14)
            ax.set_title('驗證測試信心度分析', fontsize=16, fontweight='bold')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_metrics_trend_chart(self, config: ReportConfig) -> str:
        """創建指標趨勢圖表"""
        fig, ax = plt.subplots(figsize=config.figure_size)
        
        metrics_history = self.system_state_tracker.metrics_history
        
        if metrics_history and len(metrics_history) > 1:
            timestamps = [m.timestamp for m in metrics_history]
            workstation_util = [m.workstation_utilization for m in metrics_history]
            task_completion = [m.task_completion_rate for m in metrics_history]
            overall_efficiency = [m.overall_efficiency for m in metrics_history]
            
            ax.plot(timestamps, workstation_util, label='工作站利用率', marker='o', linewidth=2)
            ax.plot(timestamps, task_completion, label='任務完成率', marker='s', linewidth=2)
            ax.plot(timestamps, overall_efficiency, label='整體效率', marker='^', linewidth=2)
            
            ax.set_title('系統關鍵指標趨勢', fontsize=16, fontweight='bold')
            ax.set_xlabel('時間', fontsize=12)
            ax.set_ylabel('百分比 (%)', fontsize=12)
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # 格式化時間軸
            if len(timestamps) > 10:
                ax.xaxis.set_major_locator(plt.MaxNLocator(10))
            
        else:
            ax.text(0.5, 0.5, '指標數據不足', ha='center', va='center', 
                   transform=ax.transAxes, fontsize=14)
            ax.set_title('系統關鍵指標趨勢', fontsize=16, fontweight='bold')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_utilization_analysis_chart(self, config: ReportConfig) -> str:
        """創建利用率分析圖表"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=config.figure_size)
        
        # 工作站利用率 (左圖)
        current_time = datetime.now()
        ws_summary = self.system_state_tracker.track_station_status(current_time)
        
        floor_util = ws_summary.get('utilization_by_floor', {})
        if floor_util:
            floors = [f"{floor}F" for floor in floor_util.keys()]
            utilizations = list(floor_util.values())
            
            bars1 = ax1.bar(floors, utilizations, color='lightcoral', alpha=0.7)
            ax1.set_title('各樓層工作站利用率', fontweight='bold')
            ax1.set_ylabel('利用率 (%)')
            ax1.set_ylim(0, 100)
            
            # 添加目標線
            ax1.axhline(y=80, color='red', linestyle='--', alpha=0.7, label='目標 80%')
            ax1.legend()
            
            # 添加數值標籤
            for bar, util in zip(bars1, utilizations):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{util:.1f}%', ha='center', va='bottom')
        
        # 人員利用率 (右圖)
        staff_summary = self.system_state_tracker._get_staff_summary(current_time)
        staff_by_floor = staff_summary.get('staff_by_floor', {})
        active_by_floor = staff_summary.get('active_by_floor', {})
        
        if staff_by_floor and active_by_floor:
            floors = [f"{floor}F" for floor in staff_by_floor.keys() if floor != 'unknown']
            staff_utilizations = []
            
            for floor in staff_by_floor.keys():
                if floor != 'unknown':
                    total = staff_by_floor[floor]
                    active = active_by_floor.get(floor, 0)
                    util = (active / total * 100) if total > 0 else 0
                    staff_utilizations.append(util)
            
            if floors and staff_utilizations:
                bars2 = ax2.bar(floors, staff_utilizations, color='lightgreen', alpha=0.7)
                ax2.set_title('各樓層人員利用率', fontweight='bold')
                ax2.set_ylabel('利用率 (%)')
                ax2.set_ylim(0, 100)
                
                # 添加目標線
                ax2.axhline(y=75, color='red', linestyle='--', alpha=0.7, label='目標 75%')
                ax2.legend()
                
                # 添加數值標籤
                for bar, util in zip(bars2, staff_utilizations):
                    height = bar.get_height()
                    ax2.text(bar.get_x() + bar.get_width()/2., height + 1,
                            f'{util:.1f}%', ha='center', va='bottom')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    # === 格式化方法 ===
    
    def _format_wave_summary(self, summary_data: Dict) -> str:
        """格式化波次摘要"""
        active_summary = summary_data.get('active_summary', {})
        history_summary = summary_data.get('history_summary', {})
        
        text = f"""
        ## 波次執行摘要
        
        **活躍波次狀況：**
        - 當前活躍波次：{active_summary.get('total_active_waves', 0)} 個
        - 整體進度：{active_summary.get('overall_progress_percent', 0):.1f}%
        - 進行中任務：{active_summary.get('total_tasks_in_progress', 0)} 個
        - 已完成任務：{active_summary.get('total_completed_tasks', 0)} 個
        
        **歷史執行績效：**
        - 總計完成波次：{history_summary.get('total_completed_waves', 0)} 個
        - 平均波次時間：{history_summary.get('average_wave_duration', 0):.1f} 分鐘
        - 總處理任務：{history_summary.get('total_tasks_completed', 0)} 個
        
        **波次類型分布：**
        """
        
        waves_by_type = active_summary.get('waves_by_type', {})
        for wave_type, count in waves_by_type.items():
            text += f"\n- {wave_type}: {count} 個"
        
        return text
    
    def _format_whatif_summary(self, summary_data: Dict) -> str:
        """格式化What-if分析摘要"""
        scenario_results = summary_data.get('scenario_results', {})
        
        text = f"""
        ## What-if 分析摘要
        
        **測試概況：**
        - 總測試情境：{summary_data.get('total_scenarios_tested', 0)} 個
        - 可用模板：{summary_data.get('available_templates', 0)} 個
        
        **風險評估：**
        """
        
        risk_levels = defaultdict(int)
        for result in scenario_results.values():
            if result.impact_level:
                risk_levels[result.impact_level.value] += 1
        
        for level, count in risk_levels.items():
            text += f"\n- {level} 影響情境：{count} 個"
        
        text += f"""
        
        **關鍵發現：**
        - 系統對參數變化的敏感性適中
        - 人員短缺是主要風險因子
        - 建議建立應急預案
        """
        
        return text
    
    def _format_validation_summary(self, summary_data: Dict) -> str:
        """格式化驗證摘要"""
        validation_reports = summary_data.get('validation_reports', {})
        
        text = f"""
        ## 驗證測試摘要
        
        **測試執行狀況：**
        - 總執行測試：{summary_data.get('total_tests_run', 0)} 個
        - 可用測試：{summary_data.get('available_tests', 0)} 個
        
        **測試結果分布：**
        """
        
        result_counts = defaultdict(int)
        for report in validation_reports.values():
            result_counts[report.result.value] += 1
        
        for result, count in result_counts.items():
            text += f"\n- {result}: {count} 個"
        
        # 計算平均信心度
        confidence_scores = [r.confidence_score for r in validation_reports.values()]
        avg_confidence = np.mean(confidence_scores) if confidence_scores else 0
        
        text += f"""
        
        **信心度評估：**
        - 平均信心度：{avg_confidence:.2f}
        - 高信心度測試：{sum(1 for s in confidence_scores if s >= 0.8)} 個
        
        **驗證結論：**
        - 模擬系統驗證結果{"良好" if avg_confidence >= 0.8 else "需要改進"}
        - 建議{"投入生產使用" if avg_confidence >= 0.8 else "進一步調整和驗證"}
        """
        
        return text
    
    def _format_performance_summary(self, summary_data: Dict) -> str:
        """格式化性能摘要"""
        latest_metrics = summary_data.get('latest_metrics', {})
        
        text = f"""
        ## 系統性能分析摘要
        
        **當前性能指標：**
        - 工作站利用率：{latest_metrics.get('workstation_utilization', 0):.1f}%
        - 任務完成率：{latest_metrics.get('task_completion_rate', 0):.1f}%
        - 人員利用率：{latest_metrics.get('staff_utilization', 0):.1f}%
        - 整體效率：{latest_metrics.get('overall_efficiency', 0):.1f}%
        
        **數據統計：**
        - 監控時長：{summary_data.get('time_span_hours', 0):.1f} 小時
        - 數據點數：{summary_data.get('metrics_count', 0)} 個
        
        **性能評估：**
        """
        
        # 性能評估邏輯
        efficiency = latest_metrics.get('overall_efficiency', 0)
        if efficiency >= 80:
            text += "\n- 系統性能優秀"
        elif efficiency >= 60:
            text += "\n- 系統性能良好"
        else:
            text += "\n- 系統性能需要改進"
        
        return text
    
    def _format_exception_summary(self, summary_data: Dict) -> str:
        """格式化異常摘要"""
        text = f"""
        ## 異常處理分析摘要
        
        **當前異常狀況：**
        - 活躍異常：{summary_data.get('active_exceptions_count', 0)} 個
        - 已解決異常：{summary_data.get('resolved_exceptions_count', 0)} 個
        - 今日總異常：{summary_data.get('total_exceptions_today', 0)} 個
        
        **處理效率：**
        - 平均處理時間：{summary_data.get('handling_efficiency', {}).get('avg_handling_time', 0):.1f} 分鐘
        - 準時解決率：{summary_data.get('handling_efficiency', {}).get('on_time_resolution_rate', 0):.1f}%
        
        **資源利用：**
        - 主管利用率：{summary_data.get('resource_utilization', {}).get('leader_utilization_rate', 0):.1f}%
        - 可用主管：{summary_data.get('resource_utilization', {}).get('available_leaders', 0)} 人
        """
        
        # 異常類型分布
        exceptions_by_type = summary_data.get('exceptions_by_type', {})
        if exceptions_by_type:
            text += "\n\n**異常類型分布：**"
            for exc_type, count in exceptions_by_type.items():
                text += f"\n- {exc_type}: {count} 個"
        
        return text
    
    def _format_workload_summary(self, summary_data: Dict) -> str:
        """格式化工作量摘要"""
        latest_workload = summary_data.get('latest_workload', {})
        
        text = f"""
        ## 工作量分析摘要
        
        **數據統計：**
        - 每日工作量記錄：{summary_data.get('daily_workloads_count', 0)} 日
        - 歷史工作量記錄：{summary_data.get('historical_workloads_count', 0)} 日
        
        **最新工作量狀況 ({latest_workload.get('date', 'N/A')})：**
        - 總工作時數：{latest_workload.get('total_hours', 0):.1f} 小時
        - 產能利用率：{latest_workload.get('utilization', 0):.1%}
        - 需要加班：{'是' if latest_workload.get('overtime_required', False) else '否'}
        
        **趨勢分析：**
        - 工作量呈現{"上升" if latest_workload.get('utilization', 0) > 0.8 else "穩定"}趨勢
        - 產能利用率{"偏高" if latest_workload.get('utilization', 0) > 0.9 else "適中"}
        """
        
        return text
    
    def _format_staff_summary(self, summary_data: Dict) -> str:
        """格式化人員摘要"""
        text = f"""
        ## 人員利用率分析摘要
        
        **人員配置：**
        - 總在職人員：{summary_data.get('total_staff', 0)} 人
        - 當前工作人員：{summary_data.get('active_staff', 0)} 人
        - 空閒人員：{summary_data.get('idle_staff', 0)} 人
        - 整體利用率：{summary_data.get('utilization_rate', 0):.1f}%
        
        **主管資源：**
        - 可用主管：{summary_data.get('leaders_available', 0)} 人
        - 忙碌主管：{summary_data.get('leaders_busy', 0)} 人
        """
        
        # 樓層分布
        staff_by_floor = summary_data.get('staff_by_floor', {})
        active_by_floor = summary_data.get('active_by_floor', {})
        
        if staff_by_floor:
            text += "\n\n**樓層人員分布：**"
            for floor, total in staff_by_floor.items():
                if floor != 'unknown':
                    active = active_by_floor.get(floor, 0)
                    util = (active / total * 100) if total > 0 else 0
                    text += f"\n- {floor}F: {active}/{total} 人 ({util:.1f}%)"
        
        return text
    
    def _format_health_summary(self, health_assessment: Dict) -> str:
        """格式化健康摘要"""
        text = f"""
        ## 系統健康狀況報告
        
        **整體評估：**
        - 健康狀態：{health_assessment.get('overall_status', 'UNKNOWN')}
        - 健康分數：{health_assessment.get('score', 0)}/100
        
        **發現的問題：**
        """
        
        issues = health_assessment.get('issues', [])
        warnings = health_assessment.get('warnings', [])
        
        if issues:
            for issue in issues:
                text += f"\n-  {issue}"
        
        if warnings:
            text += "\n\n**警告事項：**"
            for warning in warnings:
                text += f"\n- ️ {warning}"
        
        recommendations = health_assessment.get('recommendations', [])
        if recommendations:
            text += "\n\n**改善建議：**"
            for rec in recommendations:
                text += f"\n-  {rec}"
        
        return text
    
    # === 表格準備方法 ===
    
    def _prepare_wave_details_table(self) -> pd.DataFrame:
        """準備波次詳細資料表"""
        data = []
        
        # 從 wave_manager 取得波次資料
        for wave_id, wave in self.wave_manager.waves.items():
            data.append({
                '波次ID': wave_id,
                '波次類型': wave.wave_type.value,
                '優先權': wave.priority_level,
                '狀態': wave.status.value,
                '總任務數': wave.total_tasks,
                '完成任務數': wave.completed_tasks,
                '進度': f"{(wave.completed_tasks/wave.total_tasks*100):.1f}%" if wave.total_tasks > 0 else "0%",
                '分配工作站': len(wave.assigned_workstations),
                '開始時間': wave.actual_start_time.strftime('%H:%M:%S') if wave.actual_start_time else '',
                '預計完成': wave.estimated_completion_time.strftime('%H:%M:%S') if wave.estimated_completion_time else ''
            })
        
        return pd.DataFrame(data)
    
    def _prepare_active_items_table(self, current_time: datetime) -> pd.DataFrame:
        """準備活躍項目表"""
        data = []
        
        # 活躍任務
        for task_id, task_state in self.system_state_tracker.current_state.get('TASK', {}).items():
            if task_state.get('status') in ['ASSIGNED', 'IN_PROGRESS']:
                data.append({
                    '項目類型': '任務',
                    '項目ID': task_id,
                    '狀態': task_state.get('status', ''),
                    '優先權': task_state.get('priority_level', ''),
                    '進度': f"{task_state.get('progress_percent', 0):.1f}%",
                    '分配工作站': task_state.get('assigned_station', ''),
                    '剩餘時間': f"{task_state.get('remaining_minutes', 0):.1f} 分鐘"
                })
        
        # 活躍異常
        for exc_id, exc_state in self.system_state_tracker.current_state.get('EXCEPTION', {}).items():
            if exc_state.get('status') in ['ASSIGNED', 'IN_PROGRESS']:
                data.append({
                    '項目類型': '異常',
                    '項目ID': exc_id,
                    '狀態': exc_state.get('status', ''),
                    '優先權': exc_state.get('priority', ''),
                    '進度': f"{exc_state.get('progress_percent', 0):.1f}%",
                    '分配工作站': exc_state.get('handling_station', ''),
                    '剩餘時間': f"{exc_state.get('remaining_time', 0):.1f} 分鐘"
                })
        
        return pd.DataFrame(data)
    
    def _prepare_whatif_results_table(self) -> pd.DataFrame:
        """準備What-if分析結果表"""
        data = []
        
        for scenario_id, result in self.whatif_analyzer.scenario_results.items():
            data.append({
                '情境ID': scenario_id,
                '情境類型': result.scenario_config.scenario_type.value,
                '描述': result.scenario_config.description,
                '影響等級': result.impact_level.value if result.impact_level else 'UNKNOWN',
                '影響分數': result.impact_summary.get('overall_impact_score', 0) if result.impact_summary else 0,
                '臨界值突破': len(result.critical_thresholds_breached),
                '執行時間': f"{result.simulation_duration_seconds:.1f} 秒",
                '建議數量': len(result.recommendations)
            })
        
        return pd.DataFrame(data)
    
    def _prepare_validation_details_table(self) -> pd.DataFrame:
        """準備驗證詳細結果表"""
        data = []
        
        for test_id, report in self.validation_engine.validation_reports.items():
            data.append({
                '測試ID': test_id,
                '驗證類型': report.validation_type.value,
                '功能類型': report.function_type.value,
                '測試結果': report.result.value,
                '信心度': f"{report.confidence_score:.2f}",
                '執行時間': f"{report.execution_duration_seconds:.1f} 秒",
                '發現數量': len(report.findings),
                '警告數量': len(report.warnings),
                '建議數量': len(report.recommendations)
            })
        
        return pd.DataFrame(data)
    
    def _prepare_performance_statistics_table(self) -> pd.DataFrame:
        """準備性能統計表"""
        metrics_history = self.system_state_tracker.metrics_history
        
        if not metrics_history:
            return pd.DataFrame()
        
        data = []
        
        # 計算各指標的統計量
        metrics_data = {
            '工作站利用率': [m.workstation_utilization for m in metrics_history],
            '任務完成率': [m.task_completion_rate for m in metrics_history],
            '人員利用率': [m.staff_utilization for m in metrics_history],
            '整體效率': [m.overall_efficiency for m in metrics_history],
            '異常數量': [m.exception_count for m in metrics_history]
        }
        
        for metric_name, values in metrics_data.items():
            if values:
                data.append({
                    '指標名稱': metric_name,
                    '平均值': f"{np.mean(values):.2f}",
                    '最大值': f"{np.max(values):.2f}",
                    '最小值': f"{np.min(values):.2f}",
                    '標準差': f"{np.std(values):.2f}",
                    '最新值': f"{values[-1]:.2f}"
                })
        
        return pd.DataFrame(data)
    
    def _prepare_exception_details_table(self) -> pd.DataFrame:
        """準備異常詳細記錄表"""
        # 使用異常處理器的匯出功能
        exception_log = self.exception_handler.export_exception_log()
        
        if not exception_log.empty:
            # 選擇關鍵欄位並重命名
            columns_mapping = {
                'exception_id': '異常ID',
                'exception_type': '異常類型',
                'priority': '優先權',
                'status': '狀態',
                'detection_time': '檢測時間',
                'resolution_time': '解決時間',
                'estimated_handling_time': '預估處理時間',
                'actual_handling_time': '實際處理時間'
            }
            
            result_df = exception_log[list(columns_mapping.keys())].copy()
            result_df = result_df.rename(columns=columns_mapping)
            
            # 格式化時間欄位
            for col in ['檢測時間', '解決時間']:
                if col in result_df.columns:
                    result_df[col] = pd.to_datetime(result_df[col]).dt.strftime('%m/%d %H:%M')
            
            return result_df
        else:
            return pd.DataFrame()
    
    def _prepare_system_issues_table(self, health_assessment: Dict) -> pd.DataFrame:
        """準備系統問題表"""
        data = []
        
        issues = health_assessment.get('issues', [])
        warnings = health_assessment.get('warnings', [])
        recommendations = health_assessment.get('recommendations', [])
        
        for issue in issues:
            data.append({
                '問題類型': '嚴重問題',
                '描述': issue,
                '優先權': '高',
                '狀態': '需立即處理'
            })
        
        for warning in warnings:
            data.append({
                '問題類型': '警告',
                '描述': warning,
                '優先權': '中',
                '狀態': '需關注'
            })
        
        for rec in recommendations:
            data.append({
                '問題類型': '建議',
                '描述': rec,
                '優先權': '低',
                '狀態': '可改善'
            })
        
        return pd.DataFrame(data)
    
    def _prepare_comprehensive_analysis_table(self) -> pd.DataFrame:
        """準備綜合分析結果表"""
        data = []
        
        # 系統整體指標
        if self.system_state_tracker.metrics_history:
            latest_metrics = self.system_state_tracker.metrics_history[-1]
            data.append({
                '分析類別': '系統性能',
                '項目': '工作站利用率',
                '數值': f"{latest_metrics.workstation_utilization:.1f}%",
                '狀態': '正常' if latest_metrics.workstation_utilization < 90 else '偏高',
                '建議': '持續監控' if latest_metrics.workstation_utilization < 90 else '考慮增加工作站'
            })
            
            data.append({
                '分析類別': '系統性能',
                '項目': '整體效率',
                '數值': f"{latest_metrics.overall_efficiency:.1f}%",
                '狀態': '優秀' if latest_metrics.overall_efficiency >= 80 else '需改進',
                '建議': '保持現況' if latest_metrics.overall_efficiency >= 80 else '分析瓶頸原因'
            })
        
        # What-if分析結果
        if self.whatif_analyzer.scenario_results:
            high_risk_scenarios = [
                r for r in self.whatif_analyzer.scenario_results.values() 
                if r.impact_level and r.impact_level.value in ['HIGH', 'SEVERE']
            ]
            
            data.append({
                '分析類別': 'What-if分析',
                '項目': '高風險情境',
                '數值': f"{len(high_risk_scenarios)} 個",
                '狀態': '需關注' if len(high_risk_scenarios) > 0 else '良好',
                '建議': '制定應急計劃' if len(high_risk_scenarios) > 0 else '持續監控'
            })
        
        # 驗證結果
        if self.validation_engine.validation_reports:
            failed_tests = [
                r for r in self.validation_engine.validation_reports.values() 
                if r.result.value == 'FAIL'
            ]
            
            data.append({
                '分析類別': '驗證測試',
                '項目': '失敗測試',
                '數值': f"{len(failed_tests)} 個",
                '狀態': '良好' if len(failed_tests) == 0 else '需改進',
                '建議': '系統可用' if len(failed_tests) == 0 else '修正失敗項目'
            })
        
        return pd.DataFrame(data)
    
    # === 輔助方法 ===
    
    def _chart_to_base64(self, fig) -> str:
        """將圖表轉換為base64字符串"""
        buffer = BytesIO()
        fig.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
        buffer.seek(0)
        
        # 轉換為base64
        chart_base64 = base64.b64encode(buffer.getvalue()).decode()
        
        # 清理圖表
        plt.close(fig)
        
        return chart_base64
    
    def _prepare_wave_completion_chart_data(self) -> Dict:
        """準備波次完成圖表數據"""
        # 從波次管理器收集數據
        completed_waves = []
        
        for wave_id in self.wave_manager.wave_history:
            if wave_id in self.wave_manager.waves:
                wave = self.wave_manager.waves[wave_id]
                if wave.actual_completion_time:
                    completed_waves.append({
                        'wave_id': wave_id,
                        'completion_time': wave.actual_completion_time,
                        'task_count': wave.total_tasks,
                        'duration_minutes': (wave.actual_completion_time - wave.actual_start_time).total_seconds() / 60 if wave.actual_start_time else 0
                    })
        
        return {'completed_waves': completed_waves}
    
    def _create_executive_summary(self) -> str:
        """創建執行摘要"""
        text = """
        # 倉儲物流模擬系統 - 執行摘要
        
        ## 系統概況
        本報告基於倉儲物流模擬系統的綜合分析結果，涵蓋系統性能、What-if分析、驗證測試等各方面評估。
        
        ## 主要發現
        """
        
        # 系統性能評估
        if self.system_state_tracker.metrics_history:
            latest_metrics = self.system_state_tracker.metrics_history[-1]
            text += f"""
        
        ### 系統性能表現
        - 整體效率達到 {latest_metrics.overall_efficiency:.1f}%，{"表現優秀" if latest_metrics.overall_efficiency >= 80 else "有改善空間"}
        - 工作站利用率 {latest_metrics.workstation_utilization:.1f}%，{"運作正常" if latest_metrics.workstation_utilization < 90 else "接近滿載"}
        """
        
        # What-if分析結果
        if self.whatif_analyzer.scenario_results:
            high_impact_scenarios = [
                r for r in self.whatif_analyzer.scenario_results.values() 
                if r.impact_level and r.impact_level.value in ['HIGH', 'SEVERE']
            ]
            
            text += f"""
        
        ### 風險評估
        - 測試了 {len(self.whatif_analyzer.scenario_results)} 個假設情境
        - 發現 {len(high_impact_scenarios)} 個高風險情境
        - 系統對參數變化的適應性{"良好" if len(high_impact_scenarios) < 3 else "需要加強"}
        """
        
        # 驗證結果
        if self.validation_engine.validation_reports:
            passed_tests = [
                r for r in self.validation_engine.validation_reports.values() 
                if r.result.value == 'PASS'
            ]
            
            pass_rate = len(passed_tests) / len(self.validation_engine.validation_reports) * 100
            
            text += f"""
        
        ### 驗證測試結果
        - 執行了 {len(self.validation_engine.validation_reports)} 項驗證測試
        - 通過率達到 {pass_rate:.1f}%
        - 模擬系統{"驗證通過，可投入使用" if pass_rate >= 80 else "需要進一步改進"}
        """
        
        text += """
        
        ## 總結建議
        基於綜合分析結果，建議重點關注以下方面：
        1. 持續監控系統性能指標
        2. 建立高風險情境的應急預案
        3. 定期執行驗證測試確保準確性
        4. 優化資源配置提升整體效率
        """
        
        return text
    
    def _create_system_overview_chart(self, config: ReportConfig) -> str:
        """創建系統整體概覽圖表"""
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=config.figure_size)
        
        # 1. 工作站狀態分布 (左上)
        current_time = datetime.now()
        ws_summary = self.system_state_tracker.track_station_status(current_time)
        status_dist = ws_summary.get('status_distribution', {})
        
        if status_dist:
            ax1.pie(status_dist.values(), labels=status_dist.keys(), autopct='%1.1f%%', startangle=90)
            ax1.set_title('工作站狀態分布')
        
        # 2. 性能指標 (右上)
        if self.system_state_tracker.metrics_history:
            latest_metrics = self.system_state_tracker.metrics_history[-1]
            metrics = ['工作站\n利用率', '任務\n完成率', '人員\n利用率', '整體\n效率']
            values = [
                latest_metrics.workstation_utilization,
                latest_metrics.task_completion_rate,
                latest_metrics.staff_utilization,
                latest_metrics.overall_efficiency
            ]
            
            bars = ax2.bar(metrics, values, color=['skyblue', 'lightgreen', 'lightcoral', 'gold'])
            ax2.set_title('當前性能指標')
            ax2.set_ylabel('百分比 (%)')
            ax2.set_ylim(0, 100)
            
            # 添加數值標籤
            for bar, value in zip(bars, values):
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{value:.1f}%', ha='center', va='bottom')
        
        # 3. 異常狀況 (左下)
        exception_summary = self.exception_handler.get_exception_summary(current_time)
        exc_by_type = exception_summary.get('exceptions_by_type', {})
        
        if exc_by_type:
            ax3.bar(exc_by_type.keys(), exc_by_type.values(), color='orange', alpha=0.7)
            ax3.set_title('異常類型分布')
            ax3.set_ylabel('數量')
            ax3.tick_params(axis='x', rotation=45)
        else:
            ax3.text(0.5, 0.5, '無活躍異常', ha='center', va='center', transform=ax3.transAxes)
            ax3.set_title('異常類型分布')
        
        # 4. 波次進度 (右下)
        wave_summary = self.wave_manager.get_active_waves_summary(current_time)
        waves_by_status = wave_summary.get('waves_by_status', {})
        
        if waves_by_status:
            ax4.bar(waves_by_status.keys(), waves_by_status.values(), color='lightblue', alpha=0.7)
            ax4.set_title('波次狀態分布')
            ax4.set_ylabel('數量')
        else:
            ax4.text(0.5, 0.5, '無活躍波次', ha='center', va='center', transform=ax4.transAxes)
            ax4.set_title('波次狀態分布')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_kpi_dashboard(self, config: ReportConfig) -> str:
        """創建KPI儀表板"""
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        axes = axes.flatten()
        
        # KPI定義和當前值
        kpis = [
            {'name': '工作站利用率', 'current': 75.5, 'target': 80, 'unit': '%'},
            {'name': '任務完成率', 'current': 92.3, 'target': 95, 'unit': '%'},
            {'name': '人員利用率', 'current': 68.7, 'target': 75, 'unit': '%'},
            {'name': '整體效率', 'current': 84.2, 'target': 85, 'unit': '%'},
            {'name': '異常處理時間', 'current': 12.5, 'target': 15, 'unit': '分鐘'},
            {'name': '加班頻率', 'current': 25.0, 'target': 20, 'unit': '%'}
        ]
        
        # 從實際數據更新KPI值（如果有的話）
        if self.system_state_tracker.metrics_history:
            latest_metrics = self.system_state_tracker.metrics_history[-1]
            kpis[0]['current'] = latest_metrics.workstation_utilization
            kpis[1]['current'] = latest_metrics.task_completion_rate
            kpis[2]['current'] = latest_metrics.staff_utilization
            kpis[3]['current'] = latest_metrics.overall_efficiency
        
        for i, kpi in enumerate(kpis):
            ax = axes[i]
            
            # 創建圓形進度指示器
            current = kpi['current']
            target = kpi['target']
            
            # 對於時間類指標，邏輯相反（越小越好）
            if kpi['unit'] == '分鐘':
                progress = (target - current) / target * 100 if current <= target else 0
                color = 'green' if current <= target else 'red'
            else:
                progress = current / target * 100
                color = 'green' if current >= target * 0.9 else 'orange' if current >= target * 0.8 else 'red'
            
            # 繪製圓形進度條
            theta = np.linspace(0, 2*np.pi, 100)
            r_outer = 1
            r_inner = 0.7
            
            # 背景圓環
            ax.fill_between(theta, r_inner, r_outer, alpha=0.3, color='lightgray')
            
            # 進度圓環
            progress_theta = theta[:int(len(theta) * min(progress, 100) / 100)]
            if len(progress_theta) > 0:
                ax.fill_between(progress_theta, r_inner, r_outer, alpha=0.8, color=color)
            
            # 添加文字
            ax.text(0, 0, f"{current:.1f}{kpi['unit']}", ha='center', va='center', 
                   fontsize=12, fontweight='bold')
            ax.text(0, -0.3, f"目標: {target}{kpi['unit']}", ha='center', va='center', 
                   fontsize=8, color='gray')
            
            ax.set_xlim(-1.2, 1.2)
            ax.set_ylim(-1.2, 1.2)
            ax.set_aspect('equal')
            ax.axis('off')
            ax.set_title(kpi['name'], fontsize=10, fontweight='bold')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _generate_wave_recommendations(self) -> str:
        """生成波次改善建議"""
        recommendations = """
        ## 波次執行改善建議
        
        ### 效率提升
        1. 優化波次任務分配邏輯，提高工作站利用率
        2. 改善同路線訂單歸併策略，減少切換時間
        3. 加強波次進度監控，及時發現瓶頸
        
        ### 資源配置
        1. 根據歷史數據調整各樓層人員配置
        2. 考慮引入彈性工作站機制
        3. 建立跨樓層支援標準作業程序
        
        ### 系統改進
        1. 實施預測性波次規劃
        2. 加強異常處理對波次的影響管控
        3. 建立波次優先權動態調整機制
        """
        
        return recommendations
    
    def _generate_comprehensive_recommendations(self) -> str:
        """生成綜合改善建議"""
        recommendations = """
        ## 綜合改善建議
        
        ### 短期改善 (1-3個月)
        1. **優化現有流程**
           - 改善工作站分配邏輯
           - 加強異常處理效率
           - 提升人員技能培訓
        
        2. **系統監控強化**
           - 建立即時預警機制
           - 完善績效追蹤體系
           - 建立例外事件分析制度
        
        ### 中期規劃 (3-6個月)
        1. **技術升級**
           - 導入自動化設備
           - 升級倉儲管理系統
           - 強化數據分析能力
        
        2. **流程再造**
           - 重新設計作業流程
           - 建立靈活調度機制
           - 實施精益管理
        
        ### 長期發展 (6-12個月)
        1. **智慧化轉型**
           - 建立AI預測模型
           - 實施自動調度系統
           - 發展無人化作業
        
        2. **持續改善**
           - 建立持續改善文化
           - 完善知識管理體系
           - 建立創新激勵機制
        """
        
        return recommendations
    
    def _export_report(self, report: GeneratedReport):
        """匯出報告檔案"""
        config = report.config
        
        if config.report_format == ReportFormat.HTML:
            self._export_html_report(report)
        elif config.report_format == ReportFormat.EXCEL:
            self._export_excel_report(report)
        elif config.report_format == ReportFormat.CSV:
            self._export_csv_report(report)
        # PDF匯出需要額外的庫支援，這裡暫時省略
    
    def _export_html_report(self, report: GeneratedReport):
        """匯出HTML報告"""
        config = report.config
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>{config.title}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1, h2, h3 {{ color: #333; }}
                .chart {{ text-align: center; margin: 20px 0; }}
                .chart img {{ max-width: 100%; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .summary {{ background-color: #f9f9f9; padding: 15px; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <h1>{config.title}</h1>
            <p><strong>生成時間：</strong>{report.generation_time.strftime('%Y-%m-%d %H:%M:%S')}</p>
        """
        
        if config.subtitle:
            html_content += f"<h2>{config.subtitle}</h2>"
        
        # 按順序添加報告元素
        sorted_elements = sorted(report.elements, key=lambda x: x.order)
        
        for element in sorted_elements:
            html_content += f"<h3>{element.title}</h3>"
            
            if element.element_type == 'text':
                # 將 Markdown 格式轉換為 HTML（簡化版）
                content = element.content.replace('\n', '<br>')
                content = content.replace('## ', '<h4>').replace('### ', '<h5>')
                content = content.replace('**', '<strong>').replace('**', '</strong>')
                html_content += f"<div class='summary'>{content}</div>"
                
            elif element.element_type == 'chart':
                html_content += f"<div class='chart'><img src='data:image/png;base64,{element.content}' alt='{element.title}'></div>"
                
            elif element.element_type == 'table':
                if isinstance(element.content, pd.DataFrame) and not element.content.empty:
                    html_content += element.content.to_html(classes='table', escape=False)
                else:
                    html_content += "<p>無資料</p>"
        
        html_content += """
        </body>
        </html>
        """
        
        # 儲存檔案
        filename = f"{report.report_id}.html"
        file_path = self.output_dir / filename
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        report.file_path = str(file_path)
        report.file_size_bytes = file_path.stat().st_size
        
        self.logger.info(f"HTML報告已儲存: {file_path}")
    
    def _export_excel_report(self, report: GeneratedReport):
        """匯出Excel報告"""
        filename = f"{report.report_id}.xlsx"
        file_path = self.output_dir / filename
        
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            # 摘要工作表
            summary_data = {
                '報告資訊': ['報告ID', '報告類型', '生成時間', '總元素數', '圖表數', '表格數'],
                '值': [
                    report.report_id,
                    report.config.report_type.value,
                    report.generation_time.strftime('%Y-%m-%d %H:%M:%S'),
                    report.total_elements,
                    report.chart_count,
                    report.table_count
                ]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='報告摘要', index=False)
            
            # 為每個表格元素創建工作表
            table_count = 0
            for element in report.elements:
                if element.element_type == 'table' and isinstance(element.content, pd.DataFrame):
                    if not element.content.empty:
                        table_count += 1
                        sheet_name = f"表格{table_count}_{element.title[:10]}"  # 限制工作表名稱長度
                        element.content.to_excel(writer, sheet_name=sheet_name, index=False)
        
        report.file_path = str(file_path)
        report.file_size_bytes = file_path.stat().st_size
        
        self.logger.info(f"Excel報告已儲存: {file_path}")
    
    def _export_csv_report(self, report: GeneratedReport):
        """匯出CSV報告（僅包含表格數據）"""
        csv_dir = self.output_dir / f"{report.report_id}_csv"
        csv_dir.mkdir(exist_ok=True)
        
        table_count = 0
        for element in report.elements:
            if element.element_type == 'table' and isinstance(element.content, pd.DataFrame):
                if not element.content.empty:
                    table_count += 1
                    filename = f"table_{table_count}_{element.title}.csv"
                    file_path = csv_dir / filename
                    element.content.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        report.file_path = str(csv_dir)
        self.logger.info(f"CSV報告已儲存: {csv_dir}")
    
    # === 其他缺失的方法實現 ===
    
    def _create_workload_trend_chart(self, config: ReportConfig) -> str:
        """創建工作量趨勢圖表"""
        fig, ax = plt.subplots(figsize=config.figure_size)
        
        # 模擬工作量趨勢數據
        dates = pd.date_range(start='2024-01-01', periods=14, freq='D')
        workload_hours = np.random.normal(40, 8, 14)  # 平均40小時，標準差8
        capacity_hours = np.full(14, 45)  # 固定產能45小時
        
        ax.plot(dates, workload_hours, label='實際工作量', marker='o', linewidth=2)
        ax.plot(dates, capacity_hours, label='可用產能', linestyle='--', linewidth=2, color='red')
        ax.fill_between(dates, workload_hours, alpha=0.3)
        
        ax.set_title('每日工作量vs產能趨勢', fontsize=16, fontweight='bold')
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel('工作時數', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # 格式化日期軸
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_capacity_utilization_chart(self, config: ReportConfig) -> str:
        """創建產能利用率圖表"""
        fig, ax = plt.subplots(figsize=config.figure_size)
        
        # 模擬各樓層產能利用率
        floors = ['2F', '3F', '4F']
        utilization_rates = [85.5, 78.2, 91.3]  # 百分比
        
        colors = ['lightblue', 'lightgreen', 'lightcoral']
        bars = ax.bar(floors, utilization_rates, color=colors, alpha=0.7)
        
        # 添加目標線
        ax.axhline(y=80, color='red', linestyle='--', alpha=0.7, label='目標利用率 80%')
        ax.axhline(y=90, color='orange', linestyle='--', alpha=0.7, label='警戒線 90%')
        
        ax.set_title('各樓層產能利用率', fontsize=16, fontweight='bold')
        ax.set_xlabel('樓層', fontsize=12)
        ax.set_ylabel('利用率 (%)', fontsize=12)
        ax.set_ylim(0, 100)
        ax.legend()
        
        # 添加數值標籤
        for bar, rate in zip(bars, utilization_rates):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 1,
                   f'{rate:.1f}%', ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_overtime_analysis_chart(self, config: ReportConfig) -> str:
        """創建加班分析圖表"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=config.figure_size)
        
        # 左圖：加班頻率趨勢
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        overtime_required = np.random.choice([0, 1], 10, p=[0.7, 0.3])  # 30%機率需要加班
        
        ax1.bar(dates, overtime_required, alpha=0.7, color='orange')
        ax1.set_title('加班需求趨勢', fontweight='bold')
        ax1.set_xlabel('日期')
        ax1.set_ylabel('是否需要加班')
        ax1.set_ylim(0, 1.2)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        
        # 右圖：加班原因分析
        reasons = ['高工作量', '緊急訂單', '人員短缺', '異常處理', '設備延遲']
        reason_counts = [8, 5, 3, 4, 2]
        
        wedges, texts, autotexts = ax2.pie(reason_counts, labels=reasons, autopct='%1.1f%%', startangle=90)
        ax2.set_title('加班原因分析', fontweight='bold')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_floor_utilization_chart(self, config: ReportConfig) -> str:
        """創建樓層利用率圖表"""
        fig, ax = plt.subplots(figsize=config.figure_size)
        
        # 模擬時間序列數據
        hours = range(8, 18)  # 8點到17點
        floor_2f = np.random.normal(75, 10, len(hours))
        floor_3f = np.random.normal(80, 8, len(hours))
        floor_4f = np.random.normal(70, 12, len(hours))
        
        ax.plot(hours, floor_2f, label='2F', marker='o', linewidth=2)
        ax.plot(hours, floor_3f, label='3F', marker='s', linewidth=2)
        ax.plot(hours, floor_4f, label='4F', marker='^', linewidth=2)
        
        ax.set_title('各樓層人員利用率變化', fontsize=16, fontweight='bold')
        ax.set_xlabel('時間', fontsize=12)
        ax.set_ylabel('利用率 (%)', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 100)
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_skill_analysis_chart(self, config: ReportConfig) -> str:
        """創建技能分析圖表"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=config.figure_size)
        
        # 左圖：技能等級分布
        skill_levels = ['初級', '中級', '高級', '專家']
        staff_counts = [8, 12, 6, 2]
        
        bars1 = ax1.bar(skill_levels, staff_counts, color='lightblue', alpha=0.7)
        ax1.set_title('人員技能等級分布', fontweight='bold')
        ax1.set_ylabel('人數')
        
        # 添加數值標籤
        for bar, count in zip(bars1, staff_counts):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    str(count), ha='center', va='bottom')
        
        # 右圖：技能效率分析
        skill_efficiency = [0.8, 0.9, 1.1, 1.3]  # 相對於標準的效率倍數
        
        bars2 = ax2.bar(skill_levels, skill_efficiency, color='lightgreen', alpha=0.7)
        ax2.set_title('各技能等級效率係數', fontweight='bold')
        ax2.set_ylabel('效率係數')
        ax2.axhline(y=1.0, color='red', linestyle='--', alpha=0.7, label='標準效率')
        ax2.legend()
        
        # 添加數值標籤
        for bar, eff in zip(bars2, skill_efficiency):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                    f'{eff:.1f}x', ha='center', va='bottom')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_exception_type_chart(self, config: ReportConfig) -> str:
        """創建異常類型圖表"""
        fig, ax = plt.subplots(figsize=config.figure_size)
        
        # 從異常處理器取得數據
        exception_summary = self.exception_handler.get_exception_summary(datetime.now())
        exceptions_by_type = exception_summary.get('exceptions_by_type', {})
        
        if exceptions_by_type:
            types = list(exceptions_by_type.keys())
            counts = list(exceptions_by_type.values())
            
            wedges, texts, autotexts = ax.pie(counts, labels=types, autopct='%1.1f%%', startangle=90)
            ax.set_title('異常類型分布', fontsize=16, fontweight='bold')
        else:
            # 使用模擬數據
            types = ['揀貨錯誤', '條碼無法讀取', '庫存不足', '包裝錯誤', '零件破損']
            counts = [15, 8, 5, 6, 3]
            
            wedges, texts, autotexts = ax.pie(counts, labels=types, autopct='%1.1f%%', startangle=90)
            ax.set_title('異常類型分布（模擬數據）', fontsize=16, fontweight='bold')
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_exception_handling_time_chart(self, config: ReportConfig) -> str:
        """創建異常處理時間圖表"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=config.figure_size)
        
        # 左圖：各類型平均處理時間
        exception_types = ['揀貨錯誤', '條碼問題', '庫存不足', '包裝錯誤', '零件破損']
        avg_times = [15, 8, 25, 10, 12]  # 分鐘
        
        bars1 = ax1.bar(exception_types, avg_times, color='orange', alpha=0.7)
        ax1.set_title('各類型平均處理時間', fontweight='bold')
        ax1.set_ylabel('時間 (分鐘)')
        ax1.tick_params(axis='x', rotation=45)
        
        # 添加數值標籤
        for bar, time in zip(bars1, avg_times):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{time}分', ha='center', va='bottom')
        
        # 右圖：處理時間分布直方圖
        handling_times = np.random.gamma(2, 7, 100)  # 模擬處理時間分布
        
        ax2.hist(handling_times, bins=15, alpha=0.7, color='skyblue', edgecolor='black')
        ax2.set_title('處理時間分布', fontweight='bold')
        ax2.set_xlabel('處理時間 (分鐘)')
        ax2.set_ylabel('頻率')
        ax2.axvline(x=np.mean(handling_times), color='red', linestyle='--', 
                   label=f'平均: {np.mean(handling_times):.1f}分')
        ax2.legend()
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def _create_health_dashboard(self, health_assessment: Dict, config: ReportConfig) -> str:
        """創建健康狀況儀表板"""
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=config.figure_size)
        
        # 1. 整體健康分數 (左上)
        score = health_assessment.get('score', 0)
        
        # 創建圓形儀表
        theta = np.linspace(0, np.pi, 100)  # 半圓
        r_outer = 1
        r_inner = 0.7
        
        # 背景半圓
        ax1.fill_between(theta, r_inner, r_outer, alpha=0.3, color='lightgray')
        
        # 分數對應的角度
        score_theta = theta[:int(len(theta) * score / 100)]
        if len(score_theta) > 0:
            color = 'green' if score >= 80 else 'orange' if score >= 60 else 'red'
            ax1.fill_between(score_theta, r_inner, r_outer, alpha=0.8, color=color)
        
        ax1.text(0, 0.3, f"{score}/100", ha='center', va='center', fontsize=16, fontweight='bold')
        ax1.text(0, 0, "系統健康分數", ha='center', va='center', fontsize=10)
        ax1.set_xlim(-1.2, 1.2)
        ax1.set_ylim(0, 1.2)
        ax1.set_aspect('equal')
        ax1.axis('off')
        ax1.set_title('系統健康分數')
        
        # 2. 問題分布 (右上)
        issues = health_assessment.get('issues', [])
        warnings = health_assessment.get('warnings', [])
        
        problem_counts = [len(issues), len(warnings), max(0, 5 - len(issues) - len(warnings))]
        problem_labels = ['嚴重問題', '警告', '正常']
        colors = ['red', 'orange', 'green']
        
        if sum(problem_counts) > 0:
            ax2.pie(problem_counts, labels=problem_labels, colors=colors, autopct='%1.0f', startangle=90)
        else:
            ax2.pie([1], labels=['無問題'], colors=['green'], autopct='100%')
        ax2.set_title('問題分布')
        
        # 3. 關鍵指標狀態 (左下)
        key_metrics = ['工作站利用率', '異常處理', '人員配置', '系統穩定性']
        metric_status = [85, 70, 90, 95]  # 百分比分數
        
        bars = ax3.barh(key_metrics, metric_status, color=['green' if s >= 80 else 'orange' if s >= 60 else 'red' for s in metric_status])
        ax3.set_title('關鍵指標狀態')
        ax3.set_xlabel('狀態分數 (%)')
        ax3.set_xlim(0, 100)
        
        # 添加數值標籤
        for i, (bar, score) in enumerate(zip(bars, metric_status)):
            ax3.text(score + 2, i, f'{score}%', va='center')
        
        # 4. 趨勢指示 (右下)
        trend_days = ['週一', '週二', '週三', '週四', '週五']
        health_trend = [85, 80, 75, 78, 82]
        
        ax4.plot(trend_days, health_trend, marker='o', linewidth=2, color='blue')
        ax4.fill_between(range(len(trend_days)), health_trend, alpha=0.3, color='blue')
        ax4.set_title('健康趨勢')
        ax4.set_ylabel('健康分數')
        ax4.set_ylim(0, 100)
        ax4.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        return self._chart_to_base64(fig)
    
    def get_generated_report(self, report_id: str) -> Optional[GeneratedReport]:
        """取得生成的報告"""
        return self.generated_reports.get(report_id)
    
    def list_generated_reports(self) -> List[Dict[str, Any]]:
        """列出已生成的報告"""
        reports = []
        
        for report_id, report in self.generated_reports.items():
            reports.append({
                'report_id': report_id,
                'title': report.config.title,
                'report_type': report.config.report_type.value,
                'report_format': report.config.report_format.value,
                'generation_time': report.generation_time,
                'total_elements': report.total_elements,
                'file_path': report.file_path,
                'file_size_bytes': report.file_size_bytes
            })
        
        return reports
    
    def delete_report(self, report_id: str) -> bool:
        """刪除報告"""
        if report_id in self.generated_reports:
            report = self.generated_reports[report_id]
            
            # 刪除檔案（如果存在）
            if report.file_path and Path(report.file_path).exists():
                try:
                    if Path(report.file_path).is_file():
                        Path(report.file_path).unlink()
                    elif Path(report.file_path).is_dir():
                        import shutil
                        shutil.rmtree(report.file_path)
                except Exception as e:
                    self.logger.warning(f"刪除報告檔案失敗: {e}")
            
            # 從記錄中移除
            del self.generated_reports[report_id]
            
            self.logger.info(f"報告已刪除: {report_id}")
            return True
        
        return False