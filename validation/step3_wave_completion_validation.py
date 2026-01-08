"""
Step 3: 波次完成度與時間驗證
驗證波次是否能在截止時間內完成，分析瓶頸和加班需求
"""

import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, date, time, timedelta

# 加入父目錄以便 import
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_manager import DataManager
from src.order_priority_manager import OrderPriorityManager
from src.wave_manager import WaveManager
from src.workstation_task_manager import WorkstationTaskManager
from src.staff_schedule_generator import StaffScheduleGenerator

def validate_wave_completion_feasibility(target_date="2024-06-15", target_delivery_time="1000"):
    """驗證波次是否能按時完成"""
    print(f"⏰ Step 3: 驗證波次完成度與時間...")
    print(f"  目標日期: {target_date}")
    print(f"  目標出車時間: {target_delivery_time}")
    
    # 重用 Step 2 的邏輯取得基本資料
    print("\n🔧 初始化並取得基本資料...")
    
    try:
        # 引用 step2 的函數取得基本分配結果
        from step2_wave_task_validation import validate_single_wave_assignment
        
        step2_result = validate_single_wave_assignment(target_date, target_delivery_time)
        
        if not step2_result:
            print("❌ 無法取得 Step 2 的分配結果！")
            return
        
    except ImportError:
        print("⚠️ 無法導入 step2，將重新執行基本分配...")
        step2_result = run_basic_assignment(target_date, target_delivery_time)
    
    # 開始時間約束分析
    print(f"\n⏱️ Step 1: 時間約束分析...")
    
    # 重新初始化管理器以取得詳細資訊
    data_manager = DataManager()
    master_data = data_manager.load_master_data()
    
    # 建立 mock workstation manager
    class MockWorkstationManager:
        def __init__(self):
            self.workstations = {}
            self.tasks = {}
    
    workstation_manager = MockWorkstationManager()
    wave_manager = WaveManager(data_manager, workstation_manager)
    
    # 🔧 修正：確保 WorkstationTaskManager 有正確的 wave_manager
    workstation_task_manager = WorkstationTaskManager(data_manager, wave_manager)
    workstation_task_manager.wave_manager = wave_manager
    
    # 重新建立目標波次
    target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
    waves = wave_manager.create_waves_from_schedule(target_datetime)
    
    target_wave = None
    for wave in waves:
        if wave.delivery_time_str == target_delivery_time:
            target_wave = wave
            break
    
    if not target_wave:
        print(f"❌ 找不到目標波次！")
        return
    
    # 時間約束資訊
    time_constraints = {
        'delivery_time': target_wave.delivery_datetime,
        'latest_cutoff_time': target_wave.latest_cutoff_time,
        'available_work_minutes': target_wave.available_work_time_minutes,
        'delivery_time_str': target_wave.delivery_time_str
    }
    
    print(f"  出車時間: {time_constraints['delivery_time'].strftime('%H:%M')}")
    print(f"  最晚截止時間: {time_constraints['latest_cutoff_time'].strftime('%H:%M')}")
    print(f"  可用作業時間: {time_constraints['available_work_minutes']} 分鐘")
    
    # Step 2: 工作負載分析
    print(f"\n📊 Step 2: 工作負載分析...")
    
    total_estimated_time = step2_result['total_estimated_time']
    stations_used = step2_result['stations_used']
    station_assignments = step2_result['station_assignments']
    
    print(f"  總工作負載: {total_estimated_time:.1f} 分鐘")
    print(f"  使用工作站: {stations_used} 個")
    print(f"  平均每站負載: {total_estimated_time/stations_used:.1f} 分鐘" if stations_used > 0 else "N/A")
    
    # 分析各工作站的負載分布
    station_loads = []
    max_station_time = 0
    min_station_time = float('inf')
    
    for station_id, info in station_assignments.items():
        station_time = info['total_time']
        station_loads.append({
            'station_id': station_id,
            'floor': info['floor'],
            'task_count': info['task_count'],
            'total_time': station_time,
            'partcustid_count': len(info['partcustids'])
        })
        
        max_station_time = max(max_station_time, station_time)
        min_station_time = min(min_station_time, station_time)
    
    load_imbalance = max_station_time - min_station_time
    
    print(f"\n  工作站負載分布:")
    print(f"    最大負載: {max_station_time:.1f} 分鐘")
    print(f"    最小負載: {min_station_time:.1f} 分鐘")
    print(f"    負載不平衡度: {load_imbalance:.1f} 分鐘")
    print(f"    負載變異係數: {np.std([s['total_time'] for s in station_loads])/np.mean([s['total_time'] for s in station_loads]):.2f}")
    
    # Step 3: 完成時間預測
    print(f"\n🎯 Step 3: 完成時間預測...")
    
    # 假設工作開始時間（截止時間）
    work_start_time = time_constraints['latest_cutoff_time']
    
    # 計算各工作站的預計完成時間
    station_completion_times = []
    
    for station_load in station_loads:
        # 加入啟動時間（3分鐘）
        startup_time_minutes = 3
        total_time_with_startup = station_load['total_time'] + startup_time_minutes
        
        # 計算完成時間
        completion_time = work_start_time + timedelta(minutes=total_time_with_startup)
        
        station_completion_times.append({
            'station_id': station_load['station_id'],
            'floor': station_load['floor'],
            'start_time': work_start_time,
            'work_time': station_load['total_time'],
            'completion_time': completion_time,
            'meets_deadline': completion_time <= time_constraints['delivery_time']
        })
    
    # 找出最晚完成的工作站
    latest_completion = max(station_completion_times, key=lambda x: x['completion_time'])
    earliest_completion = min(station_completion_times, key=lambda x: x['completion_time'])
    
    print(f"  預計開始時間: {work_start_time.strftime('%H:%M')}")
    print(f"  最早完成時間: {earliest_completion['completion_time'].strftime('%H:%M')} ({earliest_completion['station_id']})")
    print(f"  最晚完成時間: {latest_completion['completion_time'].strftime('%H:%M')} ({latest_completion['station_id']})")
    print(f"  出車時間: {time_constraints['delivery_time'].strftime('%H:%M')}")
    
    # Step 4: 可行性判斷
    print(f"\n✅ Step 4: 可行性判斷...")
    
    # 計算時間餘裕或超時
    time_margin = (time_constraints['delivery_time'] - latest_completion['completion_time']).total_seconds() / 60
    
    if time_margin >= 0:
        print(f"  ✅ 波次可按時完成")
        print(f"  時間餘裕: {time_margin:.1f} 分鐘")
        feasibility_status = "FEASIBLE"
    else:
        print(f"  ❌ 波次無法按時完成")
        print(f"  超時時間: {abs(time_margin):.1f} 分鐘")
        feasibility_status = "INFEASIBLE"
    
    # 統計達標的工作站
    on_time_stations = [s for s in station_completion_times if s['meets_deadline']]
    delayed_stations = [s for s in station_completion_times if not s['meets_deadline']]
    
    print(f"  按時完成的工作站: {len(on_time_stations)}/{len(station_completion_times)} 個")
    
    if delayed_stations:
        print(f"  超時的工作站:")
        for station in delayed_stations:
            delay_minutes = (station['completion_time'] - time_constraints['delivery_time']).total_seconds() / 60
            print(f"    {station['station_id']}: 超時 {delay_minutes:.1f} 分鐘")
    
    # Step 5: 瓶頸分析
    print(f"\n🔍 Step 5: 瓶頸分析...")
    
    # 按負載排序找出瓶頸工作站
    station_loads_sorted = sorted(station_loads, key=lambda x: x['total_time'], reverse=True)
    
    print(f"  瓶頸工作站（前5個）:")
    for i, station in enumerate(station_loads_sorted[:5], 1):
        print(f"    {i}. {station['station_id']}: {station['total_time']:.1f}分鐘 ({station['task_count']}任務, {station['partcustid_count']}據點)")
    
    # 分析瓶頸原因
    bottleneck_analysis = {
        'load_imbalance': load_imbalance > 30,  # 負載不平衡超過30分鐘
        'single_station_overload': max_station_time > time_constraints['available_work_minutes'] * 0.9,  # 單站負載過高
        'insufficient_capacity': total_estimated_time > time_constraints['available_work_minutes'] * stations_used * 0.8,  # 總容量不足
        'poor_distribution': len(delayed_stations) > 0  # 分配不當
    }
    
    print(f"\n  瓶頸原因分析:")
    if bottleneck_analysis['load_imbalance']:
        print(f"    ⚠️ 負載分配不平衡（差異 {load_imbalance:.1f} 分鐘）")
    if bottleneck_analysis['single_station_overload']:
        print(f"    ⚠️ 單一工作站負載過重（{max_station_time:.1f} 分鐘）")
    if bottleneck_analysis['insufficient_capacity']:
        print(f"    ⚠️ 總體容量不足")
    if bottleneck_analysis['poor_distribution']:
        print(f"    ⚠️ 任務分配策略待優化")
    
    if not any(bottleneck_analysis.values()):
        print(f"    ✅ 無明顯瓶頸")
    
    # Step 6: 改善建議
    print(f"\n💡 Step 6: 改善建議...")
    
    suggestions = []
    
    if feasibility_status == "INFEASIBLE":
        if bottleneck_analysis['load_imbalance']:
            suggestions.append("重新平衡工作站負載分配")
        
        if bottleneck_analysis['single_station_overload']:
            suggestions.append(f"增加工作站數量或安排加班處理瓶頸站台")
        
        if bottleneck_analysis['insufficient_capacity']:
            needed_stations = int(np.ceil(total_estimated_time / time_constraints['available_work_minutes']))
            additional_stations = needed_stations - stations_used
            suggestions.append(f"建議增加 {additional_stations} 個工作站")
        
        # 計算需要的加班時間
        if time_margin < 0:
            required_overtime = abs(time_margin)
            overtime_stations = len(delayed_stations)
            suggestions.append(f"安排 {overtime_stations} 個工作站加班 {required_overtime:.1f} 分鐘")
    
    else:
        if time_margin < 15:  # 時間餘裕不足15分鐘
            suggestions.append("時間餘裕較少，建議增加緩衝時間")
        
        if bottleneck_analysis['load_imbalance']:
            suggestions.append("優化任務分配以減少負載不平衡")
    
    if suggestions:
        for i, suggestion in enumerate(suggestions, 1):
            print(f"    {i}. {suggestion}")
    else:
        print(f"    ✅ 當前配置良好，無需特別改善")
    
    # Step 7: 加班需求分析
    print(f"\n🕒 Step 7: 加班需求分析...")
    
    if feasibility_status == "INFEASIBLE":
        # 計算所需加班時間
        overtime_requirements = {}
        
        for station in delayed_stations:
            delay_minutes = (station['completion_time'] - time_constraints['delivery_time']).total_seconds() / 60
            overtime_hours = delay_minutes / 60
            
            overtime_requirements[station['station_id']] = {
                'required_minutes': delay_minutes,
                'required_hours': overtime_hours,
                'reason': f"波次超時 {delay_minutes:.1f} 分鐘"
            }
        
        total_overtime_hours = sum(req['required_hours'] for req in overtime_requirements.values())
        
        print(f"  需要加班的工作站: {len(overtime_requirements)} 個")
        print(f"  總加班時數: {total_overtime_hours:.1f} 小時")
        
        for station_id, req in overtime_requirements.items():
            print(f"    {station_id}: {req['required_hours']:.1f} 小時")
    
    else:
        print(f"  ✅ 無需加班")
        overtime_requirements = {}
    
    # Step 8: 輸出詳細分析報告
    print(f"\n📁 Step 8: 輸出詳細分析報告...")
    
    # 工作站完成時間分析
    completion_analysis_df = pd.DataFrame(station_completion_times)
    completion_analysis_df['delay_minutes'] = completion_analysis_df.apply(
        lambda row: (row['completion_time'] - time_constraints['delivery_time']).total_seconds() / 60,
        axis=1
    )
    
    output_file = os.path.join(os.path.dirname(__file__), '..', 'output', 
                             f'wave_completion_analysis_{target_date}_{target_delivery_time}.csv')
    completion_analysis_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  完成時間分析: {output_file}")
    
    # 瓶頸分析報告
    bottleneck_report = {
        'wave_id': f"WAVE_{target_delivery_time}_{target_date.replace('-', '')}",
        'feasibility_status': feasibility_status,
        'time_margin_minutes': time_margin,
        'total_workload_minutes': total_estimated_time,
        'available_time_minutes': time_constraints['available_work_minutes'],
        'stations_used': stations_used,
        'max_station_load': max_station_time,
        'min_station_load': min_station_time,
        'load_imbalance': load_imbalance,
        'on_time_stations': len(on_time_stations),
        'delayed_stations': len(delayed_stations),
        'overtime_required': len(overtime_requirements) > 0,
        'total_overtime_hours': sum(req['required_hours'] for req in overtime_requirements.values()) if overtime_requirements else 0
    }
    
    bottleneck_df = pd.DataFrame([bottleneck_report])
    output_file = os.path.join(os.path.dirname(__file__), '..', 'output', 
                             f'wave_bottleneck_analysis_{target_date}_{target_delivery_time}.csv')
    bottleneck_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  瓶頸分析報告: {output_file}")
    
    # Step 9: 總結
    print(f"\n📋 Step 3 驗證總結:")
    print(f"  波次可行性: {feasibility_status}")
    print(f"  時間餘裕/超時: {time_margin:.1f} 分鐘")
    print(f"  瓶頸工作站: {station_loads_sorted[0]['station_id']} ({max_station_time:.1f}分鐘)")
    print(f"  負載不平衡度: {load_imbalance:.1f} 分鐘")
    print(f"  加班需求: {'是' if overtime_requirements else '否'}")
    
    if overtime_requirements:
        print(f"  需加班工作站: {len(overtime_requirements)} 個")
        print(f"  總加班時數: {sum(req['required_hours'] for req in overtime_requirements.values()):.1f} 小時")
    
    return {
        'feasibility_status': feasibility_status,
        'time_margin_minutes': time_margin,
        'bottleneck_station': station_loads_sorted[0]['station_id'],
        'load_imbalance': load_imbalance,
        'overtime_required': len(overtime_requirements) > 0,
        'overtime_stations': len(overtime_requirements),
        'total_overtime_hours': sum(req['required_hours'] for req in overtime_requirements.values()) if overtime_requirements else 0,
        'completion_analysis': completion_analysis_df.to_dict('records'),
        'bottleneck_analysis': bottleneck_analysis,
        'suggestions': suggestions
    }

def run_basic_assignment(target_date, target_delivery_time):
    """如果無法導入 step2，執行基本分配邏輯"""
    # 這裡是簡化版的邏輯，實際使用時應該用完整的 step2 結果
    return {
        'total_estimated_time': 480,  # 假設值
        'stations_used': 6,
        'station_assignments': {}
    }

if __name__ == "__main__":
    try:
        # 可以修改這些參數來測試不同的波次
        target_date = "2024-06-15"
        target_delivery_time = "1000"
        
        result = validate_wave_completion_feasibility(target_date, target_delivery_time)
        print(f"\n🎯 波次完成度驗證完成！")
        
    except Exception as e:
        print(f"\n❌ 驗證過程發生錯誤: {str(e)}")
        import traceback
        traceback.print_exc()