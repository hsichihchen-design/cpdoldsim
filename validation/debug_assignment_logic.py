"""
Debug Assignment Logic - 診斷工作站分配邏輯
深入追蹤任務分配過程，找出問題根源
"""

import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, timedelta

# 加入父目錄以便 import
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_manager import DataManager
from src.order_priority_manager import OrderPriorityManager
from src.wave_manager import WaveManager
from src.workstation_task_manager import WorkstationTaskManager
from src.staff_schedule_generator import StaffScheduleGenerator



def debug_assignment_process():
    """深度診斷分配過程"""
    print("🔍 開始診斷工作站分配邏輯...")
    
    # 初始化（重用Step2的邏輯）
    target_date = "2025-06-03"
    target_delivery_time = "1000"
    
    data_manager = DataManager()
    master_data = data_manager.load_master_data()
    transaction_data = data_manager.load_transaction_data(
        start_date=target_date, 
        end_date=target_date,
        filter_valid_items=True
    )
    
    orders_df = transaction_data['historical_orders']
    order_priority_manager = OrderPriorityManager(data_manager)
    
    class MockWorkstationManager:
        def __init__(self):
            self.workstations = {}
            self.tasks = {}
    
    workstation_manager = MockWorkstationManager()
    wave_manager = WaveManager(data_manager, workstation_manager)
    workstation_task_manager = WorkstationTaskManager(data_manager)
    workstation_task_manager.wave_manager = wave_manager
    staff_schedule_generator = StaffScheduleGenerator(data_manager)
    
    
    # 重現問題場景
    processed_orders = order_priority_manager.process_orders_batch(orders_df)
    target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
    waves = wave_manager.create_waves_from_schedule(target_datetime)
    
    target_wave = None
    for wave in waves:
        if wave.delivery_time_str == target_delivery_time:
            target_wave = wave
            break
    
    # 篩選波次訂單
    wave_orders = processed_orders[
        (processed_orders['ROUTECD'].isin(target_wave.included_routes)) &
        (processed_orders['PARTCUSTID'].isin(target_wave.included_partcustids))
    ].copy()
    
    # 建立任務
    shipping_tasks = workstation_task_manager.create_tasks_from_orders(wave_orders)
    staff_schedule = staff_schedule_generator.generate_daily_schedule(target_date)
    current_time = datetime.strptime(f"{target_date} 08:50:00", '%Y-%m-%d %H:%M:%S')
    
    print(f"\n📊 診斷基礎資料:")
    print(f"  波次任務數: {len(shipping_tasks)}")
    print(f"  員工排班數: {len(staff_schedule)}")
    print(f"  目標波次可用時間: {target_wave.available_work_time_minutes} 分鐘")
    
    # === 診斷點1: 檢查工作站初始化 ===
    print(f"\n🔧 診斷點1: 工作站初始化檢查")
    
    print(f"  總工作站數: {len(workstation_task_manager.workstations)}")
    
    # 按樓層統計工作站
    floor_stations = {}
    for station_id, station in workstation_task_manager.workstations.items():
        floor = station.floor
        if floor not in floor_stations:
            floor_stations[floor] = []
        floor_stations[floor].append(station_id)
    
    for floor, stations in sorted(floor_stations.items()):
        print(f"    樓層{floor}: {len(stations)} 個工作站 {stations}")
    
    # === 診斷點2: 檢查任務樓層分布 ===
    print(f"\n📦 診斷點2: 任務樓層分布檢查")
    
    task_floor_distribution = {}
    for task in shipping_tasks:
        floor = task.floor
        task_floor_distribution[floor] = task_floor_distribution.get(floor, 0) + 1
    
    print(f"  任務樓層分布: {task_floor_distribution}")
    
    # === 診斷點3: 檢查員工排班 ===
    print(f"\n👥 診斷點3: 員工排班檢查")
    
    staff_floor_distribution = staff_schedule['floor'].value_counts().to_dict()
    print(f"  員工樓層分布: {staff_floor_distribution}")
    
    # 詳細員工分配
    for floor in sorted(staff_floor_distribution.keys()):
        floor_staff = staff_schedule[staff_schedule['floor'] == floor]
        print(f"    樓層{floor}: {len(floor_staff)} 名員工")
        for _, staff in floor_staff.iterrows():
            print(f"      {staff['station_id']} - 員工{staff['staff_id']}")
    
    # === 診斷點4: 手動追蹤分配邏輯 ===
    print(f"\n🎯 診斷點4: 手動追蹤分配邏輯")
    
    # 檢查分配方法的關鍵參數
    params = workstation_task_manager.params
    print(f"  關鍵參數:")
    print(f"    max_partcustids_per_station: {params.get('max_partcustids_per_station', 'NOT_SET')}")
    print(f"    time_buffer_minutes: {params.get('time_buffer_minutes', 'NOT_SET')}")
    
    # 檢查時間約束
    print(f"  時間約束:")
    print(f"    波次可用時間: {target_wave.available_work_time_minutes} 分鐘")
    print(f"    波次截止時間: {target_wave.latest_cutoff_time}")
    print(f"    波次出車時間: {target_wave.delivery_datetime}")
    
    # === 診斷點5: 模擬分配過程 ===
    print(f"\n🔄 診斷點5: 模擬分配過程")
    
    # 按任務類型分組
    task_groups = workstation_task_manager._group_tasks_by_type_and_wave(shipping_tasks, current_time)
    
    print(f"  任務分組結果:")
    for group_name, group_tasks in task_groups.items():
        if group_name != 'shipping_waves':
            if group_tasks:
                print(f"    {group_name}: {len(group_tasks)} 個任務")
        else:
            for wave_id, wave_tasks in group_tasks.items():
                print(f"    {wave_id}: {len(wave_tasks)} 個任務")
    
    # === 診斷點6: 檢查據點分組 ===
    print(f"\n🏗️ 診斷點6: 據點分組檢查")
    
    # 找到該波次的任務
    wave_tasks = []
    if 'shipping_waves' in task_groups:
        for wave_id, tasks in task_groups['shipping_waves'].items():
            if wave_id == target_wave.wave_id:
                wave_tasks = tasks
                break
    
    if wave_tasks:
        partcustid_groups = workstation_task_manager._group_tasks_by_partcustid(wave_tasks)
        
        print(f"  據點分組數量: {len(partcustid_groups)}")
        print(f"  前10個據點分組:")
        
        for i, group in enumerate(partcustid_groups[:10]):
            print(f"    {i+1}. {group.partcustid}: {group.task_count}任務, {group.total_workload_minutes:.1f}分鐘")
    
    # === 診斷點7: 檢查時間可行性檢查 ===
    print(f"\n⏰ 診斷點7: 時間可行性檢查")
    
    if wave_tasks:
        deadline_check = workstation_task_manager._check_wave_deadline_feasibility(wave_tasks, current_time)
        
        print(f"  可行性檢查結果:")
        print(f"    可行: {deadline_check['feasible']}")
        print(f"    可用時間: {deadline_check['available_minutes']} 分鐘")
        print(f"    需要時間: {deadline_check['required_minutes']} 分鐘")
        print(f"    估算需要工作站: {deadline_check.get('estimated_stations_needed', 'N/A')}")
        
        if not deadline_check['feasible']:
            print(f"    ⚠️ 時間不可行！系統應該觸發加班邏輯")
    
    # === 診斷點8: 檢查工作站分配演算法 ===
    print(f"\n🧮 診斷點8: 工作站分配演算法檢查")
    
    if wave_tasks and partcustid_groups:
        # 手動執行 Bin Packing
        assigned_stations = set()
        
        try:
            station_assignments = workstation_task_manager._assign_partcustids_to_stations(
                partcustid_groups, current_time, assigned_stations, 
                deadline_check['available_minutes']
            )
            
            print(f"  Bin Packing 結果:")
            print(f"    分配的工作站數: {len(station_assignments)}")
            
            for i, assignment in enumerate(station_assignments):
                print(f"      工作站{i+1} ({assignment.station_id}):")
                print(f"        據點數: {assignment.total_partcustids}")
                print(f"        工作量: {assignment.total_workload_minutes:.1f} 分鐘")
                print(f"        據點清單: {[g.partcustid for g in assignment.partcustid_groups[:5]]}...")
                
                if assignment.total_partcustids > params.get('max_partcustids_per_station', 12):
                    print(f"        ⚠️ 超過據點上限！({assignment.total_partcustids} > {params.get('max_partcustids_per_station', 12)})")
                
                if assignment.total_workload_minutes > deadline_check['available_minutes']:
                    print(f"        ⚠️ 超過時間限制！({assignment.total_workload_minutes:.1f} > {deadline_check['available_minutes']})")
                    
        except Exception as e:
            print(f"    ❌ Bin Packing 執行失敗: {str(e)}")
    
    # === 診斷點9: 檢查實際分配結果 ===
    print(f"\n📋 診斷點9: 實際分配結果檢查")
    
    # 執行實際分配
    assignment_result = workstation_task_manager.assign_tasks_to_stations(
        shipping_tasks, staff_schedule, current_time
    )
    
    print(f"  實際分配結果:")
    print(f"    已分配: {len(assignment_result['assigned'])}")
    print(f"    未分配: {len(assignment_result['unassigned'])}")
    print(f"    需加班: {len(assignment_result.get('overtime_required', []))}")
    
    # 分析分配到的工作站
    assigned_stations_analysis = {}
    for task_id in assignment_result['assigned']:
        task = workstation_task_manager.tasks[task_id]
        if task.assigned_station:
            station_id = task.assigned_station
            if station_id not in assigned_stations_analysis:
                assigned_stations_analysis[station_id] = {
                    'task_count': 0,
                    'total_time': 0,
                    'partcustids': set(),
                    'floor': task.floor
                }
            
            assigned_stations_analysis[station_id]['task_count'] += 1
            assigned_stations_analysis[station_id]['total_time'] += task.estimated_duration
            assigned_stations_analysis[station_id]['partcustids'].add(task.partcustid)
    
    print(f"\n  分配到的工作站詳情:")
    for station_id, info in assigned_stations_analysis.items():
        print(f"    {station_id} (樓層{info['floor']}):")
        print(f"      任務數: {info['task_count']}")
        print(f"      據點數: {len(info['partcustids'])}")
        print(f"      總時間: {info['total_time']:.1f} 分鐘")
        
        # 檢查異常
        if len(info['partcustids']) > params.get('max_partcustids_per_station', 12):
            print(f"      ❌ 據點數超限: {len(info['partcustids'])} > {params.get('max_partcustids_per_station', 12)}")
        
        if info['total_time'] > target_wave.available_work_time_minutes:
            print(f"      ❌ 時間超限: {info['total_time']:.1f} > {target_wave.available_work_time_minutes}")
    
    # === 總結診斷結果 ===
    print(f"\n📋 診斷總結:")
    
    issues_found = []
    
    # 檢查1: 工作站使用不均
    used_floors = set(info['floor'] for info in assigned_stations_analysis.values())
    available_floors = set(floor_stations.keys())
    unused_floors = available_floors - used_floors
    
    if unused_floors:
        issues_found.append(f"未使用樓層: {unused_floors}")
    
    # 檢查2: 據點超限
    overloaded_stations = [
        station_id for station_id, info in assigned_stations_analysis.items()
        if len(info['partcustids']) > params.get('max_partcustids_per_station', 12)
    ]
    
    if overloaded_stations:
        issues_found.append(f"據點超限工作站: {overloaded_stations}")
    
    # 檢查3: 時間超限
    overtime_stations = [
        station_id for station_id, info in assigned_stations_analysis.items()
        if info['total_time'] > target_wave.available_work_time_minutes
    ]
    
    if overtime_stations:
        issues_found.append(f"時間超限工作站: {overtime_stations}")
    
    # 檢查4: 可行性檢查被忽略
    if not deadline_check['feasible'] and len(assignment_result.get('overtime_required', [])) == 0:
        issues_found.append("時間不可行但未觸發加班邏輯")
    
    if issues_found:
        print(f"  🚨 發現問題:")
        for i, issue in enumerate(issues_found, 1):
            print(f"    {i}. {issue}")
    else:
        print(f"  ✅ 未發現明顯問題")
    
    return {
        'total_workstations': len(workstation_task_manager.workstations),
        'floor_stations': floor_stations,
        'assigned_stations': len(assigned_stations_analysis),
        'assigned_stations_details': assigned_stations_analysis,
        'issues_found': issues_found,
        'deadline_feasible': deadline_check['feasible'],
        'partcustid_groups_count': len(partcustid_groups) if 'partcustid_groups' in locals() else 0
    }

if __name__ == "__main__":
    try:
        result = debug_assignment_process()
        print(f"\n🎯 診斷完成！")
        
    except Exception as e:
        print(f"\n❌ 診斷過程發生錯誤: {str(e)}")
        import traceback
        traceback.print_exc()