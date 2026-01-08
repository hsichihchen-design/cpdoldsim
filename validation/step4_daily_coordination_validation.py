"""
Step 4: 一天內多波次協調驗證
驗證一天內所有波次的資源協調、時間銜接和衝突處理
"""

import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, date, time, timedelta
from collections import defaultdict

# 加入父目錄以便 import
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_manager import DataManager
from src.order_priority_manager import OrderPriorityManager
from src.wave_manager import WaveManager
from src.workstation_task_manager import WorkstationTaskManager, TaskType
from src.staff_schedule_generator import StaffScheduleGenerator
from src.receiving_manager import ReceivingManager

def validate_daily_wave_coordination(target_date="2024-06-15"):
    """驗證一天內所有波次的協調情況"""
    print(f"📅 Step 4: 驗證一天內多波次協調...")
    print(f"  目標日期: {target_date}")
    
    # 初始化系統模組
    print("\n🔧 初始化系統模組...")
    data_manager = DataManager()
    
    # 載入資料
    master_data = data_manager.load_master_data()
    transaction_data = data_manager.load_transaction_data(
        start_date=target_date, 
        end_date=target_date,
        filter_valid_items=True
    )
    
    if 'historical_orders' not in transaction_data:
        print("❌ 找不到歷史訂單資料！")
        return
    
    orders_df = transaction_data['historical_orders']
    receiving_df = transaction_data.get('historical_receiving', pd.DataFrame())
    
    print(f"📊 當日資料:")
    print(f"  出貨訂單: {len(orders_df):,} 筆")
    print(f"  進貨記錄: {len(receiving_df):,} 筆")
    
    # 初始化管理器
    order_priority_manager = OrderPriorityManager(data_manager)
    receiving_manager = ReceivingManager(data_manager)
    
    class MockWorkstationManager:
        def __init__(self):
            self.workstations = {}
            self.tasks = {}
    
    workstation_manager = MockWorkstationManager()
    wave_manager = WaveManager(data_manager, workstation_manager)
    workstation_task_manager = WorkstationTaskManager(data_manager, wave_manager)
    staff_schedule_generator = StaffScheduleGenerator(data_manager)
    
    # 🔧 修正：設定 wave_manager
    workstation_task_manager.wave_manager = wave_manager
    
    # Step 1: 建立當日所有波次
    print("\n🌊 Step 1: 建立當日所有波次...")
    target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
    waves = wave_manager.create_waves_from_schedule(target_datetime)
    
    print(f"  當日波次數量: {len(waves)} 個")
    
    # 按出車時間排序
    waves_sorted = sorted(waves, key=lambda w: w.delivery_datetime)
    
    print(f"  波次時間表:")
    for wave in waves_sorted:
        print(f"    {wave.delivery_time_str}: {wave.wave_id}")
        print(f"      路線: {wave.included_routes}")
        print(f"      據點: {len(wave.included_partcustids)} 個")
        print(f"      可用時間: {wave.available_work_time_minutes} 分鐘")
    
    # Step 2: 處理出貨訂單
    print("\n📦 Step 2: 處理出貨訂單...")
    processed_orders = order_priority_manager.process_orders_batch(orders_df)
    
    # 按波次分組訂單
    wave_orders = {}
    unassigned_orders = []
    
    for _, order in processed_orders.iterrows():
        assigned_to_wave = False
        
        for wave in waves_sorted:
            if (order['ROUTECD'] in wave.included_routes and 
                order['PARTCUSTID'] in wave.included_partcustids):
                
                if wave.wave_id not in wave_orders:
                    wave_orders[wave.wave_id] = []
                wave_orders[wave.wave_id].append(order)
                assigned_to_wave = True
                break
        
        if not assigned_to_wave:
            unassigned_orders.append(order)
    
    print(f"  訂單分配結果:")
    for wave_id, orders in wave_orders.items():
        print(f"    {wave_id}: {len(orders)} 筆訂單")
    
    if unassigned_orders:
        print(f"    未分配訂單: {len(unassigned_orders)} 筆")
    
    # Step 3: 處理進貨任務
    print("\n📥 Step 3: 處理進貨任務...")
    
    current_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    receiving_tasks = []
    
    if len(receiving_df) > 0:
        # 處理進貨優先權
        processed_receiving = receiving_manager.process_receiving_batch(receiving_df, current_date)
        
        # 建立進貨任務
        receiving_tasks = workstation_task_manager.create_tasks_from_receiving(processed_receiving, current_date)
        
        print(f"  進貨任務數量: {len(receiving_tasks)} 個")
        
        # 分析進貨任務優先權
        receiving_priority_stats = {}
        for task in receiving_tasks:
            priority = task.priority_level
            receiving_priority_stats[priority] = receiving_priority_stats.get(priority, 0) + 1
        
        print(f"  進貨優先權分布: {receiving_priority_stats}")
    
    else:
        print(f"  無進貨任務")
    
    # Step 4: 生成員工排班
    print("\n👥 Step 4: 生成員工排班...")
    staff_schedule = staff_schedule_generator.generate_daily_schedule(target_date)
    
    print(f"  員工排班數量: {len(staff_schedule)} 個班次")
    
    floor_staff_stats = staff_schedule['floor'].value_counts()
    print(f"  各樓層人力: {dict(floor_staff_stats)}")
    
    # Step 5: 逐波次分配任務
    print("\n🎯 Step 5: 逐波次分配任務...")
    
    wave_analysis_results = {}
    resource_timeline = []
    total_assigned_tasks = 0
    total_unassigned_tasks = 0
    total_overtime_required = 0
    
    # 追蹤工作站使用情況
    station_usage_timeline = defaultdict(list)
    assigned_stations = set()
    
    current_simulation_time = datetime.strptime(f"{target_date} 08:50:00", '%Y-%m-%d %H:%M:%S')
    
    for wave in waves_sorted:
        print(f"\n  處理波次: {wave.wave_id}")
        
        if wave.wave_id not in wave_orders:
            print(f"    無對應訂單，跳過")
            continue
        
        # 建立該波次的出貨任務
        wave_order_list = wave_orders[wave.wave_id]
        wave_orders_df = pd.DataFrame(wave_order_list)
        
        shipping_tasks = workstation_task_manager.create_tasks_from_orders(wave_orders_df)
        
        print(f"    出貨任務: {len(shipping_tasks)} 個")
        
        # 分配任務到工作站（排除已被佔用的工作站）
        assignment_result = workstation_task_manager.assign_tasks_to_stations(
            shipping_tasks, staff_schedule, current_simulation_time
        )
        
        # 分析分配結果
        assigned_count = len(assignment_result['assigned'])
        unassigned_count = len(assignment_result['unassigned'])
        overtime_count = len(assignment_result.get('overtime_required', []))
        
        total_assigned_tasks += assigned_count
        total_unassigned_tasks += unassigned_count
        total_overtime_required += overtime_count
        
        print(f"    分配結果: 已分配 {assigned_count}, 未分配 {unassigned_count}, 需加班 {overtime_count}")
        
        # 收集該波次使用的工作站
        wave_stations = set()
        wave_total_time = 0
        
        for task_id in assignment_result['assigned']:
            task = workstation_task_manager.tasks[task_id]
            if task.assigned_station:
                wave_stations.add(task.assigned_station)
                wave_total_time += task.estimated_duration
                
                # 記錄工作站使用時間線
                station_usage_timeline[task.assigned_station].append({
                    'wave_id': wave.wave_id,
                    'task_id': task_id,
                    'start_time': task.start_time,
                    'end_time': task.estimated_completion,
                    'duration': task.estimated_duration
                })
        
        # 更新已分配工作站集合
        assigned_stations.update(wave_stations)
        
        # 記錄波次分析結果
        wave_analysis_results[wave.wave_id] = {
            'delivery_time': wave.delivery_time_str,
            'total_orders': len(wave_order_list),
            'total_tasks': len(shipping_tasks),
            'assigned_tasks': assigned_count,
            'unassigned_tasks': unassigned_count,
            'overtime_required': overtime_count,
            'stations_used': len(wave_stations),
            'total_workload': wave_total_time,
            'avg_station_load': wave_total_time / len(wave_stations) if wave_stations else 0,
            'assignment_success_rate': assigned_count / len(shipping_tasks) if shipping_tasks else 0
        }
        
        # 記錄資源使用時間線
        resource_timeline.append({
            'time': wave.latest_cutoff_time,
            'event': f'波次 {wave.wave_id} 開始',
            'stations_used': len(wave_stations),
            'cumulative_stations': len(assigned_stations)
        })
        
        resource_timeline.append({
            'time': wave.delivery_datetime,
            'event': f'波次 {wave.wave_id} 出車',
            'stations_released': len(wave_stations)
        })
    
    # Step 6: 分配進貨任務
    print("\n📥 Step 6: 分配進貨任務...")
    
    receiving_assignment_result = {'assigned': [], 'unassigned': [], 'overtime_required': []}
    
    if receiving_tasks:
        # 在出貨波次之間的空檔分配進貨任務
        receiving_assignment_result = workstation_task_manager.assign_tasks_to_stations(
            receiving_tasks, staff_schedule, current_simulation_time
        )
        
        print(f"  進貨分配結果:")
        print(f"    已分配: {len(receiving_assignment_result['assigned'])} 個")
        print(f"    未分配: {len(receiving_assignment_result['unassigned'])} 個")
        print(f"    需加班: {len(receiving_assignment_result.get('overtime_required', []))} 個")
        
        total_assigned_tasks += len(receiving_assignment_result['assigned'])
        total_unassigned_tasks += len(receiving_assignment_result['unassigned'])
        total_overtime_required += len(receiving_assignment_result.get('overtime_required', []))
    
    # Step 7: 工作站衝突分析
    print("\n⚠️ Step 7: 工作站衝突分析...")
    
    # 檢查工作站時間衝突
    conflicts = []
    
    for station_id, usage_list in station_usage_timeline.items():
        if len(usage_list) <= 1:
            continue
        
        # 按開始時間排序
        usage_list_sorted = sorted(usage_list, key=lambda x: x['start_time'])
        
        for i in range(len(usage_list_sorted) - 1):
            current_task = usage_list_sorted[i]
            next_task = usage_list_sorted[i + 1]
            
            # 檢查時間重疊
            if current_task['end_time'] > next_task['start_time']:
                overlap_minutes = (current_task['end_time'] - next_task['start_time']).total_seconds() / 60
                
                conflicts.append({
                    'station_id': station_id,
                    'first_wave': current_task['wave_id'],
                    'second_wave': next_task['wave_id'],
                    'overlap_minutes': overlap_minutes,
                    'first_end': current_task['end_time'],
                    'second_start': next_task['start_time']
                })
    
    print(f"  發現時間衝突: {len(conflicts)} 個")
    
    if conflicts:
        for conflict in conflicts:
            print(f"    {conflict['station_id']}: {conflict['first_wave']} vs {conflict['second_wave']}")
            print(f"      重疊時間: {conflict['overlap_minutes']:.1f} 分鐘")
    
    # Step 8: 資源利用率分析
    print("\n📊 Step 8: 資源利用率分析...")
    
    # 計算總工作站數
    total_workstations = len(workstation_task_manager.workstations)
    
    # 計算各時段的工作站利用率
    utilization_analysis = {
        'total_stations': total_workstations,
        'max_concurrent_usage': len(assigned_stations),
        'peak_utilization_rate': len(assigned_stations) / total_workstations * 100,
        'average_wave_stations': np.mean([result['stations_used'] for result in wave_analysis_results.values()]),
        'station_conflicts': len(conflicts),
        'resource_efficiency': (total_assigned_tasks / (total_assigned_tasks + total_unassigned_tasks) * 100) if (total_assigned_tasks + total_unassigned_tasks) > 0 else 0
    }
    
    print(f"  資源利用率分析:")
    print(f"    總工作站數: {utilization_analysis['total_stations']}")
    print(f"    最大同時使用: {utilization_analysis['max_concurrent_usage']} 個")
    print(f"    峰值利用率: {utilization_analysis['peak_utilization_rate']:.1f}%")
    print(f"    平均每波次用站: {utilization_analysis['average_wave_stations']:.1f} 個")
    print(f"    資源效率: {utilization_analysis['resource_efficiency']:.1f}%")
    
    # Step 9: 加班需求統計
    print("\n🕒 Step 9: 加班需求統計...")
    
    overtime_analysis = {
        'total_overtime_tasks': total_overtime_required,
        'overtime_by_type': {
            'shipping': 0,
            'receiving': 0
        },
        'estimated_overtime_hours': 0
    }
    
    # 統計出貨加班
    for result in wave_analysis_results.values():
        overtime_analysis['overtime_by_type']['shipping'] += result['overtime_required']
    
    # 統計進貨加班
    overtime_analysis['overtime_by_type']['receiving'] = len(receiving_assignment_result.get('overtime_required', []))
    
    # 估算總加班時數（假設每個需加班任務平均2小時）
    overtime_analysis['estimated_overtime_hours'] = total_overtime_required * 2
    
    print(f"  加班需求統計:")
    print(f"    總需加班任務: {total_overtime_required} 個")
    print(f"    出貨加班: {overtime_analysis['overtime_by_type']['shipping']} 個")
    print(f"    進貨加班: {overtime_analysis['overtime_by_type']['receiving']} 個")
    print(f"    估算加班時數: {overtime_analysis['estimated_overtime_hours']} 小時")
    
    # Step 10: 輸出詳細分析報告
    print(f"\n📁 Step 10: 輸出詳細分析報告...")
    
    # 波次分析報告
    wave_analysis_df = pd.DataFrame([
        {'wave_id': wave_id, **analysis} 
        for wave_id, analysis in wave_analysis_results.items()
    ])
    
    output_file = os.path.join(os.path.dirname(__file__), '..', 'output', 
                             f'daily_wave_analysis_{target_date}.csv')
    wave_analysis_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  波次分析報告: {output_file}")
    
    # 衝突報告
    if conflicts:
        conflicts_df = pd.DataFrame(conflicts)
        output_file = os.path.join(os.path.dirname(__file__), '..', 'output', 
                                 f'station_conflicts_{target_date}.csv')
        conflicts_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"  工作站衝突報告: {output_file}")
    
    # 工作站使用時間線
    station_timeline_records = []
    for station_id, usage_list in station_usage_timeline.items():
        for usage in usage_list:
            station_timeline_records.append({
                'station_id': station_id,
                'wave_id': usage['wave_id'],
                'task_id': usage['task_id'],
                'start_time': usage['start_time'],
                'end_time': usage['end_time'],
                'duration_minutes': usage['duration']
            })
    
    if station_timeline_records:
        timeline_df = pd.DataFrame(station_timeline_records)
        output_file = os.path.join(os.path.dirname(__file__), '..', 'output', 
                                 f'station_timeline_{target_date}.csv')
        timeline_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"  工作站時間線: {output_file}")
    
    # 總結報告
    daily_summary = {
        'date': target_date,
        'total_waves': len(waves),
        'total_shipping_orders': len(processed_orders),
        'total_receiving_tasks': len(receiving_tasks),
        'total_assigned_tasks': total_assigned_tasks,
        'total_unassigned_tasks': total_unassigned_tasks,
        'total_overtime_required': total_overtime_required,
        'assignment_success_rate': (total_assigned_tasks / (total_assigned_tasks + total_unassigned_tasks) * 100) if (total_assigned_tasks + total_unassigned_tasks) > 0 else 0,
        'station_conflicts': len(conflicts),
        'peak_station_utilization': utilization_analysis['peak_utilization_rate'],
        'estimated_overtime_hours': overtime_analysis['estimated_overtime_hours']
    }
    
    summary_df = pd.DataFrame([daily_summary])
    output_file = os.path.join(os.path.dirname(__file__), '..', 'output', 
                             f'daily_summary_{target_date}.csv')
    summary_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  每日總結報告: {output_file}")
    
    # Step 11: 總結
    print(f"\n📋 Step 4 驗證總結:")
    print(f"  處理日期: {target_date}")
    print(f"  波次數量: {len(waves)} 個")
    print(f"  總任務數: {total_assigned_tasks + total_unassigned_tasks} 個")
    print(f"  分配成功率: {daily_summary['assignment_success_rate']:.1f}%")
    print(f"  工作站衝突: {len(conflicts)} 個")
    print(f"  峰值利用率: {utilization_analysis['peak_utilization_rate']:.1f}%")
    print(f"  加班需求: {total_overtime_required} 個任務")
    
    if len(conflicts) == 0 and daily_summary['assignment_success_rate'] >= 90:
        print(f"  ✅ 當日作業協調良好")
    elif len(conflicts) > 0:
        print(f"  ⚠️ 發現工作站時間衝突，需要調整")
    else:
        print(f"  ⚠️ 任務分配成功率偏低，需要檢討")
    
    return {
        'daily_summary': daily_summary,
        'wave_analysis': wave_analysis_results,
        'conflicts': conflicts,
        'utilization_analysis': utilization_analysis,
        'overtime_analysis': overtime_analysis
    }

if __name__ == "__main__":
    try:
        # 可以修改這個參數來測試不同的日期
        target_date = "2024-06-15"
        
        result = validate_daily_wave_coordination(target_date)
        print(f"\n🎯 一天內多波次協調驗證完成！")
        
    except Exception as e:
        print(f"\n❌ 驗證過程發生錯誤: {str(e)}")
        import traceback
        traceback.print_exc()