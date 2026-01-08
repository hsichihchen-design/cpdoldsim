"""
Step 2: 單一波次任務分配驗證
驗證特定波次的任務如何分配到工作站，每個工作站分配到什麼任務
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

def validate_single_wave_assignment(target_date="2025-06-05", target_delivery_time="1000"):
    """驗證單一波次的任務分配"""
    print(f"🌊 Step 2: 驗證單一波次任務分配...")
    print(f"  目標日期: {target_date}")
    print(f"  目標出車時間: {target_delivery_time}")
    
    # 初始化各個管理器
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
    print(f"📊 當日訂單數量: {len(orders_df):,} 筆")
    
    # 初始化管理器
    order_priority_manager = OrderPriorityManager(data_manager)
    
    # 建立 workstation_task_manager 的虛擬實例
    class MockWorkstationManager:
        def __init__(self, data_manager):
            self.workstations = {}
            self.tasks = {}
        
    workstation_manager = MockWorkstationManager(data_manager)
    wave_manager = WaveManager(data_manager, workstation_manager)
    workstation_task_manager = WorkstationTaskManager(data_manager, wave_manager)
    staff_schedule_generator = StaffScheduleGenerator(data_manager)
    
    # 🔧 修正：設定 wave_manager 讓任務能正確分配到波次
    workstation_task_manager.wave_manager = wave_manager
    
    # Step 1: 處理訂單優先權
    print("\n📋 Step 1: 處理訂單優先權...")
    processed_orders = order_priority_manager.process_orders_batch(orders_df)
    
    priority_stats = processed_orders['priority_level'].value_counts()
    print(f"  優先權分布: {dict(priority_stats)}")
    
    # Step 2: 建立當日波次
    print("\n🌊 Step 2: 建立當日波次...")
    target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
    waves = wave_manager.create_waves_from_schedule(target_datetime)
    
    print(f"  建立波次數量: {len(waves)} 個")
    
    # 找到目標波次
    target_wave = None
    for wave in waves:
        if wave.delivery_time_str == target_delivery_time:
            target_wave = wave
            break
    
    if not target_wave:
        print(f"❌ 找不到出車時間 {target_delivery_time} 的波次！")
        print(f"可用的出車時間: {[wave.delivery_time_str for wave in waves]}")
        return
    
    print(f"✅ 找到目標波次: {target_wave.wave_id}")
    print(f"  出車時間: {target_wave.delivery_time_str}")
    print(f"  包含路線: {target_wave.included_routes}")
    print(f"  包含據點: {len(target_wave.included_partcustids)} 個")
    
    # Step 3: 篩選屬於該波次的訂單
    print("\n🎯 Step 3: 篩選屬於該波次的訂單...")
    
    # 找到波次對應的路線和據點組合
    wave_orders = processed_orders[
        (processed_orders['ROUTECD'].isin(target_wave.included_routes)) &
        (processed_orders['PARTCUSTID'].isin(target_wave.included_partcustids))
    ].copy()
    
    print(f"  波次訂單數量: {len(wave_orders):,} 筆")
    print(f"  佔當日訂單比例: {len(wave_orders)/len(processed_orders)*100:.1f}%")
    
    if len(wave_orders) == 0:
        print("⚠️ 該波次沒有對應的訂單！")
        return
    
    print()  # 空行分隔

    # 分析波次訂單的據點分布
    partcustid_stats = wave_orders['PARTCUSTID'].value_counts()
    print(f"\n📊 波次中的據點分布:")
    for partcustid, count in partcustid_stats.head(10).items():
        print(f"  {partcustid}: {count:,} 筆訂單")
    
    # Step 4: 建立出貨任務
    print("\n📦 Step 4: 建立出貨任務...")
    shipping_tasks = workstation_task_manager.create_tasks_from_orders(wave_orders)
    
    print(f"  建立任務數量: {len(shipping_tasks)} 個")

    # 🔧 DEBUG: 確認 wave_manager 設定
    print(f"\n🔧 DEBUG: 檢查 wave_manager 設定...")
    print(f"  wave_manager 是否為 None: {workstation_task_manager.wave_manager is None}")

    if shipping_tasks:
        test_task = shipping_tasks[0]
        # 🔧 修正：使用當前時間或目標波次的時間
        debug_current_time = datetime.strptime(f"{target_date} 08:50:00", '%Y-%m-%d %H:%M:%S')
        test_wave_id = workstation_task_manager._determine_task_wave_id(test_task, debug_current_time)
        print(f"  測試任務波次ID: {test_wave_id}")
        print(f"  預期波次ID: {target_wave.wave_id}")
        
        if test_wave_id.startswith('WAVE_UNKNOWN'):
            print(f"  ❌ 波次ID仍然錯誤！")
        else:
            print(f"  ✅ 波次ID正確！")
    
    # 分析任務分布
    task_stats = {
        'total_tasks': len(shipping_tasks),
        'by_floor': {},
        'by_priority': {},
        'by_partcustid': {},
        'requires_repack': 0,
        'total_estimated_time': 0
    }
    
    for task in shipping_tasks:
        # 樓層分布
        floor = task.floor
        task_stats['by_floor'][floor] = task_stats['by_floor'].get(floor, 0) + 1
        
        # 優先權分布
        priority = task.priority_level
        task_stats['by_priority'][priority] = task_stats['by_priority'].get(priority, 0) + 1
        
        # 據點分布
        partcustid = task.partcustid or 'UNKNOWN'
        task_stats['by_partcustid'][partcustid] = task_stats['by_partcustid'].get(partcustid, 0) + 1
        
        # 再包裝統計
        if task.requires_repack:
            task_stats['requires_repack'] += 1
        
        # 總預估時間
        task_stats['total_estimated_time'] += task.estimated_duration
    
    print(f"  任務統計:")
    print(f"    樓層分布: {task_stats['by_floor']}")
    print(f"    優先權分布: {task_stats['by_priority']}")
    print(f"    需要再包裝: {task_stats['requires_repack']} 個")
    print(f"    總預估時間: {task_stats['total_estimated_time']:.1f} 分鐘")
    
    # Step 5: 生成員工排班
    print("\n👥 Step 5: 生成員工排班...")
    staff_schedule = staff_schedule_generator.generate_daily_schedule(target_date)
    
    print(f"  員工排班數量: {len(staff_schedule)} 個班次")
    
    floor_staff_stats = staff_schedule['floor'].value_counts()
    print(f"  各樓層人力: {dict(floor_staff_stats)}")
    
    # Step 6: 執行任務分配
    print("\n🎯 Step 6: 執行任務分配...")
    current_time = datetime.strptime(f"{target_date} 08:50:00", '%Y-%m-%d %H:%M:%S')
    
    assignment_result = workstation_task_manager.assign_tasks_to_stations(
        shipping_tasks, staff_schedule, current_time
    )
    
    print(f"  分配結果:")
    print(f"    已分配任務: {len(assignment_result['assigned'])} 個")
    print(f"    未分配任務: {len(assignment_result['unassigned'])} 個")
    print(f"    需要加班: {len(assignment_result.get('overtime_required', []))} 個")
    print(f"    錯誤任務: {len(assignment_result.get('errors', []))} 個")
    
    # Step 7: 分析工作站分配詳情
    print("\n🏗️ Step 7: 分析工作站分配詳情...")
    
    station_assignments = {}
    task_details = []
    
    for task_id in assignment_result['assigned']:
        task = workstation_task_manager.tasks[task_id]
        
        if task.assigned_station:
            if task.assigned_station not in station_assignments:
                station_assignments[task.assigned_station] = {
                    'station_id': task.assigned_station,
                    'floor': task.floor,
                    'assigned_staff': task.assigned_staff,
                    'tasks': [],
                    'partcustids': set(),
                    'total_time': 0,
                    'task_count': 0
                }
            
            station_info = station_assignments[task.assigned_station]
            station_info['tasks'].append(task_id)
            station_info['partcustids'].add(task.partcustid)
            station_info['total_time'] += task.estimated_duration
            station_info['task_count'] += 1
            
            task_details.append({
                'task_id': task_id,
                'order_id': task.order_id,
                'station_id': task.assigned_station,
                'assigned_staff': task.assigned_staff,
                'floor': task.floor,
                'partcustid': task.partcustid,
                'frcd': task.frcd,
                'partno': task.partno,
                'quantity': task.quantity,
                'priority_level': task.priority_level,
                'requires_repack': task.requires_repack,
                'estimated_duration': task.estimated_duration,
                'start_time': task.start_time,
                'estimated_completion': task.estimated_completion
            })
    
    print(f"  分配的工作站數量: {len(station_assignments)} 個")
    
    # 詳細工作站分析
    print(f"\n📋 工作站分配詳情:")
    for station_id, info in sorted(station_assignments.items()):
        print(f"  {station_id} (樓層{info['floor']}, 員工{info['assigned_staff']}):")
        print(f"    任務數量: {info['task_count']} 個")
        print(f"    據點數量: {len(info['partcustids'])} 個")
        print(f"    據點清單: {sorted(list(info['partcustids']))}")
        print(f"    總預估時間: {info['total_time']:.1f} 分鐘")
        print(f"    平均任務時間: {info['total_time']/info['task_count']:.1f} 分鐘")
    
    # Step 8: 檢查據點分組邏輯
    print(f"\n🎯 Step 8: 檢查據點分組邏輯...")
    
    partcustid_distribution = {}
    for station_id, info in station_assignments.items():
        for partcustid in info['partcustids']:
            if partcustid not in partcustid_distribution:
                partcustid_distribution[partcustid] = []
            partcustid_distribution[partcustid].append(station_id)
    
    # 檢查是否有據點被分散到多個工作站
    scattered_partcustids = {
        partcustid: stations for partcustid, stations in partcustid_distribution.items()
        if len(stations) > 1
    }
    
    if scattered_partcustids:
        print(f"  ⚠️ 發現分散的據點 ({len(scattered_partcustids)} 個):")
        for partcustid, stations in scattered_partcustids.items():
            print(f"    {partcustid}: 分散到 {stations}")
    else:
        print(f"  ✅ 所有據點都保持完整分組")
    
    # Step 9: 輸出詳細報告
    print(f"\n📁 Step 9: 輸出詳細報告...")
    
    # 保存任務分配詳情
    task_details_df = pd.DataFrame(task_details)
    if len(task_details_df) > 0:
        output_file = os.path.join(os.path.dirname(__file__), '..', 'output', 
                                 f'wave_task_assignment_{target_date}_{target_delivery_time}.csv')
        task_details_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"  任務分配詳情: {output_file}")
    
    # 保存工作站摘要
    station_summary = []
    for station_id, info in station_assignments.items():
        station_summary.append({
            'station_id': station_id,
            'floor': info['floor'],
            'assigned_staff': info['assigned_staff'],
            'task_count': info['task_count'],
            'partcustid_count': len(info['partcustids']),
            'partcustids': ','.join(sorted(list(info['partcustids']))),
            'total_time_minutes': round(info['total_time'], 1),
            'avg_task_time_minutes': round(info['total_time']/info['task_count'], 1)
        })
    
    station_summary_df = pd.DataFrame(station_summary)
    if len(station_summary_df) > 0:
        output_file = os.path.join(os.path.dirname(__file__), '..', 'output', 
                                 f'wave_station_summary_{target_date}_{target_delivery_time}.csv')
        station_summary_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"  工作站摘要: {output_file}")
    
    # Step 10: 總結
    print(f"\n📋 Step 2 驗證總結:")
    print(f"  波次ID: {target_wave.wave_id}")
    print(f"  訂單數量: {len(wave_orders):,} 筆")
    print(f"  任務數量: {len(shipping_tasks)} 個")
    print(f"  工作站數量: {len(station_assignments)} 個")
    print(f"  分配成功率: {len(assignment_result['assigned'])/len(shipping_tasks)*100:.1f}%")
    print(f"  總預估時間: {task_stats['total_estimated_time']:.1f} 分鐘")
    print(f"  平均每工作站時間: {task_stats['total_estimated_time']/len(station_assignments):.1f} 分鐘" if station_assignments else "N/A")
    
    return {
        'wave_id': target_wave.wave_id,
        'total_orders': len(wave_orders),
        'total_tasks': len(shipping_tasks),
        'assigned_tasks': len(assignment_result['assigned']),
        'unassigned_tasks': len(assignment_result['unassigned']),
        'stations_used': len(station_assignments),
        'total_estimated_time': task_stats['total_estimated_time'],
        'assignment_success_rate': len(assignment_result['assigned'])/len(shipping_tasks) if shipping_tasks else 0,
        'scattered_partcustids': len(scattered_partcustids),
        'station_assignments': station_assignments
    }

def list_available_waves(target_date="2025-06-05"):
    """列出指定日期可用的波次"""
    print(f"📅 列出 {target_date} 可用的波次...")
    
    data_manager = DataManager()
    master_data = data_manager.load_master_data()
    
    if 'route_schedule_master' not in master_data:
        print("❌ 找不到路線時刻表資料！")
        return
    
    # 建立 mock workstation manager
    class MockWorkstationManager:
        def __init__(self):
            self.workstations = {}
            self.tasks = {}
    
    workstation_manager = MockWorkstationManager()
    wave_manager = WaveManager(data_manager, workstation_manager)
    
    target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
    waves = wave_manager.create_waves_from_schedule(target_datetime)
    
    print(f"\n🌊 可用波次 ({len(waves)} 個):")
    for wave in waves:
        print(f"  {wave.delivery_time_str}: {wave.wave_id}")
        print(f"    路線: {wave.included_routes}")
        print(f"    據點數量: {len(wave.included_partcustids)}")
        if hasattr(wave, 'latest_cutoff_time') and wave.latest_cutoff_time:
            print(f"    截止時間: {wave.latest_cutoff_time.strftime('%H:%M')}")
        print(f"    可用時間: {wave.available_work_time_minutes} 分鐘")
        print()

if __name__ == "__main__":
    try:
        # 可以修改這些參數來測試不同的波次
        target_date = "2025-06-05"  # 修改為您想測試的日期
        
        # 先列出可用的波次
        print("🔍 Step 0: 列出可用波次...")
        list_available_waves(target_date)
        
        # 選擇一個波次進行詳細驗證
        target_delivery_time = "1000"  # 修改為您想測試的出車時間
        
        result = validate_single_wave_assignment(target_date, target_delivery_time)
        print(f"\n🎯 單一波次驗證完成！")
        
    except Exception as e:
        print(f"\n❌ 驗證過程發生錯誤: {str(e)}")
        import traceback
        traceback.print_exc()