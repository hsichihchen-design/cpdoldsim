"""
Root Cause Analysis - 根本原因分析
深入檢查 WorkstationTaskManager 的核心分配邏輯
"""

import sys
import os

# 加入父目錄以便 import
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def analyze_assignment_logic():
    """分析分配邏輯的根本問題"""
    print("🔍 根本原因分析...")
    
    # 讀取 workstation_task_manager.py 的關鍵方法
    workstation_file = os.path.join(os.path.dirname(__file__), '..', 'src', 'workstation_task_manager.py')
    
    with open(workstation_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print("\n📋 分析關鍵方法...")
    
    # 問題1: 檢查 _group_tasks_by_type_and_wave 方法
    print("\n🔍 問題1: 任務分組邏輯")
    
    if '_group_tasks_by_type_and_wave' in content:
        print("✅ 找到 _group_tasks_by_type_and_wave 方法")
        
        # 檢查 _determine_task_wave_id 方法
        if '_determine_task_wave_id' in content:
            print("✅ 找到 _determine_task_wave_id 方法")
            
            # 提取方法內容
            start_idx = content.find('def _determine_task_wave_id')
            if start_idx != -1:
                # 找到方法結束位置
                lines = content[start_idx:].split('\n')
                method_lines = []
                indent_level = None
                
                for line in lines:
                    if line.strip().startswith('def _determine_task_wave_id'):
                        indent_level = len(line) - len(line.lstrip())
                        method_lines.append(line)
                    elif indent_level is not None:
                        current_indent = len(line) - len(line.lstrip())
                        if line.strip() and current_indent <= indent_level:
                            break
                        method_lines.append(line)
                
                print("📝 _determine_task_wave_id 方法內容:")
                for line in method_lines[:20]:  # 顯示前20行
                    print(f"    {line}")
                
                # 檢查關鍵邏輯
                method_content = '\n'.join(method_lines)
                if 'WAVE_DEFAULT' in method_content:
                    print("⚠️ 發現問題：方法返回 WAVE_DEFAULT")
                if 'WAVE_UNKNOWN' in method_content:
                    print("🚨 發現問題：方法返回 WAVE_UNKNOWN")
        else:
            print("❌ 找不到 _determine_task_wave_id 方法")
    else:
        print("❌ 找不到 _group_tasks_by_type_and_wave 方法")
    
    # 問題2: 檢查 assign_tasks_to_stations 方法
    print("\n🔍 問題2: 主分配邏輯")
    
    if 'def assign_tasks_to_stations' in content:
        print("✅ 找到 assign_tasks_to_stations 方法")
        
        # 檢查是否有分階段處理
        if '_assign_wave_tasks_with_partcustid_grouping' in content:
            print("✅ 找到波次分配方法")
        else:
            print("❌ 找不到波次分配方法")
        
        if '_assign_other_stage_tasks' in content:
            print("✅ 找到其他階段分配方法")
        else:
            print("❌ 找不到其他階段分配方法")
    
    # 問題3: 檢查時間約束檢查
    print("\n🔍 問題3: 時間約束檢查")
    
    if '_check_wave_deadline_feasibility' in content:
        print("✅ 找到時間可行性檢查方法")
    else:
        print("❌ 找不到時間可行性檢查方法")
    
    # 問題4: 檢查 Bin Packing 實作
    print("\n🔍 問題4: Bin Packing 實作")
    
    if '_assign_partcustids_to_stations' in content:
        print("✅ 找到 Bin Packing 方法")
    else:
        print("❌ 找不到 Bin Packing 方法")
    
    # 問題5: 檢查約束檢查
    print("\n🔍 問題5: 約束檢查")
    
    constraint_checks = [
        'max_partcustids_per_station',
        'time_buffer_minutes', 
        'available_minutes'
    ]
    
    for constraint in constraint_checks:
        if constraint in content:
            print(f"✅ 找到約束: {constraint}")
        else:
            print(f"❌ 找不到約束: {constraint}")
    
    return True

def find_exact_problem():
    """找出確切的問題點"""
    print("\n🎯 找出確切問題點...")
    
    from src.data_manager import DataManager
    from src.workstation_task_manager import WorkstationTaskManager
    
    # 初始化
    data_manager = DataManager()
    data_manager.load_master_data()
    workstation_task_manager = WorkstationTaskManager(data_manager)
    
    # 測試 _determine_task_wave_id 方法
    print("\n🧪 測試 _determine_task_wave_id 方法:")
    
    # 創建一個測試任務
    class MockTask:
        def __init__(self):
            self.partcustid = "C707"
            self.route_code = "R12"
    
    test_task = MockTask()
    current_time = "2025-06-03 08:50:00"
    from datetime import datetime
    current_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
    
    # 測試波次ID確定
    wave_id = workstation_task_manager._determine_task_wave_id(test_task, current_time)
    print(f"  測試任務波次ID: {wave_id}")
    
    if 'UNKNOWN' in wave_id:
        print("🚨 問題確認：波次ID錯誤！")
        
        # 檢查 wave_manager 是否存在
        if hasattr(workstation_task_manager, 'wave_manager'):
            print("✅ workstation_task_manager 有 wave_manager")
            
            # 測試 wave_manager 的方法
            if hasattr(workstation_task_manager.wave_manager, 'find_wave_for_partcustid'):
                print("✅ wave_manager 有 find_wave_for_partcustid 方法")
                
                # 直接測試
                result = workstation_task_manager.wave_manager.find_wave_for_partcustid("C707", current_time)
                print(f"  wave_manager 返回: {result}")
            else:
                print("❌ wave_manager 沒有 find_wave_for_partcustid 方法")
        else:
            print("🚨 關鍵問題：workstation_task_manager 沒有 wave_manager！")
    
    # 測試分組邏輯
    print("\n🧪 測試任務分組邏輯:")
    
    test_tasks = [test_task]
    task_groups = workstation_task_manager._group_tasks_by_type_and_wave(test_tasks, current_time)
    
    print(f"  分組結果: {list(task_groups.keys())}")
    for group_name, group_content in task_groups.items():
        if isinstance(group_content, dict):
            print(f"    {group_name}: {list(group_content.keys())}")
        else:
            print(f"    {group_name}: {len(group_content)} 個任務")

if __name__ == "__main__":
    try:
        analyze_assignment_logic()
        find_exact_problem()
        
    except Exception as e:
        print(f"\n❌ 分析過程發生錯誤: {str(e)}")
        import traceback
        traceback.print_exc()