"""
Validation Controller - 驗證流程主控制器
統籌執行所有驗證步驟，提供友善的操作介面
"""

import os
import sys
from datetime import datetime, timedelta

def print_banner():
    """顯示程式標題"""
    print("=" * 80)
    print("🏭 倉庫模擬系統驗證工具")
    print("   Warehouse Simulation System Validation Tool")
    print("=" * 80)
    print()

def print_menu():
    """顯示主選單"""
    print("📋 驗證步驟選單:")
    print("  1. Step 1: 路線對應關係驗證")
    print("  2. Step 2: 單一波次任務分配驗證") 
    print("  3. Step 3: 波次完成度與時間驗證")
    print("  4. Step 4: 一天內多波次協調驗證")
    print("  5. Step 5: 多天連續運作驗證")
    print("  6. 執行完整驗證流程 (Step 1-5)")
    print("  0. 退出")
    print()

def check_prerequisites():
    """檢查前置條件"""
    print("🔍 檢查前置條件...")
    
    # 檢查資料檔案
    required_files = [
        "../data/master_data/route_schedule_master.csv",
        "../data/master_data/item_master.csv", 
        "../data/master_data/staff_skill_master.csv",
        "../data/master_data/workstation_capacity.csv",
        "../data/master_data/system_parameters.csv",
        "../data/transaction_data/historical_orders.csv"
    ]
    
    missing_files = []
    for file_path in required_files:
        full_path = os.path.join(os.path.dirname(__file__), file_path)
        if not os.path.exists(full_path):
            missing_files.append(file_path)
    
    if missing_files:
        print("❌ 缺少必要檔案:")
        for file_path in missing_files:
            print(f"   {file_path}")
        return False
    
    # 檢查輸出目錄
    output_dir = os.path.join(os.path.dirname(__file__), "..", "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print("✅ 建立輸出目錄")
    
    print("✅ 前置條件檢查通過")
    return True

def get_date_input(prompt, default_date):
    """取得日期輸入"""
    while True:
        date_str = input(f"{prompt} (格式: YYYY-MM-DD, 預設: {default_date}): ").strip()
        
        if not date_str:
            return default_date
        
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return date_str
        except ValueError:
            print("❌ 日期格式錯誤，請使用 YYYY-MM-DD 格式")

def run_step1():
    """執行 Step 1: 路線對應關係驗證"""
    print("\n🔍 執行 Step 1: 路線對應關係驗證...")
    
    try:
        from step1_route_validation import validate_route_mapping
        result = validate_route_mapping()
        
        if result:
            print(f"\n📊 Step 1 結果摘要:")
            print(f"  路線組合總數: {result['total_route_combinations']}")
            print(f"  可對應: {result['matched_routes']} ({result['match_rate']*100:.1f}%)")
            print(f"  無法對應: {result['unmatched_routes']}")
            
            if result['match_rate'] >= 0.8:
                print("✅ 路線對應驗證通過")
                return True
            else:
                print("⚠️ 路線對應率偏低，建議檢查路線資料")
                return False
        
    except Exception as e:
        print(f"❌ Step 1 執行失敗: {str(e)}")
        return False

def run_step2():
    """執行 Step 2: 單一波次任務分配驗證"""
    print("\n🌊 執行 Step 2: 單一波次任務分配驗證...")
    
    # 取得參數
    target_date = get_date_input("請輸入目標日期", "2024-06-15")
    
    print("\n🔍 先列出可用波次...")
    try:
        from step2_wave_task_validation import list_available_waves
        list_available_waves(target_date)
    except Exception as e:
        print(f"⚠️ 無法列出波次: {str(e)}")
    
    target_delivery_time = input("請輸入目標出車時間 (格式: HHMM, 如: 1000): ").strip()
    if not target_delivery_time:
        target_delivery_time = "1000"
    
    try:
        from step2_wave_task_validation import validate_single_wave_assignment
        result = validate_single_wave_assignment(target_date, target_delivery_time)
        
        if result:
            print(f"\n📊 Step 2 結果摘要:")
            print(f"  波次ID: {result['wave_id']}")
            print(f"  任務總數: {result['total_tasks']}")
            print(f"  分配成功: {result['assigned_tasks']}")
            print(f"  分配成功率: {result['assignment_success_rate']*100:.1f}%")
            print(f"  使用工作站: {result['stations_used']} 個")
            
            if result['assignment_success_rate'] >= 0.9:
                print("✅ 波次任務分配驗證通過")
                return True
            else:
                print("⚠️ 任務分配成功率偏低")
                return False
        
    except Exception as e:
        print(f"❌ Step 2 執行失敗: {str(e)}")
        return False

def run_step3():
    """執行 Step 3: 波次完成度與時間驗證"""
    print("\n⏰ 執行 Step 3: 波次完成度與時間驗證...")
    
    target_date = get_date_input("請輸入目標日期", "2024-06-15")
    target_delivery_time = input("請輸入目標出車時間 (格式: HHMM, 如: 1000): ").strip()
    if not target_delivery_time:
        target_delivery_time = "1000"
    
    try:
        from step3_wave_completion_validation import validate_wave_completion_feasibility
        result = validate_wave_completion_feasibility(target_date, target_delivery_time)
        
        if result:
            print(f"\n📊 Step 3 結果摘要:")
            print(f"  可行性: {result['feasibility_status']}")
            print(f"  時間餘裕: {result['time_margin_minutes']:.1f} 分鐘")
            print(f"  瓶頸工作站: {result['bottleneck_station']}")
            print(f"  負載不平衡: {result['load_imbalance']:.1f} 分鐘")
            print(f"  需要加班: {'是' if result['overtime_required'] else '否'}")
            
            if result['feasibility_status'] == 'FEASIBLE':
                print("✅ 波次時間驗證通過")
                return True
            else:
                print("⚠️ 波次無法按時完成")
                return False
        
    except Exception as e:
        print(f"❌ Step 3 執行失敗: {str(e)}")
        return False

def run_step4():
    """執行 Step 4: 一天內多波次協調驗證"""
    print("\n📅 執行 Step 4: 一天內多波次協調驗證...")
    
    target_date = get_date_input("請輸入目標日期", "2024-06-15")
    
    try:
        from step4_daily_coordination_validation import validate_daily_wave_coordination
        result = validate_daily_wave_coordination(target_date)
        
        if result:
            summary = result['daily_summary']
            print(f"\n📊 Step 4 結果摘要:")
            print(f"  處理波次: {summary['total_waves']} 個")
            print(f"  總任務數: {summary['total_assigned_tasks'] + summary['total_unassigned_tasks']}")
            print(f"  分配成功率: {summary['assignment_success_rate']:.1f}%")
            print(f"  工作站衝突: {summary['station_conflicts']} 個")
            print(f"  峰值利用率: {summary['peak_station_utilization']:.1f}%")
            print(f"  估算加班: {summary['estimated_overtime_hours']:.1f} 小時")
            
            if summary['assignment_success_rate'] >= 85 and summary['station_conflicts'] <= 2:
                print("✅ 每日協調驗證通過")
                return True
            else:
                print("⚠️ 每日協調存在問題")
                return False
        
    except Exception as e:
        print(f"❌ Step 4 執行失敗: {str(e)}")
        return False

def run_step5():
    """執行 Step 5: 多天連續運作驗證"""
    print("\n📊 執行 Step 5: 多天連續運作驗證...")
    
    start_date = get_date_input("請輸入開始日期", "2024-06-10")
    end_date = get_date_input("請輸入結束日期", "2024-06-16")
    
    try:
        from step5_multi_day_validation import validate_multi_day_operations
        result = validate_multi_day_operations(start_date, end_date)
        
        if result:
            stability = result['stability_metrics']
            print(f"\n📊 Step 5 結果摘要:")
            print(f"  驗證期間: {result['period']}")
            print(f"  平均分配成功率: {stability['avg_success_rate']:.1f}%")
            print(f"  平均利用率: {stability['avg_utilization']:.1f}%")
            print(f"  系統健康度: {result['health_score']}/100")
            print(f"  主要瓶頸: {result['bottlenecks'][0] if result['bottlenecks'] else '無'}")
            
            if result['health_score'] >= 80:
                print("✅ 多天運作驗證通過")
                return True
            else:
                print("⚠️ 多天運作存在問題")
                return False
        
    except Exception as e:
        print(f"❌ Step 5 執行失敗: {str(e)}")
        return False

def run_full_validation():
    """執行完整驗證流程"""
    print("\n🚀 執行完整驗證流程 (Step 1-5)...")
    
    steps = [
        ("Step 1: 路線對應關係驗證", run_step1),
        ("Step 2: 單一波次任務分配驗證", run_step2),
        ("Step 3: 波次完成度與時間驗證", run_step3), 
        ("Step 4: 一天內多波次協調驗證", run_step4),
        ("Step 5: 多天連續運作驗證", run_step5)
    ]
    
    results = {}
    
    for step_name, step_func in steps:
        print(f"\n{'='*60}")
        print(f"📍 {step_name}")
        print('='*60)
        
        try:
            result = step_func()
            results[step_name] = result
            
            if result:
                print(f"✅ {step_name} 完成")
            else:
                print(f"⚠️ {step_name} 有問題")
                
                # 詢問是否繼續
                continue_choice = input("是否繼續下一步驗證? (y/n): ").strip().lower()
                if continue_choice != 'y':
                    break
                    
        except Exception as e:
            print(f"❌ {step_name} 執行失敗: {str(e)}")
            results[step_name] = False
            
            continue_choice = input("是否繼續下一步驗證? (y/n): ").strip().lower()
            if continue_choice != 'y':
                break
    
    # 顯示完整結果摘要
    print(f"\n{'='*60}")
    print(f"📋 完整驗證結果摘要")
    print('='*60)
    
    passed_count = sum(1 for result in results.values() if result)
    total_count = len(results)
    
    print(f"總體通過率: {passed_count}/{total_count} ({passed_count/total_count*100:.1f}%)")
    print()
    
    for step_name, result in results.items():
        status = "✅ 通過" if result else "❌ 失敗"
        print(f"  {status} {step_name}")
    
    print()
    if passed_count == total_count:
        print("🎉 所有驗證步驟都通過！系統運作正常。")
    elif passed_count >= total_count * 0.8:
        print("⚠️ 大部分驗證通過，但仍有改善空間。")
    else:
        print("❌ 多項驗證失敗，系統需要重大調整。")

def main():
    """主程式"""
    print_banner()
    
    # 檢查前置條件
    if not check_prerequisites():
        print("❌ 前置條件檢查失敗，請確認資料檔案完整後重新執行")
        return
    
    while True:
        print_menu()
        
        try:
            choice = input("請選擇驗證步驟 (0-6): ").strip()
            
            if choice == '0':
                print("👋 感謝使用倉庫模擬系統驗證工具！")
                break
            elif choice == '1':
                run_step1()
            elif choice == '2':
                run_step2()
            elif choice == '3':
                run_step3()
            elif choice == '4':
                run_step4()
            elif choice == '5':
                run_step5()
            elif choice == '6':
                run_full_validation()
            else:
                print("❌ 無效選擇，請輸入 0-6")
                continue
            
            # 詢問是否繼續
            print("\n" + "="*60)
            continue_choice = input("是否繼續其他驗證? (y/n): ").strip().lower()
            if continue_choice != 'y':
                print("👋 感謝使用倉庫模擬系統驗證工具！")
                break
            
            print()  # 空行分隔
            
        except KeyboardInterrupt:
            print("\n\n👋 程式被使用者中斷")
            break
        except Exception as e:
            print(f"\n❌ 程式執行錯誤: {str(e)}")
            continue

if __name__ == "__main__":
    main()