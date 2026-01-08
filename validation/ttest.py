"""
波次時間驗證程式
檢查特定據點的訂單時間是否符合波次截止時間要求
"""

import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, time, timedelta

# 加入父目錄以便 import
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_manager import DataManager
from src.order_priority_manager import OrderPriorityManager
from src.wave_manager import WaveManager

def validate_wave_time_constraints(target_date="2025-06-05", target_delivery_time="1000", target_partcustid="C718"):
    """驗證波次時間約束"""
    print(f"⏰ 驗證波次時間約束...")
    print(f"  目標日期: {target_date}")
    print(f"  目標出車時間: {target_delivery_time}")
    print(f"  目標據點: {target_partcustid}")
    
    # 初始化管理器
    data_manager = DataManager()
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
    order_priority_manager = OrderPriorityManager(data_manager)
    
    # 建立 wave manager
    class MockWorkstationManager:
        def __init__(self):
            self.workstations = {}
            self.tasks = {}
    
    workstation_manager = MockWorkstationManager()
    wave_manager = WaveManager(data_manager, workstation_manager)
    
    # 處理訂單優先權
    processed_orders = order_priority_manager.process_orders_batch(orders_df)
    
    # 建立當日波次
    target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
    waves = wave_manager.create_waves_from_schedule(target_datetime)
    
    # 找到目標波次
    target_wave = None
    for wave in waves:
        if wave.delivery_time_str == target_delivery_time:
            target_wave = wave
            break
    
    if not target_wave:
        print(f"❌ 找不到出車時間 {target_delivery_time} 的波次！")
        return
    
    print(f"\n🌊 目標波次資訊:")
    print(f"  波次ID: {target_wave.wave_id}")
    print(f"  出車時間: {target_wave.delivery_time_str}")
    if target_wave.delivery_datetime:
        print(f"  出車時間 (完整): {target_wave.delivery_datetime}")
    if target_wave.latest_cutoff_time:
        print(f"  最晚截止時間: {target_wave.latest_cutoff_time}")
    print(f"  可用作業時間: {target_wave.available_work_time_minutes} 分鐘")
    
    # 🎯 重點：檢查特定據點的所有訂單
    print(f"\n🔍 檢查據點 {target_partcustid} 的所有訂單...")
    
    partcustid_orders = processed_orders[
        processed_orders['PARTCUSTID'] == target_partcustid
    ].copy()
    
    print(f"  據點 {target_partcustid} 總訂單數: {len(partcustid_orders)} 筆")
    
    if len(partcustid_orders) == 0:
        print(f"❌ 找不到據點 {target_partcustid} 的訂單！")
        return
    
    # 檢查這些訂單的路線是否符合目標波次
    matching_route_orders = partcustid_orders[
        partcustid_orders['ROUTECD'].isin(target_wave.included_routes)
    ].copy()
    
    print(f"  符合路線的訂單數: {len(matching_route_orders)} 筆")
    print(f"  目標波次包含路線: {target_wave.included_routes}")
    
    # 🚨 關鍵：檢查訂單時間
    print(f"\n⏰ 檢查訂單時間約束...")
    
    # 解析訂單時間
    def parse_order_time(time_str):
        """解析訂單時間字串"""
        if pd.isna(time_str) or time_str == '':
            return None
        try:
            time_str = str(time_str).strip()
            if ':' in time_str:
                parts = time_str.split(':')
                hour = int(parts[0])
                minute = int(parts[1])
                second = int(parts[2]) if len(parts) > 2 else 0
                return time(hour, minute, second)
        except:
            return None
        return None
    
    # 為所有訂單添加解析後的時間
    matching_route_orders['parsed_time'] = matching_route_orders['TIME'].apply(parse_order_time)
    
    # 移除無法解析時間的訂單
    valid_time_orders = matching_route_orders[
        matching_route_orders['parsed_time'].notna()
    ].copy()
    
    print(f"  有效時間的訂單數: {len(valid_time_orders)} 筆")
    
    if len(valid_time_orders) == 0:
        print("❌ 沒有有效時間的訂單！")
        return
    
    # 🎯 核心檢查：比較訂單時間與波次截止時間
    if not target_wave.latest_cutoff_time:
        print("⚠️ 目標波次沒有截止時間資訊！")
        return
    
    cutoff_time = target_wave.latest_cutoff_time.time()
    print(f"  波次截止時間: {cutoff_time}")
    
    # 分類訂單
    valid_orders = []
    late_orders = []
    
    for idx, order in valid_time_orders.iterrows():
        order_time = order['parsed_time']
        
        # 比較時間（簡單比較，不考慮跨日）
        if order_time <= cutoff_time:
            valid_orders.append(order)
        else:
            late_orders.append(order)
    
    print(f"\n📊 時間檢查結果:")
    print(f"  符合時間要求的訂單: {len(valid_orders)} 筆")
    print(f"  超出截止時間的訂單: {len(late_orders)} 筆")
    print(f"  超時比例: {len(late_orders)/len(valid_time_orders)*100:.1f}%")
    
    # 詳細分析超時訂單
    if late_orders:
        print(f"\n🚨 超時訂單詳情:")
        late_df = pd.DataFrame(late_orders)
        
        # 確保有 parsed_time 欄位，如果沒有則重新解析
        if 'parsed_time' not in late_df.columns:
            late_df['parsed_time'] = late_df['TIME'].apply(parse_order_time)
        
        # 計算時間分鐘數
        late_df['time_minutes'] = late_df['parsed_time'].apply(
            lambda t: t.hour * 60 + t.minute if t and hasattr(t, 'hour') else 0
        )
        
        # 按時間排序
        late_df = late_df.sort_values('time_minutes')
        
        # 過濾掉無效時間的記錄
        valid_late_df = late_df[late_df['parsed_time'].notna()]
        
        if len(valid_late_df) > 0:
            print(f"  時間範圍: {valid_late_df['parsed_time'].min()} - {valid_late_df['parsed_time'].max()}")
            
            # 顯示前10個超時訂單
            print(f"  前10個超時訂單:")
            cutoff_minutes = cutoff_time.hour * 60 + cutoff_time.minute
            
            for i, (_, order) in enumerate(valid_late_df.head(10).iterrows()):
                if order['time_minutes'] > 0:  # 確保是有效時間
                    overtime_minutes = order['time_minutes'] - cutoff_minutes
                    print(f"    {order['INDEXNO']}: {order['parsed_time']} (超時 {overtime_minutes} 分鐘)")
            
            # 時間分布統計
            late_hours = valid_late_df['parsed_time'].apply(lambda t: t.hour if t and hasattr(t, 'hour') else 0)
            hour_dist = late_hours.value_counts().sort_index()
            print(f"  超時訂單按小時分布:")
            for hour, count in hour_dist.items():
                print(f"    {hour:02d}:xx - {count} 筆")
        else:
            print("  無有效的超時訂單時間資料")
    
    # 驗證原始篩選邏輯的問題
    print(f"\n🔧 驗證原始篩選邏輯:")
    
    # 原始邏輯（只檢查路線和據點）
    original_filter_orders = processed_orders[
        (processed_orders['ROUTECD'].isin(target_wave.included_routes)) &
        (processed_orders['PARTCUSTID'] == target_partcustid)
    ]
    
    print(f"  原始邏輯篩選的 {target_partcustid} 訂單: {len(original_filter_orders)} 筆")
    print(f"  應該篩選的 {target_partcustid} 訂單: {len(valid_orders)} 筆")
    print(f"  差異: {len(original_filter_orders) - len(valid_orders)} 筆 (應該被排除)")
    
    # 🎯 建議修正的篩選邏輯
    print(f"\n✅ 建議的修正篩選邏輯:")
    print(f"  1. 先按路線和據點篩選")
    print(f"  2. 再檢查訂單時間是否在波次截止時間之前")
    print(f"  3. 移除超時的訂單")
    
    # 輸出詳細報告
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    if late_orders:
        late_df = pd.DataFrame(late_orders)
        
        # 確保有 parsed_time 欄位
        if 'parsed_time' not in late_df.columns:
            late_df['parsed_time'] = late_df['TIME'].apply(parse_order_time)
        
        # 重新計算時間分鐘數和超時分鐘數
        late_df['time_minutes'] = late_df['parsed_time'].apply(
            lambda t: t.hour * 60 + t.minute if t and hasattr(t, 'hour') else 0
        )
        cutoff_minutes = cutoff_time.hour * 60 + cutoff_time.minute
        late_df['overtime_minutes'] = late_df['time_minutes'] - cutoff_minutes
        
        # 只保存有效時間的記錄
        valid_late_df = late_df[late_df['parsed_time'].notna()]
        
        if len(valid_late_df) > 0:
            output_file = os.path.join(output_dir, f'late_orders_{target_partcustid}_{target_date}_{target_delivery_time}.csv')
            valid_late_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"  超時訂單詳情: {output_file}")
        else:
            print(f"  無有效的超時訂單可輸出")
    
    return {
        'total_orders': len(partcustid_orders),
        'matching_route_orders': len(matching_route_orders), 
        'valid_time_orders': len(valid_time_orders),
        'valid_orders': len(valid_orders),
        'late_orders': len(late_orders),
        'late_percentage': len(late_orders)/len(valid_time_orders)*100 if len(valid_time_orders) > 0 else 0,
        'cutoff_time': cutoff_time,
        'late_orders_data': late_orders if late_orders else []
    }

def check_all_partcustids_for_wave(target_date="2025-06-05", target_delivery_time="1000"):
    """檢查波次中所有據點的時間約束問題"""
    print(f"🔍 檢查波次中所有據點的時間約束...")
    
    # 初始化
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
    
    processed_orders = order_priority_manager.process_orders_batch(orders_df)
    target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
    waves = wave_manager.create_waves_from_schedule(target_datetime)
    
    # 找到目標波次
    target_wave = None
    for wave in waves:
        if wave.delivery_time_str == target_delivery_time:
            target_wave = wave
            break
    
    if not target_wave or not target_wave.latest_cutoff_time:
        print("❌ 找不到目標波次或截止時間！")
        return
    
    cutoff_time = target_wave.latest_cutoff_time.time()
    print(f"波次截止時間: {cutoff_time}")
    
    # 檢查每個據點
    partcustid_issues = []
    
    for partcustid in target_wave.included_partcustids:
        # 取得該據點在該波次路線的訂單
        partcustid_orders = processed_orders[
            (processed_orders['PARTCUSTID'] == partcustid) &
            (processed_orders['ROUTECD'].isin(target_wave.included_routes))
        ].copy()
        
        if len(partcustid_orders) == 0:
            continue
        
        # 解析時間並檢查
        def safe_parse_time(time_str):
            """安全解析時間字串"""
            if pd.isna(time_str) or time_str == '':
                return None
            try:
                time_str = str(time_str).strip()
                if ':' in time_str:
                    parts = time_str.split(':')
                    hour = int(parts[0])
                    minute = int(parts[1])
                    second = int(parts[2]) if len(parts) > 2 else 0
                    return time(hour, minute, second)
            except:
                return None
            return None
        
        partcustid_orders['parsed_time'] = partcustid_orders['TIME'].apply(safe_parse_time)
        
        valid_time_orders = partcustid_orders[partcustid_orders['parsed_time'].notna()]
        
        if len(valid_time_orders) == 0:
            continue
        
        # 統計超時訂單
        late_count = 0
        for _, order in valid_time_orders.iterrows():
            order_time = order['parsed_time']
            if order_time and hasattr(order_time, 'hour') and order_time > cutoff_time:
                late_count += 1
        
        if late_count > 0:
            late_percentage = late_count / len(valid_time_orders) * 100
            partcustid_issues.append({
                'partcustid': partcustid,
                'total_orders': len(valid_time_orders),
                'late_orders': late_count,
                'late_percentage': late_percentage
            })
    
    # 排序並顯示問題據點
    partcustid_issues.sort(key=lambda x: x['late_orders'], reverse=True)
    
    print(f"\n🚨 發現時間約束問題的據點:")
    for issue in partcustid_issues:
        print(f"  {issue['partcustid']}: {issue['late_orders']}/{issue['total_orders']} 筆超時 ({issue['late_percentage']:.1f}%)")
    
    return partcustid_issues

if __name__ == "__main__":
    try:
        target_date = "2025-06-05"
        target_delivery_time = "1000"
        target_partcustid = "C718"
        
        print("="*60)
        print("🕐 波次時間約束驗證")
        print("="*60)
        
        # 1. 檢查特定據點
        print(f"\n1️⃣ 檢查特定據點 {target_partcustid}...")
        result = validate_wave_time_constraints(target_date, target_delivery_time, target_partcustid)
        
        # 2. 檢查所有據點
        print(f"\n2️⃣ 檢查波次中所有據點...")
        all_issues = check_all_partcustids_for_wave(target_date, target_delivery_time)
        
        print(f"\n📋 總結:")
        if result:
            print(f"  {target_partcustid} 據點: {result['late_orders']}/{result['valid_time_orders']} 筆超時 ({result['late_percentage']:.1f}%)")
        print(f"  問題據點總數: {len(all_issues)} 個")
        
        if all_issues:
            total_late = sum(issue['late_orders'] for issue in all_issues)
            total_orders = sum(issue['total_orders'] for issue in all_issues)
            print(f"  整體超時比例: {total_late}/{total_orders} ({total_late/total_orders*100:.1f}%)")
        
    except Exception as e:
        print(f"\n❌ 驗證過程發生錯誤: {str(e)}")
        import traceback
        traceback.print_exc()