"""
Step 1: 路線對應關係驗證
驗證歷史訂單中的 ROUTECD + PARTCUSTID 是否都能在 route_schedule_master 中找到對應
"""

import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, date

# 加入父目錄以便 import
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_manager import DataManager

def validate_route_mapping():
    """驗證路線對應關係"""
    print("🔍 Step 1: 開始驗證路線對應關係...")
    
    # 初始化資料管理器
    data_manager = DataManager()
    
    # 載入資料
    print("\n📊 載入Master Data...")
    master_data = data_manager.load_master_data()
    
    print("📊 載入Transaction Data...")
    transaction_data = data_manager.load_transaction_data(
        start_date="2025-05-04", 
        end_date="2025-06-10",
        filter_valid_items=True
    )
    
    if 'historical_orders' not in transaction_data:
        print("❌ 找不到歷史訂單資料！")
        return
    
    if 'route_schedule_master' not in master_data:
        print("❌ 找不到路線時刻表資料！")
        return
    
    orders_df = transaction_data['historical_orders']
    route_schedule = master_data['route_schedule_master']
    
    print(f"\n📈 資料概況:")
    print(f"  歷史訂單: {len(orders_df):,} 筆")
    print(f"  路線時刻表: {len(route_schedule):,} 筆")
    
    # 檢查必要欄位
    required_order_columns = ['ROUTECD', 'PARTCUSTID']
    required_schedule_columns = ['ROUTECD', 'PARTCUSTID', 'DELIVERTM', 'ORDERENDTIME']
    
    missing_order_cols = [col for col in required_order_columns if col not in orders_df.columns]
    missing_schedule_cols = [col for col in required_schedule_columns if col not in route_schedule.columns]
    
    if missing_order_cols:
        print(f"❌ 訂單資料缺少欄位: {missing_order_cols}")
        return
    
    if missing_schedule_cols:
        print(f"❌ 路線時刻表缺少欄位: {missing_schedule_cols}")
        return
    
    # 開始驗證
    print("\n🎯 開始路線對應驗證...")
    
    # 建立路線時刻表的組合鍵
    route_schedule['route_key'] = route_schedule['ROUTECD'].astype(str) + '|' + route_schedule['PARTCUSTID'].astype(str)
    valid_route_keys = set(route_schedule['route_key'].unique())
    
    # 建立訂單的組合鍵
    orders_df['route_key'] = orders_df['ROUTECD'].astype(str) + '|' + orders_df['PARTCUSTID'].astype(str)
    order_route_keys = orders_df['route_key'].unique()
    
    print(f"📊 路線組合統計:")
    print(f"  路線時刻表中的路線組合: {len(valid_route_keys):,} 種")
    print(f"  訂單中的路線組合: {len(order_route_keys):,} 種")
    
    # 找出對應和不對應的路線
    matched_routes = []
    unmatched_routes = []
    
    for route_key in order_route_keys:
        if route_key in valid_route_keys:
            matched_routes.append(route_key)
        else:
            unmatched_routes.append(route_key)
    
    # 統計結果
    print(f"\n✅ 驗證結果:")
    print(f"  可對應的路線組合: {len(matched_routes):,} 種 ({len(matched_routes)/len(order_route_keys)*100:.1f}%)")
    print(f"  無法對應的路線組合: {len(unmatched_routes):,} 種 ({len(unmatched_routes)/len(order_route_keys)*100:.1f}%)")
    
    # 分析無法對應的訂單數量
    if unmatched_routes:
        unmatched_orders = orders_df[orders_df['route_key'].isin(unmatched_routes)]
        print(f"  無法對應的訂單數量: {len(unmatched_orders):,} 筆 ({len(unmatched_orders)/len(orders_df)*100:.1f}%)")
        
        # 詳細分析無法對應的原因
        print(f"\n🔍 無法對應的路線組合分析:")
        unmatched_analysis = unmatched_orders.groupby('route_key').size().sort_values(ascending=False)
        
        print(f"  前10個最多訂單的無法對應路線:")
        for route_key, count in unmatched_analysis.head(10).items():
            routecd, partcustid = route_key.split('|')
            print(f"    {routecd} + {partcustid}: {count:,} 筆訂單")
            
            # 檢查是否是ROUTECD或PARTCUSTID的問題
            routecd_exists = routecd in route_schedule['ROUTECD'].astype(str).values
            partcustid_exists = partcustid in route_schedule['PARTCUSTID'].astype(str).values
            
            if not routecd_exists:
                print(f"      → ROUTECD '{routecd}' 不存在於路線時刻表")
            if not partcustid_exists:
                print(f"      → PARTCUSTID '{partcustid}' 不存在於路線時刻表")
            if routecd_exists and partcustid_exists:
                print(f"      → 組合不存在（ROUTECD和PARTCUSTID都存在，但組合不存在）")
    
    # 分析可對應的路線分布
    if matched_routes:
        matched_orders = orders_df[orders_df['route_key'].isin(matched_routes)]
        print(f"\n✅ 可對應的路線分析:")
        
        # 按ROUTECD統計
        routecd_stats = matched_orders['ROUTECD'].value_counts()
        print(f"  ROUTECD分布（前10）:")
        for routecd, count in routecd_stats.head(10).items():
            print(f"    {routecd}: {count:,} 筆訂單")
        
        # 按PARTCUSTID統計  
        partcustid_stats = matched_orders['PARTCUSTID'].value_counts()
        print(f"  PARTCUSTID分布（前10）:")
        for partcustid, count in partcustid_stats.head(10).items():
            print(f"    {partcustid}: {count:,} 筆訂單")
    
    # 檢查副倉庫路線
    print(f"\n🏢 副倉庫路線檢查:")
    sub_warehouse_routes = ['SDTC', 'SDHN']
    
    for sub_route in sub_warehouse_routes:
        sub_orders = orders_df[orders_df['ROUTECD'] == sub_route]
        if len(sub_orders) > 0:
            print(f"  {sub_route}: {len(sub_orders):,} 筆訂單")
            
            # 檢查這些訂單是否都能對應
            sub_matched = sub_orders[sub_orders['route_key'].isin(valid_route_keys)]
            print(f"    可對應: {len(sub_matched):,} 筆 ({len(sub_matched)/len(sub_orders)*100:.1f}%)")
        else:
            print(f"  {sub_route}: 無訂單")
    
    # 檢查特殊組合（R15+SDTC, R16+SDHN）
    special_combinations = [
        ('R15', 'SDTC'),
        ('R16', 'SDHN')
    ]
    
    print(f"\n🔄 特殊組合檢查:")
    for routecd, partcustid in special_combinations:
        special_orders = orders_df[
            (orders_df['ROUTECD'] == routecd) & 
            (orders_df['PARTCUSTID'] == partcustid)
        ]
        if len(special_orders) > 0:
            route_key = f"{routecd}|{partcustid}"
            is_valid = route_key in valid_route_keys
            status = "✅ 可對應" if is_valid else "❌ 無法對應"
            print(f"  {routecd}+{partcustid}: {len(special_orders):,} 筆訂單 - {status}")
        else:
            print(f"  {routecd}+{partcustid}: 無訂單")
    
    # 輸出詳細報告
    print(f"\n📁 輸出詳細報告...")
    
    # 保存無法對應的路線詳情
    if unmatched_routes:
        unmatched_details = []
        for route_key in unmatched_routes:
            routecd, partcustid = route_key.split('|')
            order_count = len(orders_df[orders_df['route_key'] == route_key])
            
            unmatched_details.append({
                'ROUTECD': routecd,
                'PARTCUSTID': partcustid,
                'route_key': route_key,
                'order_count': order_count,
                'routecd_exists': routecd in route_schedule['ROUTECD'].astype(str).values,
                'partcustid_exists': partcustid in route_schedule['PARTCUSTID'].astype(str).values
            })
        
        unmatched_df = pd.DataFrame(unmatched_details)
        output_file = os.path.join(os.path.dirname(__file__), '..', 'output', 'route_validation_unmatched.csv')
        unmatched_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"  無法對應的路線詳情: {output_file}")
    
    # 保存可對應的路線詳情
    if matched_routes:
        matched_details = []
        for route_key in matched_routes:
            routecd, partcustid = route_key.split('|')
            order_count = len(orders_df[orders_df['route_key'] == route_key])
            
            # 從路線時刻表取得時間資訊
            schedule_info = route_schedule[route_schedule['route_key'] == route_key].iloc[0]
            
            matched_details.append({
                'ROUTECD': routecd,
                'PARTCUSTID': partcustid,
                'route_key': route_key,
                'order_count': order_count,
                'DELIVERTM': schedule_info['DELIVERTM'],
                'ORDERENDTIME': schedule_info.get('ORDERENDTIME', '')
            })
        
        matched_df = pd.DataFrame(matched_details)
        output_file = os.path.join(os.path.dirname(__file__), '..', 'output', 'route_validation_matched.csv')
        matched_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"  可對應的路線詳情: {output_file}")
    
    # 總結
    print(f"\n📋 Step 1 驗證總結:")
    print(f"  總路線組合: {len(order_route_keys):,} 種")
    print(f"  可對應: {len(matched_routes):,} 種 ({len(matched_routes)/len(order_route_keys)*100:.1f}%)")
    print(f"  無法對應: {len(unmatched_routes):,} 種 ({len(unmatched_routes)/len(order_route_keys)*100:.1f}%)")
    
    if len(matched_routes) / len(order_route_keys) >= 0.8:
        print("✅ 路線對應率良好 (≥80%)")
    else:
        print("⚠️ 路線對應率偏低 (<80%)，需要進一步檢查")
    
    return {
        'total_route_combinations': len(order_route_keys),
        'matched_routes': len(matched_routes),
        'unmatched_routes': len(unmatched_routes),
        'match_rate': len(matched_routes) / len(order_route_keys),
        'matched_orders': len(orders_df[orders_df['route_key'].isin(matched_routes)]) if matched_routes else 0,
        'unmatched_orders': len(orders_df[orders_df['route_key'].isin(unmatched_routes)]) if unmatched_routes else 0
    }

if __name__ == "__main__":
    try:
        result = validate_route_mapping()
        print(f"\n🎯 驗證完成！")
        
    except Exception as e:
        print(f"\n❌ 驗證過程發生錯誤: {str(e)}")
        import traceback
        traceback.print_exc()