"""
主程式 - 測試DataManager的資料過濾功能
"""

import logging
import pandas as pd
from src.modules.data_manager import DataManager

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """主函數"""
    print(" 啟動倉庫模擬系統...")
    
    # 初始化DataManager
    data_manager = DataManager()
    
    # 載入Master Data
    print("\n 載入Master Data...")
    master_data = data_manager.load_master_data()
    
    # 驗證資料一致性
    print("\n 驗證資料一致性...")
    validation_results = data_manager.validate_data_consistency()
    
    # 測試零件資料過濾功能
    print("\n 測試零件資料過濾功能...")
    
    # 取得有效零件摘要
    valid_items_summary = data_manager.get_valid_items_summary()
    print(f"有效零件摘要:")
    print(f"  總計: {valid_items_summary.get('total_valid_items', 0):,} 個零件")
    print(f"  樓層分布: {valid_items_summary.get('items_by_floor', {})}")
    print(f"  零件前碼種類: {valid_items_summary.get('unique_frcd_count', 0)} 種")
    print(f"  再包裝比例: {valid_items_summary.get('repack_ratio', 0):.2%}")
    
    print(f"\n 前10大零件前碼:")
    top_frcd = valid_items_summary.get('top_10_frcd', {})
    for frcd, count in top_frcd.items():
        print(f"  {frcd}: {count:,} 個零件")
    
    # 測試模擬資料過濾
    print(f"\n 模擬測試資料過濾功能...")
    
    # 建立測試用的模擬transaction data（混合有效和無效零件）
    # 先從有效零件中取一些樣本
    item_master = data_manager.master_data['item_master']
    valid_samples = item_master.sample(3)  # 取3個有效零件樣本
    
    test_transaction = pd.DataFrame({
        'frcd': list(valid_samples['frcd']) + ['INVALID', 'NOTEXIST'],
        'partno': list(valid_samples['partno']) + ['INVALID123', 'NOTEXIST456'],
        'quantity': [10, 5, 8, 3, 2],
        'date': ['2025-06-15'] * 5
    })
    
    print(f"測試資料 (包含有效和無效零件): {len(test_transaction)} 筆")
    print(test_transaction.to_string(index=False))
    
    # 執行過濾
    print(f"\n 執行零件過濾...")
    filtered_data = data_manager.filter_valid_items(test_transaction)
    print(f"\n過濾後資料: {len(filtered_data)} 筆")
    if len(filtered_data) > 0:
        print(filtered_data.to_string(index=False))
    else:
        print("沒有有效零件！")
    
    # 測試完整的transaction data載入流程
    print(f"\n 測試Transaction Data載入流程（模擬）...")
    
    # 由於還沒有實際的transaction data檔案，我們模擬這個過程
    print("注意：由於尚未準備實際的historical_orders.csv和historical_receiving.csv")
    print("此測試會顯示檔案不存在的警告，這是正常的。")
    
    # 嘗試載入transaction data（會失敗但展示流程）
    transaction_data = data_manager.load_transaction_data(
        start_date="2025-06-01", 
        end_date="2025-06-30",
        filter_valid_items=True
    )
    
    if transaction_data:
        print("Transaction Data載入成功！")
        for data_name, df in transaction_data.items():
            print(f"  - {data_name}: {len(df)} 筆資料")
    else:
        print("Transaction Data檔案尚未準備，這是預期的結果。")
    
    print("\n DataManager資料過濾功能測試完成!")
    print("\n 下一步：準備historical_orders.csv和historical_receiving.csv檔案")

if __name__ == "__main__":
    main()