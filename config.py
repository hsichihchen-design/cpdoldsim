"""
配置檔案 - 設定資料檔案路徑（修正版）
"""

from pathlib import Path
import os

# 方法1: 使用絕對路徑（根據你提供的路徑）
BASE_PATH = Path(r"C:\Users\HCCHEN\Downloads\AI練習\SIM\warehouse_simulation")

# 方法2: 動態偵測專案根目錄（推薦）
def find_project_root():
    """動態尋找專案根目錄"""
    current_path = Path(__file__).parent
    
    # 往上找到包含 'data' 資料夾的目錄
    while current_path != current_path.parent:
        if (current_path / 'data').exists():
            return current_path
        current_path = current_path.parent
    
    # 如果找不到，使用絕對路徑
    return BASE_PATH

# 使用動態偵測，如果失敗則使用絕對路徑
try:
    PROJECT_ROOT = find_project_root()
    print(f"📁 專案根目錄: {PROJECT_ROOT}")
except:
    PROJECT_ROOT = BASE_PATH
    print(f"📁 使用絕對路徑: {PROJECT_ROOT}")

# 資料檔案路徑設定
DATA_ROOT = PROJECT_ROOT / 'data'
MASTER_DATA_ROOT = DATA_ROOT / 'master_data'
TRANSACTION_DATA_ROOT = DATA_ROOT / 'transaction_data'

# Master Data 檔案路徑
MASTER_DATA_FILES = {
    'system_parameters': MASTER_DATA_ROOT / 'system_parameters.csv',
    'item_master': MASTER_DATA_ROOT / 'item_master.csv',
    'staff_skill_master': MASTER_DATA_ROOT / 'staff_skill_master.csv',
    'workstation_capacity': MASTER_DATA_ROOT / 'workstation_capacity.csv',
    'route_schedule_master': MASTER_DATA_ROOT / 'route_schedule_master.csv',
    'item_inventory': MASTER_DATA_ROOT / 'item_inventory.csv',
    'branch_route_master': MASTER_DATA_ROOT / 'branch_route_master.csv'
}

# Transaction Data 檔案路徑
TRANSACTION_DATA_FILES = {
    'historical_orders': TRANSACTION_DATA_ROOT / 'historical_orders.csv',
    'historical_receiving': TRANSACTION_DATA_ROOT / 'historical_receiving.csv'
}

# 輸出路徑
OUTPUT_ROOT = PROJECT_ROOT / 'output'
REPORTS_ROOT = OUTPUT_ROOT / 'reports'
LOGS_ROOT = OUTPUT_ROOT / 'logs'

# 確保輸出資料夾存在
OUTPUT_ROOT.mkdir(exist_ok=True)
REPORTS_ROOT.mkdir(exist_ok=True)
LOGS_ROOT.mkdir(exist_ok=True)

# 除錯：顯示路徑設定
if __name__ == "__main__":
    print("🔍 路徑設定除錯:")
    print(f"PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"DATA_ROOT: {DATA_ROOT}")
    print(f"MASTER_DATA_ROOT: {MASTER_DATA_ROOT}")
    
    print("\n📁 檢查資料夾是否存在:")
    print(f"data/ 存在: {DATA_ROOT.exists()}")
    print(f"master_data/ 存在: {MASTER_DATA_ROOT.exists()}")
    print(f"transaction_data/ 存在: {TRANSACTION_DATA_ROOT.exists()}")
    
    print("\n📄 檢查檔案是否存在:")
    for name, path in MASTER_DATA_FILES.items():
        print(f"{name}: {path.exists()} - {path}")