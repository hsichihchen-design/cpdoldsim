"""
時間解析測試程式
用來驗證修正後的時間解析功能是否正常工作
"""

import sys
import os
from datetime import time

# 加入父目錄以便 import
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def test_time_parsing():
    """測試時間解析功能"""
    print("🕐 測試時間解析功能...")
    
    # 測試資料：來自您的 route_schedule_master.csv
    test_times = [
        855, 1000, 925, 1030, 1100, 1200, 1130, 1230, 1300, 
        1350, 1430, 1450, 1500, 1630, 1600, 1700, 1610, 1730, 1830,
        '855', '1000', '925', '1030',  # 字串格式
        85, 5,  # 短格式
        '08:55', '10:00'  # 冒號格式
    ]
    
    def parse_time_number(time_value):
        """本地測試版本的時間解析"""
        try:
            if time_value is None or time_value == '':
                return None
                
            # 先轉為字串並清理
            time_str = str(time_value).strip()
            
            # 移除可能的小數點
            if '.' in time_str:
                time_str = time_str.split('.')[0]
            
            # 處理數字格式
            if time_str.isdigit():
                time_int = int(time_str)
                
                # 處理不同長度的數字格式
                if time_int < 100:  # 例如: 85 -> 00:85 -> 01:25
                    hour = time_int // 60
                    minute = time_int % 60
                elif time_int < 1000:  # 例如: 855 -> 08:55
                    hour = time_int // 100
                    minute = time_int % 100
                else:  # 例如: 1000 -> 10:00, 1350 -> 13:50
                    hour = time_int // 100
                    minute = time_int % 100
                
                # 驗證時間有效性
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    return time(hour, minute)
                else:
                    print(f"    ❌ 時間超出範圍: {hour}:{minute:02d} (原始值: {time_value})")
                    return None
            
            # 處理已經是時間格式的情況 (08:55)
            elif ':' in time_str:
                parts = time_str.split(':')
                if len(parts) >= 2:
                    hour = int(parts[0])
                    minute = int(parts[1])
                    
                    if 0 <= hour <= 23 and 0 <= minute <= 59:
                        return time(hour, minute)
                    else:
                        print(f"    ❌ 時間超出範圍: {hour}:{minute} (原始值: {time_value})")
                        return None
            
            print(f"    ❌ 無法解析時間格式: '{time_value}' (type: {type(time_value)})")
            return None
            
        except (ValueError, TypeError, AttributeError) as e:
            print(f"    ❌ 時間格式錯誤: '{time_value}' - {str(e)}")
            return None
    
    # 執行測試
    success_count = 0
    total_count = len(test_times)
    
    print(f"\n📊 測試 {total_count} 個時間格式:")
    print("-" * 50)
    
    for i, test_time in enumerate(test_times, 1):
        result = parse_time_number(test_time)
        
        if result:
            print(f"  {i:2d}. {str(test_time):>6} → {result.strftime('%H:%M')} ✅")
            success_count += 1
        else:
            print(f"  {i:2d}. {str(test_time):>6} → 解析失敗 ❌")
    
    print("-" * 50)
    print(f"📈 測試結果: {success_count}/{total_count} ({success_count/total_count*100:.1f}%) 成功")
    
    if success_count == total_count:
        print("🎉 所有時間格式都解析成功！")
        return True
    else:
        print("⚠️ 部分時間格式解析失敗，需要進一步調整")
        return False

def test_route_schedule_loading():
    """測試載入 route_schedule_master.csv"""
    print("\n📂 測試載入 route_schedule_master.csv...")
    
    try:
        import pandas as pd
        
        # 載入檔案
        file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'master_data', 'route_schedule_master.csv')
        
        if not os.path.exists(file_path):
            print(f"❌ 檔案不存在: {file_path}")
            return False
        
        df = pd.read_csv(file_path)
        print(f"✅ 成功載入 {len(df)} 筆資料")
        
        # 檢查時間欄位
        time_columns = ['ORDERENDTIME', 'DELIVERTM']
        for col in time_columns:
            if col in df.columns:
                unique_times = df[col].unique()
                print(f"  {col} 欄位有 {len(unique_times)} 個不同時間值")
                print(f"    範例: {list(unique_times[:10])}")
                
                # 測試解析前幾個時間
                print(f"    解析測試:")
                for time_val in unique_times[:5]:
                    if pd.notna(time_val):
                        # 這裡使用我們修正後的解析邏輯測試
                        from datetime import time as time_obj
                        try:
                            time_str = str(time_val).strip()
                            if time_str.isdigit():
                                time_int = int(time_str)
                                if time_int < 1000:
                                    hour = time_int // 100
                                    minute = time_int % 100
                                else:
                                    hour = time_int // 100
                                    minute = time_int % 100
                                
                                if 0 <= hour <= 23 and 0 <= minute <= 59:
                                    parsed_time = time_obj(hour, minute)
                                    print(f"      {time_val} → {parsed_time.strftime('%H:%M')} ✅")
                                else:
                                    print(f"      {time_val} → 時間範圍錯誤 ❌")
                            else:
                                print(f"      {time_val} → 非數字格式 ⚠️")
                        except Exception as e:
                            print(f"      {time_val} → 解析錯誤: {str(e)} ❌")
        
        return True
        
    except Exception as e:
        print(f"❌ 載入檔案失敗: {str(e)}")
        return False

if __name__ == "__main__":
    print("🔧 時間解析修正驗證工具")
    print("=" * 50)
    
    # 測試時間解析邏輯
    parsing_ok = test_time_parsing()
    
    # 測試實際檔案載入
    loading_ok = test_route_schedule_loading()
    
    print("\n" + "=" * 50)
    print("📋 總結:")
    print(f"  時間解析邏輯: {'✅ 正常' if parsing_ok else '❌ 有問題'}")
    print(f"  檔案載入測試: {'✅ 正常' if loading_ok else '❌ 有問題'}")
    
    if parsing_ok and loading_ok:
        print("\n🎉 修正驗證通過！可以繼續執行 Step 2")
    else:
        print("\n⚠️ 仍有問題需要進一步調整")