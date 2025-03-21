import os

def process_file(file_path, output_lines):
    """處理單個檔案，提取並格式化數據，並處理卡號長度。"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            next(f)  # 跳過標題行
            for line in f:
                data = line.strip().split(',')
                # 提取需要的欄位
                date_str = data[5]
                time_str = data[6]
                card_number = data[4]

                # 格式化日期和時間
                year = date_str[6:10]
                month = date_str[0:2]
                day = date_str[3:5]
                formatted_time = time_str.replace(":", "")

                # 處理卡號長度，通用補零
                if len(card_number) < 10:
                    card_number = card_number.zfill(10) # 使用 zfill() 補零
                elif len(card_number) >10:       #如果長度大於10
                    print(f"警告：檔案 {file_path} 中發現異常卡號長度 ({len(card_number)})：{card_number}")
                    continue  #直接跳過


                # 建立輸出行
                output_line = (
                    "00" +
                    year + month + day +
                    formatted_time +
                    card_number
                )
                output_lines.append(output_line)
    except FileNotFoundError:
        print(f"檔案未找到: {file_path}")
        return
    except Exception as e:
        print(f"在 {file_path} 中發生錯誤: {str(e)}")
        return


def main():
    """主函數，讀取檔案列表、處理檔案並寫入輸出檔案。"""
    file_paths = [
       "/Users/wenbinyang/Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles/D43DB696294C275B6EDADD711B526B65/Caches/Files/2025-03/dcb02028f9ce13b5c24850eb5d22a534/032025 (2).txt",
        "/Users/wenbinyang/Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles/D43DB696294C275B6EDADD711B526B65/Caches/Files/2025-03/fd41738fb6051e559cb0e6a62b277a33/032025 (1).txt",
        "/Users/wenbinyang/Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles/D43DB696294C275B6EDADD711B526B65/Caches/Files/2025-03/9503f5215b3f16a33b599449281c6fca/032025.txt"

    ]  # 替換為你的檔案路徑
    output_file = "formatted_output.txt"  # 輸出檔案名
    output_lines = []  # 用於存儲所有格式化後的行

    # 處理每個檔案
    for file_path in file_paths:
        if os.path.exists(file_path):
            process_file(file_path, output_lines)
        else: 
            print("文件"+file_path+"不存在")

    # 將所有行寫入輸出檔案
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for line in output_lines:
            outfile.write(line + '\n')

    print(f"處理完成，結果已保存到 {output_file}")

if __name__ == "__main__":
    main()





##################

import pandas as pd
import os

def process_excel_file(file_path, output_lines):
    """處理單個 Excel 檔案，提取並格式化數據。"""
    try:
        # 讀取 Excel 檔案，跳過標題行（如果有的話）
        df = pd.read_excel(file_path, skiprows=1, header=None) # 從第二行開始讀,不要把第一行作為header

        for _, row in df.iterrows():
            try:
                # 提取需要的欄位，注意索引從 0 開始
                date_str = str(row[5])  # 日期
                time_str = str(row[6])  # 時間
                card_number = str(row[4])  # 用戶卡號

                # 格式化日期
                month = date_str[0:2]
                day = date_str[3:5]
                year = date_str[6:10]

                # 格式化時間
                formatted_time = time_str.replace(":", "")

                # 處理卡號長度，通用補零
                if len(card_number) < 10:
                    card_number = card_number.zfill(10)
                elif len(card_number)>10:
                     print(f"警告：檔案 {file_path} 中發現異常卡號長度 ({len(card_number)})：{card_number}")  #卡號長度不等於10時發出警告
                     continue

                # 建立輸出行
                output_line = "00" + year + month + day + formatted_time + card_number
                output_lines.append(output_line)

            except (IndexError, ValueError) as e:
                print(f"在檔案 {file_path} 的某一行中發生錯誤: {e}，可能資料格式不正確，已跳過該行。")
                continue

    except FileNotFoundError:
        print(f"檔案未找到: {file_path}")
    except Exception as e:
        print(f"處理檔案 {file_path} 時發生錯誤: {e}")


def main():
    """主函數，讀取檔案列表、處理檔案並寫入輸出檔案。"""
    folder_path = r"C:\Users\TW0002\Desktop\打卡"  # 檔案夾路徑
    output_file = "formatted_output.txt"  # 輸出檔案名
    output_lines = []  # 用於存儲所有格式化後的行

    # 遍歷資料夾中的所有 .xlsx 檔案
    for filename in os.listdir(folder_path):
        if filename.endswith(".xlsx"):
            file_path = os.path.join(folder_path, filename)
            process_excel_file(file_path, output_lines)

    # 將所有行寫入輸出檔案
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for line in output_lines:
            outfile.write(line + '\n')

    print(f"處理完成，結果已保存到 {output_file}")


if __name__ == "__main__":
    main()