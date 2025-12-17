import os
import glob
import pandas as pd
import gspread
import numpy as np
from datetime import datetime, timedelta
import re

# Settings
SPREADSHEET_ID = "1swzp4-ISlM769K_dKR1BpdwQSkV19YmhHZuzwaXQCdI"
SHEET_NAME_DEBUG = "デバッグ"
API_KEY_FILE = "key.json"

def find_today_delivery_csvs(folder_path):
    """指定されたフォルダ内で今日ダウンロードしたdelivery_listから始まるCSVファイルを全て取得します。"""
    from datetime import datetime
    
    # 今日の日付を取得
    today = datetime.now().date()
    
    # delivery_listから始まるCSVファイルを検索
    delivery_files = glob.glob(os.path.join(folder_path, 'delivery_list*.csv'))
    
    if not delivery_files:
        raise FileNotFoundError(f"'{folder_path}'内にdelivery_listから始まるCSVファイルが見つかりません。")
    
    # 今日作成されたファイルのみをフィルタリング
    today_files = []
    for file_path in delivery_files:
        file_creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
        if file_creation_time.date() == today:
            today_files.append(file_path)
    
    if not today_files:
        raise FileNotFoundError(f"今日ダウンロードしたdelivery_listから始まるCSVファイルが見つかりません。")
    
    print(f"今日ダウンロードしたdelivery_listファイル: {len(today_files)}件")
    return today_files




def debug_to_spreadsheet(df):
    """
    統合されたCSVデータをスプレッドシートに書き込みます。
    """
    try:
        gc = gspread.service_account(filename=API_KEY_FILE)
        sh = gc.open_by_key(SPREADSHEET_ID)
        worksheet = sh.worksheet(SHEET_NAME_DEBUG)

        # 2行目から下をクリア（1行目は残す）
        worksheet.batch_clear(['A2:Z1000'])
        
        # NaN値を空文字列で埋めてエラーを回避
        df_for_write = df.fillna('')
        
        # データフレームをリストのリストに変換して書き込み用に準備
        data_to_write = df_for_write.values.tolist()
        
        # A2セルから全データを書き込み（ヘッダー行含む）
        worksheet.update('A2', data_to_write)
        
        print(f"\nCSVデータのスプレッドシートへの書き込みが完了しました。（{len(data_to_write)}行）")

    except gspread.exceptions.GSpreadException as e:
        print(f"スプレッドシートAPIのエラー: {e}")
    except FileNotFoundError as e:
        print(f"エラー: {e}")
    except Exception as e:
        print(f"エラーが発生しました: {e}")

# Main execution block
downloads_folder = "/Users/nj-cmd11/Downloads"

try:
    # 今日ダウンロードしたdelivery_listから始まるCSVファイルを特定
    today_csv_files = find_today_delivery_csvs(downloads_folder)
    print(f"今日ダウンロードしたdelivery_listファイル: {[os.path.basename(f) for f in today_csv_files]}")

    # 複数のCSVファイルを読み込んで統合
    dataframes = []
    for csv_file in today_csv_files:
        try:
            df_temp = pd.read_csv(csv_file, encoding='cp932')
            dataframes.append(df_temp)
            print(f"CSVファイル「{os.path.basename(csv_file)}」をCP932で読み込みました。（{len(df_temp)}行）")
        except Exception as e:
            print(f"CSVファイル「{os.path.basename(csv_file)}」の読み込みでエラーが発生しました: {e}")
            continue
    
    if not dataframes:
        raise FileNotFoundError("有効なCSVファイルが読み込めませんでした。")
    
    # データフレームを結合
    df_combined = pd.concat(dataframes, ignore_index=True)
    print(f"合計{len(dataframes)}件のCSVファイルを結合しました。（{len(df_combined)}行）")
    
    # 指定された列のみを抽出（A,D,Q,R,S,U,W,AA,AG列）
    # 列インデックス: 
    # A配送管理ID=0, 
    # D寄附者=3, 
    # Iお届け先名=8, 
    # J届け先名称カナ=9, 
    # K届け先郵便番号=10, 
    # L届け先都道府県=11, 
    # Q配送ステータス=16, 
    # S返礼品=18, 
    # U事業者名称=20, 
    # W出荷予定日=22, 
    # AA備考=26, 
    # AG申込日=32, 
    # AH出荷日=33, 
    # AJ商品コード=35
    selected_columns = [0, 3, 8, 16, 18, 22, 32, 33, 35]
    # selected_columns = [0, 3, 8, 16, 18, 22, 26, 33, 35]
    df_filtered = df_combined.iloc[:, selected_columns]
    
    print(f"列を絞り込みました。抽出列数: {len(df_filtered.columns)}列")
    
    # 統合されたデータをスプレッドシートに書き込み
    debug_to_spreadsheet(df_filtered)
    
except FileNotFoundError as e:
    print(f"エラー: {e}")
except KeyError as e:
    print(f"エラー: CSVファイルに指定された列が見つかりません: {e}")
except Exception as e:
    print(f"エラーが発生しました: {e}")