import os
import glob
import pandas as pd
import gspread
import time
from datetime import datetime

# 設定情報
SPREADSHEET_ID = "1swzp4-ISlM769K_dKR1BpdwQSkV19YmhHZuzwaXQCdI"
SHEET_NAME = "寄附受付集計"
API_KEY_FILE = "key.json"

# リトライ設定
MAX_RETRIES = 3  # 最大リトライ回数
INITIAL_RETRY_DELAY = 2  # 初回リトライ待機時間（秒）
MAX_RETRY_DELAY = 30  # 最大リトライ待機時間（秒）

def translate_error(error_str):
    """エラーメッセージを日本語に翻訳します。"""
    error_str_lower = str(error_str).lower()
    
    # よくあるエラーパターンを日本語に翻訳
    translations = {
        "remote end closed": "リモート接続が切断されました",
        "remote disconnected": "リモート接続が切断されました",
        "connection aborted": "接続が中断されました",
        "timeout": "タイムアウトが発生しました",
        "rate limit": "リクエスト制限に達しました",
        "permission denied": "アクセス権限がありません",
        "not found": "リソースが見つかりません",
        "invalid": "無効なリクエストです",
        "authentication": "認証に失敗しました",
        "service unavailable": "サービスが利用できません"
    }
    
    # エラーメッセージを日本語に置き換え
    translated = str(error_str)
    for eng, jpn in translations.items():
        if eng in error_str_lower:
            translated = translated.replace(eng, jpn)
    
    return translated

def is_retryable_error(error):
    """リトライ可能なエラーかどうかを判定します。"""
    error_str = str(error).lower()
    error_type = type(error).__name__
    
    # リトライ可能なエラーパターン
    retryable_patterns = [
        "remote end closed",
        "remote disconnected",
        "connection aborted",
        "timeout",
        "service unavailable",
        "connection reset",
        "broken pipe",
        "network is unreachable"
    ]
    
    # gspreadの特定の例外タイプ
    retryable_exceptions = [
        "APIError",
        "SpreadsheetNotFound",
        "WorksheetNotFound"
    ]
    
    # パターンマッチング
    for pattern in retryable_patterns:
        if pattern in error_str:
            return True
    
    # 例外タイプのチェック
    for exc_type in retryable_exceptions:
        if exc_type in error_type:
            return True
    
    # gspread.exceptions.GSpreadExceptionの一部はリトライ可能
    if isinstance(error, gspread.exceptions.GSpreadException):
        # 認証エラーなどはリトライ不可
        if "permission" in error_str or "authentication" in error_str:
            return False
        return True
    
    return False

def retry_with_backoff(func, *args, **kwargs):
    """
    指数バックオフを使用して関数をリトライします。
    
    Args:
        func: 実行する関数
        *args: 関数の位置引数
        **kwargs: 関数のキーワード引数
    
    Returns:
        関数の戻り値
    
    Raises:
        最後の試行で発生した例外
    """
    last_exception = None
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            
            # リトライ可能なエラーでない場合は即座に例外を発生
            if not is_retryable_error(e):
                raise
            
            # 最後の試行の場合は例外を発生
            if attempt >= MAX_RETRIES:
                error_msg_jp = translate_error(str(e))
                print(f"リトライが{MAX_RETRIES}回失敗しました。最後のエラー: {error_msg_jp}")
                raise
            
            # 指数バックオフで待機時間を計算
            delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
            error_msg_jp = translate_error(str(e))
            print(f"エラーが発生しました（試行 {attempt + 1}/{MAX_RETRIES + 1}）: {error_msg_jp}")
            print(f"{delay}秒後にリトライします...")
            time.sleep(delay)
    
    # ここには到達しないはずですが、念のため
    raise last_exception

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

def get_product_category(product_name):
    """商品名からカテゴリを分類します。"""
    if "ペットボトル" in product_name:
        return "ペットボトル"
    if "玄米" in product_name:
        return "玄米"
    if "無洗" in product_name:
        return "無洗米"
    if "白米" in product_name:
        return "白米"
    return "その他"

def get_product_type(product_name):
    """商品名から単品か定期便かを分類します。"""
    return "定期便" if "定期" in product_name else "単品"

def get_product_quantity(product_name, category):
    """商品名から数量を抽出します。"""
    # 数量の検索順序を長い文字列から短い文字列に変更
    if category == "ペットボトル":
        for num in ["6本", "5本", "4本", "3本", "2本", "1本"]:
            if num in product_name:
                return int(num.replace("本", ""))
    else:
        for num in ["30kg", "25kg", "20kg", "15kg", "10kg", "5kg"]:
            if num in product_name:
                return int(num.replace("kg", ""))
    return 0

def get_product_count():
    """CSVの1行を1件として件数を返します。"""
    return 1

def get_material_category(category, quantity):
    """カテゴリと数量から資材カテゴリを判定します。"""
    if category == "ペットボトル":
        if quantity in [1, 2]:
            return "PB2本"
        elif quantity in [3, 4]:
            return "PB4本"
        elif quantity in [5, 6]:
            return "PB6本"
    elif category in ["玄米", "無洗米", "白米"]:
        if quantity == 5:
            return "5kg箱"
        elif quantity == 10:
            return "10kg箱"
        elif quantity in [15, 20]:
            return "20kg箱"
        elif quantity in [25, 30]:
            return "30kg箱"
    return None

def get_month(date_str):
    """日付文字列から年月を抽出します。2025年9月以降のすべての月に対応。"""
    if pd.isna(date_str):
        return None
    try:
        date_obj = pd.to_datetime(date_str)
        year = date_obj.year
        month = date_obj.month
        
        # 2025年9月以降のすべての月に対応
        if year == 2025 and month >= 9:
            return f"2025年{month}月"
        elif year >= 2026:
            return f"{year}年{month}月"
        else:
            return None
    except:
        return None

def get_month_with_fallback(scheduled_date, shipped_date):
    """出荷予定日から年月を抽出し、空の場合は出荷日を参照、どちらも空の場合は2025年10月とする。"""
    # まず出荷予定日を試す
    month = get_month(scheduled_date)
    if month is not None:
        return month
    
    # 出荷予定日が空の場合は出荷日を試す
    month = get_month(shipped_date)
    if month is not None:
        return month
    
    # どちらも空の場合は2025年10月とする
    return "2025年10月"

def get_date_group(scheduled_date, shipped_date):
    """出荷予定日（なければ出荷日）から日付グループを判定します。"""
    # まず出荷予定日から日を抽出
    if not pd.isna(scheduled_date):
        try:
            date_obj = pd.to_datetime(scheduled_date)
            day = date_obj.day
            if 1 <= day <= 7:
                return "2日グループ"
            elif 8 <= day <= 10:
                return "10日グループ"
            elif 11 <= day <= 17:
                return "17日グループ"
            elif 18 <= day <= 31:
                return "24日グループ"
        except:
            pass
    
    # 出荷予定日が空の場合は出荷日から日を抽出
    if not pd.isna(shipped_date):
        try:
            date_obj = pd.to_datetime(shipped_date)
            day = date_obj.day
            if 1 <= day <= 7:
                return "2日グループ"
            elif 8 <= day <= 10:
                return "10日グループ"
            elif 11 <= day <= 17:
                return "17日グループ"
            elif 18 <= day <= 31:
                return "24日グループ"
        except:
            pass
    
    # どちらも空の場合はNone（集計対象外）
    return None

def get_delivery_status(delivery_status_str):
    """配送ステータスから出荷状況を判定します。"""
    if pd.isna(delivery_status_str):
        return "不明"
    
    # 除外対象の配送ステータス
    excluded_statuses = ["配送キャンセル", "返送", "配送対象外"]
    if delivery_status_str in excluded_statuses:
        return "集計除外"
    
    # 「出荷依頼準備中」または「出荷準備中」の場合は未出荷、それ以外は出荷済み
    if delivery_status_str in ["出荷依頼準備中", "出荷準備中"]:
        return "まだ過ぎてない"  # 未出荷（元の「まだ過ぎてない」カテゴリ）
    else:
        return "すでに過ぎた"    # 出荷済み（元の「すでに過ぎた」カテゴリ）

def update_material_consumption_spreadsheet(df):
    """
    資材消費管理シートに集計データを書き込みます。
    """
    try:
        # 認証情報（リトライ付き）
        def get_worksheet():
            gc = gspread.service_account(filename=API_KEY_FILE)
            sh = gc.open_by_key(SPREADSHEET_ID)
            return sh.worksheet("資材消費管理")
        
        material_worksheet = retry_with_backoff(get_worksheet)
        
        # 資材カテゴリ列を追加
        df['資材カテゴリ'] = df.apply(lambda row: get_material_category(row['カテゴリ'], row['数量']), axis=1)
        
        # 資材カテゴリがNoneのデータを除外
        material_df = df[df['資材カテゴリ'].notna()]
        
        # 2025年11月以降のデータのみを対象
        def is_target_month(month_str):
            if pd.isna(month_str):
                return False
            try:
                year, month = month_str.replace('月', '').split('年')
                year = int(year)
                month = int(month)
                # 2025年11月以降
                if year == 2025 and month >= 11:
                    return True
                elif year >= 2026:
                    return True
                return False
            except:
                return False
        
        material_df = material_df[material_df['月'].apply(is_target_month)]
        target_df = df[df['月'].apply(is_target_month)]
        
        # 月別・資材カテゴリ別に件数を集計
        material_summary = material_df.groupby(['月', '資材カテゴリ'])['件数'].sum().reset_index()
        
        # 追加で求める指標の集計
        rice_white_summary = (
            target_df[target_df['カテゴリ'].isin(['玄米', '白米'])]
            .groupby('月')['数量']
            .sum() / 5
        )
        musen_summary = (
            target_df[target_df['カテゴリ'] == '無洗米']
            .groupby('月')['数量']
            .sum() / 5
        )
        pb_small_summary = (
            target_df[
                (target_df['カテゴリ'] == 'ペットボトル') &
                (target_df['数量'].isin([1, 3, 5]))
            ]
            .groupby('月')['件数']
            .sum()
        )
        
        rice_white_summary = rice_white_summary.to_dict()
        musen_summary = musen_summary.to_dict()
        pb_small_summary = pb_small_summary.to_dict()
        
        # スペーサー集計（PB1本、PB3本、PB5本の件数）
        spacer_summary = (
            target_df[
                (target_df['カテゴリ'] == 'ペットボトル') &
                (target_df['数量'].isin([1, 3, 5]))
            ]
            .groupby('月')['件数']
            .sum()
        )
        spacer_summary = spacer_summary.to_dict()
        
        # データから実際に存在する月を動的に取得し、時系列順にソート
        all_months = set(material_summary['月'].dropna().unique())
        all_months.update(rice_white_summary.keys())
        all_months.update(musen_summary.keys())
        all_months.update(pb_small_summary.keys())
        all_months.update(spacer_summary.keys())
        
        # 月を時系列順にソート
        def sort_key(month_str):
            if pd.isna(month_str):
                return (9999, 13)  # Noneは最後に
            year, month = month_str.replace('月', '').split('年')
            return (int(year), int(month))
        
        months_ordered = sorted([m for m in all_months if not pd.isna(m)], key=sort_key)
        print(f"資材消費管理シート - 検出された月: {months_ordered}")
        
        # 資材カテゴリと行のマッピング
        material_row_map = {
            "5kg箱": 41,
            "10kg箱": 42,
            "20kg箱": 43,
            "30kg箱": 44,
            "PB2本": 45,
            "PB4本": 46,
            "PB6本": 47
        }
        
        # 書き込み用データを格納する配列を初期化
        data_to_write = []
        
        if not months_ordered:
            print("資材消費管理シートに書き込む対象月がありませんでした。")
            return
        
        # 各月の処理（2025年11月がC列(3)から開始）
        for i, month in enumerate(months_ordered):
            col = 3 + i  # C列(3)から開始、1列ずつ増加
            
            # 各資材カテゴリの処理
            for material_category, row in material_row_map.items():
                # 件数の集計
                count = material_summary.loc[
                    (material_summary['月'] == month) &
                    (material_summary['資材カテゴリ'] == material_category),
                    '件数'
                ].sum()
                
                # int64をintに変換し、0の場合は空にする
                count = int(count) if count > 0 else ''
                
                # 書き込み
                data_to_write.append({
                    'range': gspread.utils.rowcol_to_a1(row, col),
                    'values': [[count]]
                })
            
            # 追加行の計算と書き込み（各月のC列を基準）
            rice_white_value = rice_white_summary.get(month, 0)
            rice_white_value = int(rice_white_value) if rice_white_value > 0 else ''
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(36, col),
                'values': [[rice_white_value]]
            })
            
            musen_value = musen_summary.get(month, 0)
            musen_value = int(musen_value) if musen_value > 0 else ''
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(37, col),
                'values': [[musen_value]]
            })
            
            pb_small_value = pb_small_summary.get(month, 0)
            pb_small_value = int(pb_small_value) if pb_small_value > 0 else ''
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(16, col),
                'values': [[pb_small_value]]
            })
            
            # スペーサーの集計（PB1本、PB3本、PB5本の件数）
            spacer_value = spacer_summary.get(month, 0)
            spacer_value = int(spacer_value) if spacer_value > 0 else ''
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(48, col),
                'values': [[spacer_value]]
            })
        
        # gspreadの`batch_update`を使って効率的に書き込み（リトライ付き）
        if data_to_write:
            retry_with_backoff(material_worksheet.batch_update, data_to_write)
            print("資材消費管理シートの更新が完了しました。")
        else:
            print("資材消費管理シートに書き込むデータがありませんでした。")
    
    except gspread.exceptions.GSpreadException as e:
        error_msg_jp = translate_error(str(e))
        print(f"資材消費管理シートAPIのエラー: {error_msg_jp}")
        print(f"詳細: {e}")
    except Exception as e:
        error_msg_jp = translate_error(str(e))
        print(f"資材消費管理シートの更新でエラーが発生しました: {error_msg_jp}")
        print(f"詳細: {e}")

def update_schedule_spreadsheet(schedule_summary_quantity_df, schedule_summary_count_df):
    """
    出荷スケジュールシートに集計データを書き込みます。
    """
    try:
        # 認証情報（リトライ付き）
        def get_worksheet():
            gc = gspread.service_account(filename=API_KEY_FILE)
            sh = gc.open_by_key(SPREADSHEET_ID)
            return sh.worksheet("出荷スケジュール")
        
        schedule_worksheet = retry_with_backoff(get_worksheet)
        
        # データから実際に存在する月を動的に取得し、時系列順にソート
        all_months = set()
        for df in [schedule_summary_quantity_df, schedule_summary_count_df]:
            if not df.empty:
                all_months.update(df['月'].dropna().unique())
        
        # 月を時系列順にソート
        def sort_key(month_str):
            if pd.isna(month_str):
                return (9999, 13)  # Noneは最後に
            year, month = month_str.replace('月', '').split('年')
            return (int(year), int(month))
        
        months_ordered = sorted([m for m in all_months if not pd.isna(m)], key=sort_key)
        print(f"出荷スケジュールシート - 検出された月: {months_ordered}")
        
        # カテゴリと列のマッピング（C列=無洗米、D列=白米、E列=玄米、F列=ペットボトル）
        category_col_map = {
            "無洗米": 3,  # C列
            "白米": 4,    # D列
            "玄米": 5,    # E列
            "ペットボトル": 6  # F列
        }
        
        # 日付グループと行のマッピング
        date_group_row_map = {
            "2日グループ": {"count": 3, "quantity": 4},
            "10日グループ": {"count": 5, "quantity": 6},
            "17日グループ": {"count": 7, "quantity": 8},
            "24日グループ": {"count": 9, "quantity": 10}
        }
        
        # 書き込み用データを格納する配列を初期化
        data_to_write = []

        # 各月の処理
        for i, month in enumerate(months_ordered):
            # 各月の開始列を計算（2025年9月がC列(3)から開始、7列間隔）
            # 2025年9月: C列(3), 2025年10月: J列(10), 2025年11月: Q列(17)...
            start_col = 3 + i * 7
            
            # 各カテゴリの処理
            for category, col_offset in category_col_map.items():
                col = start_col + col_offset - 3  # C列基準でオフセット調整
                
                # 各日付グループの処理
                for date_group, rows in date_group_row_map.items():
                    # 件数の集計
                    count = schedule_summary_count_df.loc[
                        (schedule_summary_count_df['月'] == month) &
                        (schedule_summary_count_df['カテゴリ'] == category) &
                        (schedule_summary_count_df['日付グループ'] == date_group),
                        '件数'
                    ].sum()
                    
                    # 重量の集計
                    quantity = schedule_summary_quantity_df.loc[
                        (schedule_summary_quantity_df['月'] == month) &
                        (schedule_summary_quantity_df['カテゴリ'] == category) &
                        (schedule_summary_quantity_df['日付グループ'] == date_group),
                        '数量'
                    ].sum()
                    
                    # ペットボトルの場合は重量に2をかける（1本2kg）
                    if category == "ペットボトル":
                        quantity = quantity * 2
                    
                    # int64をintに変換し、0の場合は空にする
                    count = int(count) if count > 0 else ''
                    quantity = int(quantity) if quantity > 0 else ''
                    
                    # 件数の書き込み
                    data_to_write.append({
                        'range': gspread.utils.rowcol_to_a1(rows['count'], col),
                        'values': [[count]]
                    })
                    
                    # 重量の書き込み
                    data_to_write.append({
                        'range': gspread.utils.rowcol_to_a1(rows['quantity'], col),
                        'values': [[quantity]]
                    })
        
        # gspreadの`batch_update`を使って効率的に書き込み（リトライ付き）
        if data_to_write:
            retry_with_backoff(schedule_worksheet.batch_update, data_to_write)
            print("出荷スケジュールシートの更新が完了しました。")
        else:
            print("出荷スケジュールシートに書き込むデータがありませんでした。")
    
    except gspread.exceptions.GSpreadException as e:
        error_msg_jp = translate_error(str(e))
        print(f"出荷スケジュールシートAPIのエラー: {error_msg_jp}")
        print(f"詳細: {e}")
    except Exception as e:
        error_msg_jp = translate_error(str(e))
        print(f"出荷スケジュールシートの更新でエラーが発生しました: {error_msg_jp}")
        print(f"詳細: {e}")

def update_spreadsheet(summary_quantity_df, summary_count_df, not_expired_summary_quantity_df, not_expired_summary_count_df):
    """
    集計データをスプレッドシートに書き込みます。
    """
    try:
        # 認証情報（リトライ付き）
        def get_worksheet():
            gc = gspread.service_account(filename=API_KEY_FILE)
            sh = gc.open_by_key(SPREADSHEET_ID)
            return sh.worksheet(SHEET_NAME)
        
        worksheet = retry_with_backoff(get_worksheet)

        # データから実際に存在する月を動的に取得し、時系列順にソート
        all_months = set()
        for df in [summary_quantity_df, summary_count_df, not_expired_summary_quantity_df, not_expired_summary_count_df]:
            if not df.empty:
                all_months.update(df['月'].dropna().unique())
        
        # 月を時系列順にソート
        def sort_key(month_str):
            if pd.isna(month_str):
                return (9999, 13)  # Noneは最後に
            # "2025年9月" -> "2025年9" -> ["2025", "9"]
            year, month = month_str.replace('月', '').split('年')
            return (int(year), int(month))
        
        months_ordered = sorted([m for m in all_months if not pd.isna(m)], key=sort_key)
        print(f"検出された月: {months_ordered}")
        print(f"月の数: {len(months_ordered)}")
        
        # 仕様メモに合わせた行マップ
        row_map = {
            "玄米": {"quantity_all": 7, "quantity_not_expired": 8, "count_all": 9, "count_not_expired": 10},
            "白米": {"quantity_all": 11, "quantity_not_expired": 12, "count_all": 13, "count_not_expired": 14},
            "無洗米": {"quantity_all": 15, "quantity_not_expired": 16, "count_all": 17, "count_not_expired": 18},
            "ペットボトル": {"quantity_all": 32, "quantity_not_expired": 33, "count_all": 34, "count_not_expired": 35},
        }
        
        # 書き込み用データを格納する配列を初期化
        data_to_write = []
        
        # 各カテゴリの処理
        for category, rows in row_map.items():
            # 累計計算用の変数（全ての商品）
            teiki_quantity_total = 0
            tanpin_quantity_total = 0
            teiki_count_total = 0
            tanpin_count_total = 0
            
            # 累計計算用の変数（未出荷商品）
            teiki_quantity_not_expired_total = 0
            tanpin_quantity_not_expired_total = 0
            teiki_count_not_expired_total = 0
            tanpin_count_not_expired_total = 0
            
            # 月別データの書き込み
            for i, month in enumerate(months_ordered):
                # 数量データ（全ての商品）
                teiki_quantity = summary_quantity_df.loc[
                    (summary_quantity_df['月'] == month) &
                    (summary_quantity_df['カテゴリ'] == category) &
                    (summary_quantity_df['タイプ'] == '定期便'),
                    '数量'
                ].sum()
                
                tanpin_quantity = summary_quantity_df.loc[
                    (summary_quantity_df['月'] == month) &
                    (summary_quantity_df['カテゴリ'] == category) &
                    (summary_quantity_df['タイプ'] == '単品'),
                    '数量'
                ].sum()
                
                # 件数データ（全ての商品）
                teiki_count = summary_count_df.loc[
                    (summary_count_df['月'] == month) &
                    (summary_count_df['カテゴリ'] == category) &
                    (summary_count_df['タイプ'] == '定期便'),
                    '件数'
                ].sum()
                
                tanpin_count = summary_count_df.loc[
                    (summary_count_df['月'] == month) &
                    (summary_count_df['カテゴリ'] == category) &
                    (summary_count_df['タイプ'] == '単品'),
                    '件数'
                ].sum()
                
                # 未出荷商品の数量データ
                teiki_quantity_not_expired = not_expired_summary_quantity_df.loc[
                    (not_expired_summary_quantity_df['月'] == month) &
                    (not_expired_summary_quantity_df['カテゴリ'] == category) &
                    (not_expired_summary_quantity_df['タイプ'] == '定期便'),
                    '数量'
                ].sum()
                
                tanpin_quantity_not_expired = not_expired_summary_quantity_df.loc[
                    (not_expired_summary_quantity_df['月'] == month) &
                    (not_expired_summary_quantity_df['カテゴリ'] == category) &
                    (not_expired_summary_quantity_df['タイプ'] == '単品'),
                    '数量'
                ].sum()
                
                # 未出荷商品の件数データ
                teiki_count_not_expired = not_expired_summary_count_df.loc[
                    (not_expired_summary_count_df['月'] == month) &
                    (not_expired_summary_count_df['カテゴリ'] == category) &
                    (not_expired_summary_count_df['タイプ'] == '定期便'),
                    '件数'
                ].sum()
                
                tanpin_count_not_expired = not_expired_summary_count_df.loc[
                    (not_expired_summary_count_df['月'] == month) &
                    (not_expired_summary_count_df['カテゴリ'] == category) &
                    (not_expired_summary_count_df['タイプ'] == '単品'),
                    '件数'
                ].sum()

                # int64をintに変換し、0の場合は空にする
                teiki_quantity = int(teiki_quantity) if teiki_quantity > 0 else ''
                tanpin_quantity = int(tanpin_quantity) if tanpin_quantity > 0 else ''
                teiki_count = int(teiki_count) if teiki_count > 0 else ''
                tanpin_count = int(tanpin_count) if tanpin_count > 0 else ''
                teiki_quantity_not_expired = int(teiki_quantity_not_expired) if teiki_quantity_not_expired > 0 else ''
                tanpin_quantity_not_expired = int(tanpin_quantity_not_expired) if tanpin_quantity_not_expired > 0 else ''
                teiki_count_not_expired = int(teiki_count_not_expired) if teiki_count_not_expired > 0 else ''
                tanpin_count_not_expired = int(tanpin_count_not_expired) if tanpin_count_not_expired > 0 else ''

                # 累計に加算（数値の場合のみ）
                if isinstance(teiki_quantity, int):
                    teiki_quantity_total += teiki_quantity
                if isinstance(tanpin_quantity, int):
                    tanpin_quantity_total += tanpin_quantity
                if isinstance(teiki_count, int):
                    teiki_count_total += teiki_count
                if isinstance(tanpin_count, int):
                    tanpin_count_total += tanpin_count
                
                # 未出荷商品の累計に加算（数値の場合のみ）
                if isinstance(teiki_quantity_not_expired, int):
                    teiki_quantity_not_expired_total += teiki_quantity_not_expired
                if isinstance(tanpin_quantity_not_expired, int):
                    tanpin_quantity_not_expired_total += tanpin_quantity_not_expired
                if isinstance(teiki_count_not_expired, int):
                    teiki_count_not_expired_total += teiki_count_not_expired
                if isinstance(tanpin_count_not_expired, int):
                    tanpin_count_not_expired_total += tanpin_count_not_expired

                # 列番号を計算（D列(4)から開始）
                col_teiki = 4 + i * 2  # D列(4)から開始
                col_tanpin = 5 + i * 2  # E列(5)から開始

                # 書き込みリクエストを作成
                # 全ての商品(kg/本)
                data_to_write.append({
                    'range': gspread.utils.rowcol_to_a1(rows['quantity_all'], col_teiki),
                    'values': [[teiki_quantity]]
                })
                data_to_write.append({
                    'range': gspread.utils.rowcol_to_a1(rows['quantity_all'], col_tanpin),
                    'values': [[tanpin_quantity]]
                })
                
                # 未出荷商品(kg/本) - 玄米、白米、無洗米、ペットボトル
                data_to_write.append({
                    'range': gspread.utils.rowcol_to_a1(rows['quantity_not_expired'], col_teiki),
                    'values': [[teiki_quantity_not_expired]]
                })
                data_to_write.append({
                    'range': gspread.utils.rowcol_to_a1(rows['quantity_not_expired'], col_tanpin),
                    'values': [[tanpin_quantity_not_expired]]
                })
                
                # 全ての商品(件)
                data_to_write.append({
                    'range': gspread.utils.rowcol_to_a1(rows['count_all'], col_teiki),
                    'values': [[teiki_count]]
                })
                data_to_write.append({
                    'range': gspread.utils.rowcol_to_a1(rows['count_all'], col_tanpin),
                    'values': [[tanpin_count]]
                })
                
                # 未出荷商品(件) - 玄米、白米、無洗米、ペットボトル
                data_to_write.append({
                    'range': gspread.utils.rowcol_to_a1(rows['count_not_expired'], col_teiki),
                    'values': [[teiki_count_not_expired]]
                })
                data_to_write.append({
                    'range': gspread.utils.rowcol_to_a1(rows['count_not_expired'], col_tanpin),
                    'values': [[tanpin_count_not_expired]]
                })
            
            # 累計の書き込み（B列: 定期便合計、C列: 単品合計）
            # 全ての商品(kg/本)の累計
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(rows['quantity_all'], 2),  # B列
                'values': [[teiki_quantity_total if teiki_quantity_total > 0 else '']]
            })
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(rows['quantity_all'], 3),  # C列
                'values': [[tanpin_quantity_total if tanpin_quantity_total > 0 else '']]
            })
            
            # 全ての商品(件)の累計
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(rows['count_all'], 2),  # B列
                'values': [[teiki_count_total if teiki_count_total > 0 else '']]
            })
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(rows['count_all'], 3),  # C列
                'values': [[tanpin_count_total if tanpin_count_total > 0 else '']]
            })
            
            # 未出荷商品(kg/本)の累計
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(rows['quantity_not_expired'], 2),  # B列
                'values': [[teiki_quantity_not_expired_total if teiki_quantity_not_expired_total > 0 else '']]
            })
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(rows['quantity_not_expired'], 3),  # C列
                'values': [[tanpin_quantity_not_expired_total if tanpin_quantity_not_expired_total > 0 else '']]
            })
            
            # 未出荷商品(件)の累計
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(rows['count_not_expired'], 2),  # B列
                'values': [[teiki_count_not_expired_total if teiki_count_not_expired_total > 0 else '']]
            })
            data_to_write.append({
                'range': gspread.utils.rowcol_to_a1(rows['count_not_expired'], 3),  # C列
                'values': [[tanpin_count_not_expired_total if tanpin_count_not_expired_total > 0 else '']]
            })

        # A4セルに今日の日付を設定
        today = datetime.now()
        year = today.year
        month = today.month
        day = today.day
        day_of_week = ['月', '火', '水', '木', '金', '土', '日'][today.weekday()]
        formatted_date = f"{year}年{month}月{day}日({day_of_week})"
        
        data_to_write.append({
            'range': 'A4',
            'values': [[formatted_date]]
        })

        # gspreadの`batch_update`を使って効率的に書き込み（リトライ付き）
        retry_with_backoff(worksheet.batch_update, data_to_write)
        print("スプレッドシートの更新が完了しました。")

    except gspread.exceptions.GSpreadException as e:
        error_msg_jp = translate_error(str(e))
        print(f"スプレッドシートAPIのエラー: {error_msg_jp}")
        print(f"詳細: {e}")
    except FileNotFoundError as e:
        error_msg_jp = translate_error(str(e))
        print(f"エラー: {error_msg_jp}")
        print(f"詳細: {e}")
    except Exception as e:
        error_msg_jp = translate_error(str(e))
        print(f"エラーが発生しました: {error_msg_jp}")
        print(f"詳細: {e}")


# ダウンロードフォルダのパス
downloads_folder = "/Users/nj-cmd11/Downloads"

try:
    # 今日ダウンロードしたdelivery_listから始まるCSVファイルを特定
    today_csv_files = find_today_delivery_csvs(downloads_folder)
    print(f"今日ダウンロードしたdelivery_listファイル: {[os.path.basename(f) for f in today_csv_files]}")

    # 複数のCSVファイルを読み込んで結合
    dataframes = []
    for csv_file in today_csv_files:
        try:
            df_temp = pd.read_csv(csv_file, encoding='cp932')
            dataframes.append(df_temp)
            print(f"CSVファイル「{os.path.basename(csv_file)}」をCP932で読み込みました。")
        except Exception as e:
            print(f"CSVファイル「{os.path.basename(csv_file)}」の読み込みでエラーが発生しました: {e}")
            continue
    
    if not dataframes:
        raise FileNotFoundError("有効なCSVファイルが読み込めませんでした。")
    
    # データフレームを結合
    df = pd.concat(dataframes, ignore_index=True)
    print(f"合計{len(dataframes)}件のCSVファイルを結合しました。")

    # 必要な列のみを抽出
    df = df[['返礼品', '出荷予定日', '出荷日', '申込日', '商品コード', '配送ステータス']]
    
    # カテゴリ分けと数量の抽出
    df['カテゴリ'] = df['返礼品'].apply(get_product_category)
    df['タイプ'] = df['返礼品'].apply(get_product_type)
    df['数量'] = df.apply(lambda row: get_product_quantity(row['返礼品'], row['カテゴリ']), axis=1)
    df['件数'] = df.apply(lambda row: get_product_count(), axis=1)
    df['月'] = df.apply(lambda row: get_month_with_fallback(row['出荷予定日'], row['出荷日']), axis=1)
    df['日付グループ'] = df.apply(lambda row: get_date_group(row['出荷予定日'], row['出荷日']), axis=1)
    df['出荷状況'] = df['配送ステータス'].apply(get_delivery_status)
    
    # 集計対象外の商品名を出力
    other_products = df[df['カテゴリ'] == "その他"]['返礼品'].unique()
    if len(other_products) > 0:
        print("以下の商品名は集計されませんでした:")
        for product in other_products:
            print(f"- {product}")
    else:
        print("集計対象外の商品は見つかりませんでした。")
        
    # 不要なカテゴリを除外
    df = df[df['カテゴリ'].isin(["玄米", "白米", "無洗米", "ペットボトル"])]
    
    # 集計除外対象を除外
    excluded_count = len(df[df['出荷状況'] == '集計除外'])
    if excluded_count > 0:
        print(f"集計から除外された件数: {excluded_count}件（配送キャンセル、返送、配送対象外）")
    df = df[df['出荷状況'] != '集計除外']
    
    # 集計（数量と件数の両方）
    summary_quantity = df.groupby(['月', 'カテゴリ', 'タイプ'])['数量'].sum().reset_index()
    summary_count = df.groupby(['月', 'カテゴリ', 'タイプ'])['件数'].sum().reset_index()
    
    # 「まだ過ぎていない」ものの集計（玄米、白米、無洗米、ペットボトル）
    not_expired_df = df[df['出荷状況'] == 'まだ過ぎてない']
    not_expired_summary_quantity = not_expired_df[not_expired_df['カテゴリ'].isin(['玄米', '白米', '無洗米', 'ペットボトル'])].groupby(['月', 'カテゴリ', 'タイプ'])['数量'].sum().reset_index()
    not_expired_summary_count = not_expired_df[not_expired_df['カテゴリ'].isin(['玄米', '白米', '無洗米', 'ペットボトル'])].groupby(['月', 'カテゴリ', 'タイプ'])['件数'].sum().reset_index()

    # 出荷スケジュール用の集計（月別・日付グループ別・カテゴリ別）
    # 日付グループがNoneのデータを除外
    schedule_df = df[df['日付グループ'].notna()]
    schedule_summary_quantity = schedule_df.groupby(['月', 'カテゴリ', '日付グループ'])['数量'].sum().reset_index()
    schedule_summary_count = schedule_df.groupby(['月', 'カテゴリ', '日付グループ'])['件数'].sum().reset_index()

    # スプレッドシートの更新処理を呼び出し
    update_spreadsheet(summary_quantity, summary_count, not_expired_summary_quantity, not_expired_summary_count)
    
    # 出荷スケジュールシートの更新処理を呼び出し
    update_schedule_spreadsheet(schedule_summary_quantity, schedule_summary_count)
    
    # 資材消費管理シートの更新処理を呼び出し
    update_material_consumption_spreadsheet(df)
    
except FileNotFoundError as e:
    error_msg_jp = translate_error(str(e))
    print(f"エラー: {error_msg_jp}")
    print(f"詳細: {e}")
except KeyError as e:
    error_msg_jp = translate_error(str(e))
    print(f"エラー: CSVファイルに指定された列が見つかりません: {error_msg_jp}")
    print(f"詳細: {e}")
except Exception as e:
    error_msg_jp = translate_error(str(e))
    print(f"エラーが発生しました: {error_msg_jp}")
    print(f"詳細: {e}")