import os
import glob
import pandas as pd
import gspread
import re
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# 設定情報
SPREADSHEET_ID = "1swzp4-ISlM769K_dKR1BpdwQSkV19YmhHZuzwaXQCdI"
SHEET_NAME = "備考欄"
API_KEY_FILE = "key.json"


def read_csv_safely(csv_path):
    """複数のエンコーディングを試してCSVを読み込む"""
    encodings = ["cp932", "shift_jis", "utf-8", "utf-8-sig"]

    last_error = None
    for enc in encodings:
        try:
            # バイナリモードで読み込んでからデコードを試みる（エラーを置換）
            with open(csv_path, 'rb') as f:
                content = f.read()
                try:
                    content_decoded = content.decode(enc, errors='replace')
                    import io
                    df = pd.read_csv(io.StringIO(content_decoded), on_bad_lines='skip', engine='python')
                    # 列名に日本語が含まれているか確認（備考列があるか）
                    if any('備考' in str(col) or '入金' in str(col) for col in df.columns):
                        print(f"CSVファイル「{os.path.basename(csv_path)}」を {enc} で読み込みました。")
                        return df
                except Exception as decode_error:
                    last_error = f"Decode/Read error with {enc}: {str(decode_error)[:100]}"
                    continue
        except UnicodeDecodeError as e:
            last_error = f"UnicodeDecodeError with {enc}: {str(e)[:100]}"
            continue
        except Exception as e:
            last_error = f"Error with {enc}: {str(e)[:100]}"
            continue

    raise ValueError(f"CSV読み込みに失敗しました: {csv_path} (最後のエラー: {last_error})")


def process_note_text(text):
    """備考欄のテキストを加工する"""
    if pd.isna(text) or text == "":
        return ""
    
    text = str(text)
    
    # ① 改行とスペースを削除
    text = re.sub(r'\r\n|\r|\n', '', text)  # 改行を削除
    text = re.sub(r'\s+', '', text)  # 連続するスペースを削除
    
    # ② 「備考1：」より前を削除
    if '備考1：' in text:
        text = text.split('備考1：', 1)[1]
    
    # ③ 「ふるさと納税専用ページです」より後を削除
    if 'ふるさと納税専用ページです' in text:
        text = text.split('ふるさと納税専用ページです', 1)[0]
    
    # ④ 特定のテキストを削除
    text = text.replace('[備考欄:]', '')
    text = text.replace('[配送日時指定:]', '')
    text = text.replace('１．', '')
    text = text.replace('指定なし', '')
    
    # 前後の空白を削除
    text = text.strip()
    
    return text


def find_today_delivery_csvs(folder_path):
    """指定フォルダで今日ダウンロードした delivery_list*.csv を取得"""
    today = datetime.now().date()
    delivery_files = glob.glob(os.path.join(folder_path, 'delivery_list*.csv'))

    if not delivery_files:
        raise FileNotFoundError(f"{folder_path} に delivery_list*.csv がありません")

    today_files = []
    for file_path in delivery_files:
        ctime = datetime.fromtimestamp(os.path.getctime(file_path)).date()
        if ctime == today:
            today_files.append(file_path)

    if not today_files:
        raise FileNotFoundError("今日ダウンロードされた delivery_list*.csv がありません")

    print(f"対象CSVファイル数: {len(today_files)}")
    return today_files


def extract_unique_note_rows(csv_paths):
    """CSVのAA列に備考がある行で、AK列（入金日）が昨日の日付の行を取得し、重複を除外して返す"""
    all_rows = []
    note_col = None
    
    # 昨日の日付を取得（YYYY/MM/DD形式）
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")

    for csv_path in csv_paths:
        try:
            df = read_csv_safely(csv_path)
        except ValueError as e:
            print(f"警告: {csv_path} の読み込みに失敗しました。スキップします。")
            print(f"  エラー詳細: {e}")
            continue

        # AA列名が "AA" でない場合もあるため検索（「備考」列を優先、次に「配送用伝票備考」も確認）
        current_note_col = None
        for col in df.columns:
            if col == "備考":  # まず完全一致を探す
                current_note_col = col
                break
        
        # 「備考」列が見つからない場合、「配送用伝票備考」も確認
        if current_note_col is None:
            for col in df.columns:
                if col == "配送用伝票備考":
                    current_note_col = col
                    break
        
        if current_note_col is None:
            for col in df.columns:
                if "備考" in col:  # その他の備考列
                    current_note_col = col
                    break
        
        if current_note_col is None:
            for col in df.columns:
                if col == "AA":
                    current_note_col = col
                    break

        if current_note_col is None:
            raise ValueError("CSV内に備考列(AA)が見つかりません")
        
        # 最初のCSVファイルで見つかった備考列名を保存
        if note_col is None:
            note_col = current_note_col

        # 入金日列を検索
        payment_date_col = None
        for col in df.columns:
            if "入金" in col or col == "AK":
                payment_date_col = col
                break
        
        if payment_date_col is None:
            print(f"警告: {csv_path} に入金日列が見つかりません。スキップします。")
            continue

        # 備考が空でない行、かつ入金日が昨日の日付の行を抽出
        # 「備考」列と「配送用伝票備考」列の両方をチェック
        date_filter = (df[payment_date_col].astype(str).str.strip() == yesterday)
        
        # 「備考」列のフィルタ
        note_col_filter = None
        if "備考" in df.columns:
            note_col_filter = (df["備考"].notna() & 
                              (df["備考"].astype(str).str.strip() != "") &
                              (df["備考"].astype(str).str.strip() != "nan"))
        
        # 「配送用伝票備考」列のフィルタ
        delivery_note_filter = None
        if "配送用伝票備考" in df.columns:
            delivery_note_filter = (df["配送用伝票備考"].notna() & 
                                   (df["配送用伝票備考"].astype(str).str.strip() != "") &
                                   (df["配送用伝票備考"].astype(str).str.strip() != "nan"))
        
        # どちらかの備考列に値がある行を抽出
        if note_col_filter is not None and delivery_note_filter is not None:
            note_filter = note_col_filter | delivery_note_filter
        elif note_col_filter is not None:
            note_filter = note_col_filter
        elif delivery_note_filter is not None:
            note_filter = delivery_note_filter
        else:
            note_filter = pd.Series([False] * len(df), index=df.index)
        
        filtered = df[note_filter & date_filter]
        all_rows.append(filtered)

    if not all_rows:
        return pd.DataFrame()

    merged = pd.concat(all_rows, ignore_index=True)

    # 備考の完全一致で重複を削除（マージ後に再度備考列を検索）
    if note_col not in merged.columns:
        # マージ後に備考列を再検索
        for col in merged.columns:
            if col == "備考":
                note_col = col
                break
        if note_col not in merged.columns:
            for col in merged.columns:
                if "備考" in col and "配送用" not in col:
                    note_col = col
                    break
    
    merged = merged.drop_duplicates(subset=[note_col])

    # 必要な列を抽出（配送IDA, 寄付者D, 備考AA, 入金日AK）
    required_cols = {
        "A": None,
        "D": None,
        "AA": None,
        "AK": None
    }

    # CSV内の実際の列名をマッピング
    for col in merged.columns:
        if (col.startswith("配送") and ("ID" in col or "管理" in col)) or col == "A":
            required_cols["A"] = col
        # D列（寄附者）を取得（「寄附者番号」を除外）
        if col == "寄附者" or col == "寄付者":
            required_cols["D"] = col
        elif ("寄付者" in col or "寄附者" in col) and "番号" not in col and required_cols["D"] is None:
            required_cols["D"] = col
        # 備考列は既に見つけたnote_colを使用（「配送用伝票備考」を除外）
        if col == note_col:
            required_cols["AA"] = col
        if "入金" in col or col == "AK":
            required_cols["AK"] = col

    # 未発見の列チェック
    missing = [k for k, v in required_cols.items() if v is None]
    if missing:
        raise ValueError(f"CSV内に必要な列がありません: {missing}")

    # 備考列と配送用伝票備考列の両方を考慮して統合
    if "備考" in merged.columns and "配送用伝票備考" in merged.columns:
        # 両方の列がある場合、どちらか一方に値があれば使用
        merged["備考_統合"] = merged["備考"].fillna("") + merged["配送用伝票備考"].fillna("")
        merged["備考_統合"] = merged["備考_統合"].replace("", pd.NA)
        required_cols["AA"] = "備考_統合"
    elif "配送用伝票備考" in merged.columns and required_cols["AA"] is None:
        required_cols["AA"] = "配送用伝票備考"
    
    final_df = merged[[required_cols["A"], required_cols["D"], required_cols["AA"], required_cols["AK"]]].copy()
    final_df.columns = ["配送ID", "寄付者", "備考", "入金日"]
    
    # 備考欄の加工処理
    final_df["備考"] = final_df["備考"].apply(process_note_text)
    
    # ④ この段階でAA列備考欄が空になった行は削除
    final_df = final_df[final_df["備考"] != ""]
    
    # 加工後の備考欄で重複を削除（最終的な重複チェック）
    final_df = final_df.drop_duplicates(subset=["備考"], keep="first")
    
    # NaNを空文字列に変換（スプレッドシート書き込み用）
    final_df = final_df.fillna("")

    return final_df


def write_to_spreadsheet(df):
    """備考欄シート A1 と A2 の間に行を挿入。A〜Dに値、F列にチェックボックス。"""
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name(API_KEY_FILE, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    # データ件数分、2行目に一度に行を追加（レート制限対策）
    if len(df) > 0:
        empty_rows = [[""] * 6] * len(df)
        sheet.insert_rows(empty_rows, row=2)

    # A〜D に書き込み（NaNを空文字列に変換）
    values = df.fillna("").values.tolist()
    if len(values) > 0:
        sheet.update(range_name=f"A2:D{1 + len(values)}", values=values)

    # F列にチェックボックスを追加（dataValidationを使用）
    if len(df) > 0:
        checkbox_range = f"F2:F{1 + len(values)}"
        # チェックボックス形式を設定
        requests = [{
            "setDataValidation": {
                "range": {
                    "sheetId": sheet.id,
                    "startRowIndex": 1,  # 2行目（0ベース）
                    "endRowIndex": 1 + len(values),
                    "startColumnIndex": 5,  # F列（0ベース）
                    "endColumnIndex": 6
                },
                "rule": {
                    "condition": {
                        "type": "BOOLEAN"
                    },
                    "showCustomUi": True
                }
            }
        }]
        sheet.spreadsheet.batch_update({"requests": requests})
        
        # ブール値Falseを設定（チェックボックスとして表示される）
        # values_updateを使用してブール値を直接設定
        false_values = [[False] for _ in range(len(values))]
        sheet.spreadsheet.values_update(
            range=f"{SHEET_NAME}!{checkbox_range}",
            params={"valueInputOption": "USER_ENTERED"},
            body={"values": false_values}
        )

    print(f"スプレッドシートへの書き込み完了！ ({len(df)}件)")


def main():
    folder_path = "/Users/nj-cmd11/Downloads"  # 必要に応じて変更

    # 1. CSV取得
    csvs = find_today_delivery_csvs(folder_path)

    # 2-3. 備考あり行を抽出し加工
    df = extract_unique_note_rows(csvs)

    if df.empty:
        print("備考のある行がありませんでした。")
        return

    # 4. スプレッドシートへ書き込み
    write_to_spreadsheet(df)


if __name__ == "__main__":
    main()
