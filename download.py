import time
import os
import glob
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.keys import Keys

# ダウンロードディレクトリのパス
DOWNLOAD_DIR = "/Users/nj-cmd11/Downloads"

def wait_for_download_complete(timeout=60):
    """
    ダウンロードディレクトリ内の.downloadファイルがなくなるまで待機する
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        # .downloadファイルを検索
        download_files = glob.glob(os.path.join(DOWNLOAD_DIR, "*.download"))
        if len(download_files) == 0:
            print("すべてのダウンロードが完了しました。")
            return True
        print(f"ダウンロード中... ({len(download_files)}個のファイルがダウンロード中)")
        time.sleep(1)
    
    # タイムアウト時も残っている.downloadファイルを報告
    remaining_files = glob.glob(os.path.join(DOWNLOAD_DIR, "*.download"))
    if remaining_files:
        print(f"警告: {len(remaining_files)}個の.downloadファイルが残っています: {remaining_files}")
        return False
    return True

# --- ヘッドレス用オプション ---
options = webdriver.ChromeOptions()
options.add_argument("--headless")           # ヘッドレスモード
options.add_argument("--no-sandbox")         # Linuxで権限関連エラー回避
options.add_argument("--disable-dev-shm-usage") # メモリ不足対策
options.add_argument("--window-size=1920,1080") # 画面サイズを指定
options.add_argument("--disable-gpu")        # GPU無効化（Windowsなら推奨）

# ダウンロードディレクトリを指定
prefs = {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True
}
options.add_experimental_option("prefs", prefs)

# WebDriverの初期化（Chromeを想定）
driver = webdriver.Chrome(options=options)

# クリック補助（オーバーレイ除去とJSクリックのフォールバック）
def safe_click(elem):
    # 画面を覆う拡張のオーバーレイを除去
    driver.execute_script("""
    const overlay = document.getElementById('desk-compass-snippet');
    if (overlay) overlay.remove();
    """)
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", elem)
    try:
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable(elem))
        elem.click()
    except Exception:
        # JSクリックにフォールバック
        driver.execute_script("arguments[0].click();", elem)

try:
    # 1-1. DOにログインする
    print("ログイン中...")
    driver.get("https://do3.do-furusato.com/deliveries")
    time.sleep(2)
    
    # ユーザー名とパスワードフィールドを取得
    username_field = driver.find_element(By.NAME, "username")
    password_field = driver.find_element(By.NAME, "password")
    
    # フィールドをクリアしてから入力
    username_field.clear()
    username_field.send_keys("a.tsuyuzaki@nnk")
    time.sleep(0.5)
    
    password_field.clear()
    password_field.send_keys("=fCK(2WR$ESe")
    time.sleep(0.5)
    
    driver.find_element(By.ID, "loginBtn1").click()
    time.sleep(3)
    
    # ログイン失敗時のアラートを処理
    try:
        WebDriverWait(driver, 3).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        alert_text = alert.text
        print(f"エラー: {alert_text}")
        alert.accept()
        raise Exception(f"ログインに失敗しました: {alert_text}")
    except TimeoutException:
        # アラートが表示されない場合はログイン成功とみなす
        print("ログイン成功を確認しました。")
    except Exception as e:
        if "ログインに失敗" in str(e):
            raise
        # その他のエラーは無視（アラートがない場合）
        pass

    # 1-5. 印刷管理に移動する
    print("印刷管理に移動中...")
    driver.get("https://do3.do-furusato.com/print-management")
    
    # テーブルの行を取得
    data_rows = WebDriverWait(driver, 20).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.p-table__dataList tbody tr"))
    )
    
    print(f"テーブルから{len(data_rows)}行を取得しました。")
    
    # ヘッダー行から列のインデックスを取得
    header_row = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.p-table__dataList thead tr"))
    )
    
    # ヘッダーから日付列と名前列のインデックスを取得
    date_column_index = None
    name_column_index = None
    
    header_cells = header_row.find_elements(By.CSS_SELECTOR, "th")
    for i, cell in enumerate(header_cells):
        cell_text = cell.text.strip()
        if "u-w12par" in cell.get_attribute("class"):
            date_column_index = i
            print(f"日付列のインデックス: {i} (クラス: {cell.get_attribute('class')})")
        elif "u-w10par" in cell.get_attribute("class"):
            name_column_index = i
            print(f"名前列のインデックス: {i} (クラス: {cell.get_attribute('class')})")
    
    if date_column_index is None or name_column_index is None:
        print("必要な列が見つかりませんでした。")
        driver.quit()
        exit()
    
    # 今日の日付を取得
    from datetime import datetime
    today = datetime.now().strftime("%Y/%m/%d")
    print(f"今日の日付: {today}")
    
    # 条件に合致する行を検索
    matching_rows = []
    for i, row in enumerate(data_rows):
        try:
            # 指定された列インデックスのtd要素を取得
            row_cells = row.find_elements(By.CSS_SELECTOR, "td")
            
            if len(row_cells) > max(date_column_index, name_column_index):
                date_text = row_cells[date_column_index].text.strip()
                name_text = row_cells[name_column_index].text.strip()
                
                # 日付から時間部分を除去して日付のみを取得
                date_only = date_text.split()[0] if ' ' in date_text else date_text
                
                # 条件チェック：今日の日付かつ「露崎 藍」
                if date_only == today and name_text == "露崎 藍":
                    matching_rows.append(i)
                    print(f"条件に合致: 行{i+1}")
            else:
                pass
                
        except Exception as e:
            continue
    
    print(f"条件に合致する行数: {len(matching_rows)}行")
    
    # 条件に合致する行のCSVファイルをダウンロード（リフレッシュせず順番に処理）
    for count, row_index in enumerate(matching_rows, 1):
        print(f"{count}番目のCSVファイルをダウンロード中...")
        try:
            # テーブルの行を再取得（リフレッシュはしない）
            data_rows = WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.p-table__dataList tbody tr"))
            )
            if row_index >= len(data_rows):
                print(f"行インデックス{row_index}が範囲外です。")
                continue

            download_link = data_rows[row_index].find_element(By.CSS_SELECTOR, "td.u-ta-c a")
            safe_click(download_link)
            
            # アラートにOK
            WebDriverWait(driver, 5).until(EC.alert_is_present())
            alert = driver.switch_to.alert
            alert.accept()
            print(f"{count}番目のCSVファイルのダウンロードを開始しました。")
            
            # ダウンロード開始を少し待つ（特に最後のファイルの場合）
            time.sleep(2)
            
            # ダウンロード完了を待機（各ファイルごとに最大60秒）
            wait_for_download_complete(timeout=60)
                
        except Exception as e:
            print(f"{count}番目のCSVダウンロードでエラーが発生しました: {e}")
    
    if len(matching_rows) == 0:
        print("条件に合致する行が見つかりませんでした。")
    else:
        # 最後にすべてのダウンロードが完了しているか確認
        print("すべてのCSVファイルのダウンロード完了を最終確認中...")
        # 少し待ってから最終確認（最後のダウンロードが確実に開始されるように）
        time.sleep(3)
        wait_for_download_complete(timeout=60)
        print("CSVファイルのダウンロード処理を完了しました。")

finally:
    driver.quit()