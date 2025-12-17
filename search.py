import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.keys import Keys

# --- ヘッドレス用オプション ---
options = webdriver.ChromeOptions()
options.add_argument("--headless")           # ヘッドレスモード
options.add_argument("--no-sandbox")         # Linuxで権限関連エラー回避
options.add_argument("--disable-dev-shm-usage") # メモリ不足対策
options.add_argument("--window-size=1920,1080") # 画面サイズを指定
options.add_argument("--disable-gpu")        # GPU無効化（Windowsなら推奨）

# WebDriverの初期化（Chromeを想定）
driver = webdriver.Chrome(options=options)

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

    # 1-2. 配送検索に移動する
    print("配送検索に移動中...")
    driver.get("https://do3.do-furusato.com/deliveries")
    time.sleep(2)

    # 1-2. 配送検索画面に移動する
    print("配送検索画面に移動中...")
    driver.get("https://do3.do-furusato.com/deliveries")
    time.sleep(2)

    # 1-3. 絞り込み検索する
    print("絞り込み検索を実行中...")

    # 配送ステータスの選択を解除する
    search_input = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, ".chosen-search-input"))
    )

    search_input.click()
    search_input.send_keys(Keys.BACKSPACE)

    # 「もみがらエネルギー株式会社」を選択
    search_input = WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "input.chosen-search-input.default"))
    )
    search_input.click()
    search_input.send_keys("147503：もみがらエネルギー株式会社\n")
    time.sleep(3)

    # 検索ボタンをクリック
    search_btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "searchBtn"))
    )
    driver.execute_script("arguments[0].click();", search_btn)
    WebDriverWait(driver, 10).until(
        EC.invisibility_of_element_located((By.ID, "mask"))
    )
    time.sleep(2)

    # 1-4. CSVをダウンロード
    print("CSVダウンロード処理を開始中...")
    
    # データ出力ボタンをクリック
    try:
        export_btn = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "exportBtn"))
        )
        print("データ出力ボタンをクリック中...")
        export_btn.click()
        print("データ出力ボタンをクリックしました。")
    except TimeoutException:
        print("データ出力ボタンが見つかりませんでした。")
        raise
    except Exception as e:
        print(f"データ出力ボタンのクリック中にエラーが発生しました: {e}")
        raise
    
    # ポップアップ(iframe)に遷移する
    popup_body = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "iframe"))
    )
    driver.switch_to.frame(popup_body) 
    
    # 「全ての検索結果に対して処理を行う」にチェック
    all_process_checkbox = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "isallprocess"))
    )
    if not all_process_checkbox.is_selected():
        all_process_checkbox.click()
    
    # 「検索結果」のラジオボタンにチェック
    search_result_radio = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "is_export_type_search_result"))
    )
    if not search_result_radio.is_selected():
        search_result_radio.click()

    # ドロップダウン内の「検索結果」を選択
    search_result_select_container = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "export_type_search_result_chosen"))
    )
    search_result_select_container.click()
    
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//li[text()='検索結果']"))
    ).click()
    
    # ドロップダウン内の「csv」を選択
    csv_select_container = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "download_type_search_result_chosen"))
    )
    csv_select_container.click()
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//li[text()='csv']"))
    ).click()

    # ダウンロード実行ボタンをクリック
    export_confirm_btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "exportBtn"))
    )
    export_confirm_btn.click()
    
    # ダウンロード後のアラートを処理
    try:
        print("アラート待機中...")
        WebDriverWait(driver, 30).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        print("アラートを検出しました。")
        alert.accept()
        print("アラートを承認しました。")
    except TimeoutException:
        print("アラートが表示されませんでした（タイムアウト）。処理を続行します。")
        pass
    except Exception as e:
        print(f"アラート処理中にエラーが発生しました: {e}")
        pass
    
    # iframeからメインコンテンツに戻る
    driver.switch_to.default_content()
    
    # ポップアップを閉じるボタンをクリック
    close_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "li.highslide-close a"))
    )
    close_button.click()

    print("検索とCSVダウンロードが完了しました。")

finally:
    driver.quit()
