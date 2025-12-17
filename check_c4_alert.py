import gspread
import tkinter as tk
import time
from tkinter import scrolledtext

# 設定情報（edit.pyと同じ）
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
            # 元の英語メッセージも括弧内に残す
            if jpn not in translated:
                translated = f"{jpn} ({eng})"
    
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

def show_alert(title, value=None):
    """アラートウィンドウを表示します。"""
    root = tk.Tk()
    root.title("警告")
    root.attributes("-topmost", True)
    root.configure(bg="#ffffff")  # 白背景
    
    # メッセージの長さに応じてウィンドウサイズを動的に調整
    message_length = len(str(title)) + (len(str(value)) if value else 0)
    if message_length > 200:
        window_width = 800
        window_height = 500
    elif message_length > 100:
        window_width = 700
        window_height = 450
    else:
        window_width = 600
        window_height = 400
    
    # 画面サイズを取得
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    
    # 画面中央の座標を計算
    center_x = int(screen_width / 2 - window_width / 2)
    center_y = int(screen_height / 2 - window_height / 2)
    
    # ウィンドウを画面中央に配置
    root.geometry(f"{window_width}x{window_height}+{center_x}+{center_y}")
    
    # メインフレーム（パディング用）
    main_frame = tk.Frame(root, bg="#ffffff")
    main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
    
    # 警告アイコン（大きな絵文字）
    icon_label = tk.Label(
        main_frame, 
        text="⚠️", 
        font=("Arial", 80),
        bg="#ffffff",
        fg="#ff4444"
    )
    icon_label.pack(pady=(0, 20))
    
    # タイトル
    title_label = tk.Label(
        main_frame,
        text="異常検出",
        font=("Arial", 28, "bold"),
        bg="#ffffff",
        fg="#333333"
    )
    title_label.pack(pady=(0, 15))
    
    # メインメッセージ（長い場合はスクロール可能なTextウィジェットを使用）
    if len(str(title)) > 100 or (value and len(str(value)) > 100):
        # スクロール可能なTextウィジェットを使用
        msg_frame = tk.Frame(main_frame, bg="#ffffff")
        msg_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        msg_text = scrolledtext.ScrolledText(
            msg_frame,
            wrap=tk.WORD,
            font=("Arial", 14),
            bg="#ffffff",
            fg="#cc0000",
            height=8,
            width=70,
            relief=tk.FLAT,
            borderwidth=0
        )
        msg_text.pack(fill=tk.BOTH, expand=True)
        msg_text.insert("1.0", str(title))
        msg_text.config(state=tk.DISABLED)  # 編集不可
        
        if value:
            value_text = scrolledtext.ScrolledText(
                msg_frame,
                wrap=tk.WORD,
                font=("Arial", 12, "bold"),
                bg="#fff3cd",
                fg="#856404",
                height=6,
                width=70,
                relief=tk.RAISED,
                bd=2
            )
            value_text.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
            value_text.insert("1.0", f"詳細情報:\n{translate_error(str(value))}")
            value_text.config(state=tk.DISABLED)  # 編集不可
    else:
        # 短いメッセージの場合は通常のLabelを使用
        main_msg = tk.Label(
            main_frame,
            text=title,
            font=("Arial", 18),
            bg="#ffffff",
            fg="#cc0000",
            wraplength=window_width - 80,
            justify=tk.LEFT
        )
        main_msg.pack(pady=(0, 10))
        
        # 値の部分（強調表示）
        if value:
            value_label = tk.Label(
                main_frame,
                text=f"詳細情報:\n{translate_error(str(value))}",
                font=("Arial", 14, "bold"),
                bg="#fff3cd",
                fg="#856404",
                relief=tk.RAISED,
                bd=2,
                padx=15,
                pady=10,
                wraplength=window_width - 80,
                justify=tk.LEFT
            )
            value_label.pack(pady=10)
    
    # 閉じるボタン（Labelを使って背景を完全に削除）
    close_button = tk.Label(
        main_frame,
        text="閉じる",
        font=("Arial", 18, "bold"),
        fg="#000000",
        bg="#ffffff",
        cursor="hand2"
    )
    close_button.pack(pady=(20, 10))
    close_button.bind("<Button-1>", lambda e: root.destroy())
    close_button.bind("<Enter>", lambda e: close_button.config(fg="#333333"))
    close_button.bind("<Leave>", lambda e: close_button.config(fg="#000000"))
    
    root.mainloop()

def has_negative_pattern(value):
    """値にマイナスを含む異常なパターンがあるかどうかを判定します。
    例: "+ -449.292 t" のような「+ -」パターン
    """
    if value is None or value == "":
        return False
    
    value_str = str(value).strip()
    
    # 「+ -」というパターンが含まれているかチェック（例: "+ -449.292 t"）
    if "+ -" in value_str or "+-" in value_str:
        return True
    
    # 「+」と「-」が両方含まれているかチェック（異常な状態）
    if "+" in value_str and "-" in value_str:
        return True
    
    return False

def check_c4_cell():
    """C4セルの内容を確認し、マイナスが含まれている場合はアラートを表示します。"""
    try:
        # 認証情報とワークシート取得（リトライ付き）
        def get_c4_value():
            gc = gspread.service_account(filename=API_KEY_FILE)
            sh = gc.open_by_key(SPREADSHEET_ID)
            worksheet = sh.worksheet(SHEET_NAME)
            return worksheet.acell('C4').value
        
        # C4セルの値を取得（リトライ付き）
        c4_value = retry_with_backoff(get_c4_value)
        
        print(f"C4セルの内容: '{c4_value}'")
        
        # マイナスを含む異常なパターンが含まれているかチェック
        if has_negative_pattern(c4_value):
            title = "C4セルに異常な値が検出されました！"
            value = str(c4_value)
            print(f"警告: {title} 値: {value}")
            show_alert(title, value)
            return True
        else:
            print("C4セルは正常です。")
            return False
            
    except gspread.exceptions.GSpreadException as e:
        error_msg_jp = translate_error(str(e))
        error_msg = f"スプレッドシートAPIのエラーが発生しました\n\n{error_msg_jp}\n\n詳細: {str(e)}"
        print(f"スプレッドシートAPIのエラー: {e}")
        show_alert("⚠️ スプレッドシートAPIエラー", error_msg)
    except FileNotFoundError as e:
        error_msg_jp = translate_error(str(e))
        error_msg = f"ファイルが見つかりません\n\n{error_msg_jp}\n\n詳細: {str(e)}"
        print(f"エラー: {e}")
        show_alert("⚠️ ファイルエラー", error_msg)
    except Exception as e:
        error_msg_jp = translate_error(str(e))
        error_msg = f"予期しないエラーが発生しました\n\n{error_msg_jp}\n\n詳細: {str(e)}"
        print(f"エラーが発生しました: {e}")
        show_alert("⚠️ エラー", error_msg)

if __name__ == "__main__":
    check_c4_cell()

