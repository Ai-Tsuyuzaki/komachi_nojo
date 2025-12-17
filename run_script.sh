#!/bin/bash

# スクリプトの実行ディレクトリに移動
cd "/Users/nj-cmd11/Documents/2025/05 ふるさと納税/潟上市/★こまち農場"

# Pythonスクリプトを順次実行
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 search.py

echo "search.py完了。300秒待機中..."
sleep 300

/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 download.py
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 edit.py
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 bikou.py

/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 check_c4_alert.py