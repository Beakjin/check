import os
import csv
import time
import chardet
from datetime import date, timedelta
import mysql.connector


today = date.today().strftime('%Y%m%d')
yymmdd = (date.today() - timedelta(days=1)).strftime('%y%m%d')
mmdd = date.today().strftime('%m%d')

config = {
    'user': 'root',
    'password': 'proszet6651',        # ← ここを実際のパスワードに
    'host': 'localhost',
    'database': 'tes',   # ← 接続先DB名
    'allow_local_infile': True          # ← これが --local-infile=1 に相当
}
"""
file_paths = {
    "hide_file" : fr"\\192.168.0.178\contents\WinActor\シナリオ\3004_非表示\keep\{yymmdd}\非表示_リスト.csv",
    "Yahoomaido":    fr"\\192.168.0.178\contents\☆制作・価格チーム\価格チェック\{mmdd}Yahooまいど全データ.csv",
    "Yahoocodi":     fr"\\192.168.0.178\contents\☆制作・価格チーム\価格チェック\{mmdd}Yahooコーデ全データ.csv",
    "rakutenmaido":  fr"\\192.168.0.178\contents\☆制作・価格チーム\価格チェック\{mmdd}楽天まいど全データ.csv",
    "rakutencodi":   fr"\\192.168.0.178\contents\☆制作・価格チーム\価格チェック\{mmdd}楽天コーデ全データ.csv",
    "ocha1":         fr"\\192.168.0.178\contents\☆制作・価格チーム\価格チェック\{mmdd}おちゃのこ１号店全データ.csv",
    "ocha2":         fr"\\192.168.0.178\contents\☆制作・価格チーム\価格チェック\{mmdd}おちゃのこ２号店全データ.csv",
    "kakakucom1":    fr"\\192.168.0.178\contents\WinActor\シナリオ\価格全データDL\temp\{mmdd}価格コム１号店.csv",
    "kakakucom2":    fr"\\192.168.0.178\contents\WinActor\シナリオ\価格全データDL\temp\{mmdd}価格コム２号店.csv",
    "kakakurobot1":  fr"\\192.168.0.178\contents\WinActor\シナリオ\価格全データDL\temp\{mmdd}価格ロボット１号店.csv",
    "kakakurobot2":  fr"\\192.168.0.178\contents\WinActor\シナリオ\価格全データDL\temp\{mmdd}価格ロボット２号店.csv"
}
"""

file_paths = {
    "hide_file" : fr"C:\Users\PROSZET022\Desktop\リンク社タスクスケジューラ\仮想ナス\非表示_リスト.csv",
    "Yahoomaido":    fr"C:\Users\PROSZET022\Desktop\リンク社タスクスケジューラ\仮想ナス\{mmdd}Yahooまいど全データ.csv",
    "Yahoocodi":     fr"C:\Users\PROSZET022\Desktop\リンク社タスクスケジューラ\仮想ナス\{mmdd}Yahooコーデ全データ.csv",
    "rakutenmaido":  fr"C:\Users\PROSZET022\Desktop\リンク社タスクスケジューラ\仮想ナス\{mmdd}楽天まいど全データ.csv",
    "rakutencodi":   fr"C:\Users\PROSZET022\Desktop\リンク社タスクスケジューラ\仮想ナス\{mmdd}楽天コーデ全データ.csv",
    "ocha1":         fr"C:\Users\PROSZET022\Desktop\リンク社タスクスケジューラ\仮想ナス\{mmdd}おちゃのこ１号店全データ.csv",
    "ocha2":         fr"C:\Users\PROSZET022\Desktop\リンク社タスクスケジューラ\仮想ナス\{mmdd}おちゃのこ２号店全データ.csv",
    "kakakucom1":    fr"C:\Users\PROSZET022\Desktop\リンク社タスクスケジューラ\仮想ナス\{mmdd}価格コム１号店.csv",
    "kakakucom2":    fr"C:\Users\PROSZET022\Desktop\リンク社タスクスケジューラ\仮想ナス\{mmdd}価格コム２号店.csv",
    "kakakurobot1":  fr"C:\Users\PROSZET022\Desktop\リンク社タスクスケジューラ\仮想ナス\{mmdd}価格ロボット１号店.csv",
    "kakakurobot2":  fr"C:\Users\PROSZET022\Desktop\リンク社タスクスケジューラ\仮想ナス\{mmdd}価格ロボット２号店.csv"
}




#######"""全データコピー"""#######
def detect_encoding(file_path, sample_size=10000):
    with open(file_path, 'rb') as f:
        raw_data = f.read(sample_size)
    result = chardet.detect(raw_data)
    return result['encoding']

def copy_and_convert_to_utf8_csv():
    converted_paths = {}  # ← 各ファイルのラベルごとにフルパスを格納

    for label, src_path in file_paths.items():
        if os.path.isfile(src_path):
            dest_path = os.path.join(destination_folder, os.path.basename(src_path))
            try:
                # 文字コード判定
                encoding = detect_encoding(src_path)

                with open(src_path, 'r', encoding=encoding, newline='') as f_in:
                    reader = csv.reader(f_in)
                    rows = list(reader)
                    if "非表示" in src_path and len(rows) > 1:
                        rows = rows[:-1]
                        print(f"非表示リスト検出：最終行を削除します → {src_path}")

                with open(dest_path, 'w', encoding='utf-8-sig', newline='') as f_out:
                    writer = csv.writer(f_out, quoting=csv.QUOTE_ALL)
                    writer.writerows(rows)

                print(f"✅ 変換成功（{encoding} → UTF-8）: {os.path.basename(src_path)}")
                converted_paths[label] = dest_path  # ← ラベルをキーにフルパスを保存

            except Exception as e:
                print(f"❌ 変換失敗: {src_path} → {e}")
        else:
            print(f"⚠️ ファイルが存在しません: {src_path}")

    return converted_paths  # ← 辞書で返す

#######"""テーブル内クリア"""#######
def truncate_items(items, config):

    try:
        # 接続開始
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        # クエリ組み立てと実行
        cursor.execute(f"TRUNCATE TABLE `{items}`;")
        print(f"テーブル「{items}」のTRUNCATEが完了しました。")

        # 終了処理
        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"エラー: {err}")

def truncate(site, config):

    try:
        # 接続開始
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        # クエリ組み立てと実行
        cursor.execute(f"TRUNCATE TABLE `{site}`;")
        print(f"テーブル「{site}」のTRUNCATEが完了しました。")

        # 終了処理
        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"エラー: {err}")

def notyet(config):
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        # 1. テーブル初期化（全削除）
        cursor.execute("TRUNCATE TABLE result;")

        # 2. カラム一覧取得（id以外）test
        cursor.execute("SHOW COLUMNS FROM result;")
        columns = [row[0] for row in cursor.fetchall() if row[0].lower() != 'id']

        # 3. id + 他のカラムに値をINSERT（'未処理'を入れる）
        col_names = ', '.join(['id'] + [f"`{col}`" for col in columns])
        placeholders = ', '.join(['%s'] * (len(columns) + 1))
        values = [1] + ['未処理'] * len(columns)

        insert_query = f"INSERT INTO result ({col_names}) VALUES ({placeholders});"
        cursor.execute(insert_query, values)
        conn.commit()

        print("resultテーブルを初期化し、全カラムに『未処理』を設定しました。")

    finally:
        cursor.close()
        conn.close()
#######"""データ取り込み"""#######
def load_csv_hide(config):
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        
        load_sql = r"""
            LOAD DATA LOCAL INFILE 'C:\\Users\\PROSZET022\\Desktop\\全データ\\全データ\\非表示_リスト.csv'
            INTO TABLE items
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\r\n'
            IGNORE 1 LINES
            (code, col2, col3);
        """

        cursor.execute(load_sql)
        conn.commit()
        print("✅ CSVのデータを items テーブルにロードしました。")

        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"❌ MySQLエラー: {err}")

def load_yahoo(inputcsv,site):
    try:
        
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        load_sql = f"""
            LOAD DATA LOCAL INFILE '{inputcsv}'
            INTO TABLE `{site}`
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\r\\n'
            IGNORE 1 LINES
            (code, path, name, original_price, price, ship_weight, delivery, product_category, display);
        """
        
        cursor.execute(load_sql)
        conn.commit()
        

        # 件数取得
        cursor.execute(f"SELECT COUNT(code) FROM `{site}`;")
        count = cursor.fetchone()[0]
        print(f"取り込み完了✅ 登録件数: {count} 件")

        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"❌ MySQLエラー: {err}")     

def load_rakuten(inputcsv,site):
    try:
        
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        load_sql = f"""
            LOAD DATA LOCAL INFILE '{inputcsv}'
            INTO TABLE `{site}`
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\r\\n'
            IGNORE 1 LINES
            (コントロールカラム, 商品管理番号（商品URL）, 商品名, 販売価格, 表示価格, 送料, SKU倉庫指定, 配送方法セット管理番号, SKU管理番号, 倉庫指定, 在庫あり時納期管理番号, マルチSKU);
        """
        
        cursor.execute(load_sql)
        conn.commit()
        

        # 件数取得
        cursor.execute(f"SELECT COUNT(商品管理番号（商品URL）) FROM `{site}`;")
        count = cursor.fetchone()[0]
        print(f"取り込み完了✅ 登録件数: {count} 件")

        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"❌ MySQLエラー: {err}")  

#######"""データ突合せ"""#######

def tukiawase_Yahoo(config,site):

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        query = f"""
        SELECT 
            IF(COUNT(*) = 0, '完了', GROUP_CONCAT(code)) AS result
        FROM (
            SELECT 
                i.code
            FROM 
                items i
            INNER JOIN 
                `{site}` y ON i.code = y.code
            WHERE 
                y.display = 1
        ) AS sub;
        """

        cursor.execute(query)
        result = cursor.fetchone()[0]

        update_query = "UPDATE result SET `{}` = %s WHERE id = 1;".format(site)
        cursor.execute(update_query, (result,))
        conn.commit()
            
        print(result)
        return result[0]  # タプルの最初の要素（resultカラム）

    finally:
        cursor.close()
        conn.close()

def tukiawase_rakuten(config,site):
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        # 1. カラム取得
        cursor.execute(f"SHOW COLUMNS FROM `{site}`;")
        columns = [row[0] for row in cursor.fetchall()]
        match_column = columns[1]   # 2列目（比較対象）
        value_column = columns[9]   # 10列目（表示用）

        print(f"比較カラム（2列目）: {match_column}")
        print(f"取得カラム（10列目）: {value_column}")

        # 2. JOIN & 必要な2列（比較列と code）を取得
        query = f"""
        SELECT 
            IF(SUM(sub.{value_column} = 1) = COUNT(*), '完了',
                GROUP_CONCAT(
                    IF(sub.{value_column} = 1, NULL, 
                        IF(sub.{value_column} IS NULL OR sub.{value_column} = 0, sub.code, sub.{value_column})
                    )
                )
            ) AS result
        FROM (
            SELECT 
                i.code,
                r.{value_column}
            FROM 
                items i
            INNER JOIN 
                {site} r
            ON 
                i.code = r.{match_column}
        ) AS sub;
        """
        cursor.execute(query)
        result = cursor.fetchone()[0]

        update_query = "UPDATE result SET `{}` = %s WHERE id = 1;".format(site)
        cursor.execute(update_query, (result,))
        conn.commit()

        print(f"保存された結果: {result}")
        return result

    finally:
        cursor.close()
        conn.close()


#######"""CSV保存"""#######
def export_result(config,csv_path='result_export.csv'):
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        # データ取得
        cursor.execute(f"SELECT * FROM result;")
        rows = cursor.fetchall()

        # カラム名取得
        column_names = [desc[0] for desc in cursor.description]

        # CSV出力
        with open(csv_path, mode='w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(column_names)  # ヘッダー
            writer.writerows(rows)         # データ

        print(f"result テーブルをCSV出力しました: {csv_path}")

    finally:
        cursor.close()
        conn.close()

"""本処理"""

# 保存先フォルダを作成
base_dir = os.path.dirname(os.path.abspath(__file__))
destination_folder = os.path.join(base_dir, "全データ")
os.makedirs(destination_folder, exist_ok=True)

# 実行

converted_files = copy_and_convert_to_utf8_csv()
print(converted_files)

#取り込み,突合せ
time.sleep(1)
items = "items"
truncate_items(items, config)
time.sleep(1)
load_csv_hide(config)
time.sleep(1)


notyet(config)

site = "yahoomaido_products"
truncate(site, config)
time.sleep(1)
inputcsv = converted_files['Yahoomaido'].replace("\\", "\\\\")
load_yahoo(inputcsv,site)
tukiawase_Yahoo(config,site)

site = "yahoocodi_products"
truncate(site, config)
time.sleep(1)
inputcsv = converted_files['Yahoocodi'].replace("\\", "\\\\")
load_yahoo(inputcsv,site)
tukiawase_Yahoo(config,site)

site = "rakutenmaido_products"
truncate(site, config)
time.sleep(1)
inputcsv = converted_files['rakutenmaido'].replace("\\", "\\\\")
load_rakuten(inputcsv,site)
tukiawase_rakuten(config,site)

site = "rakutencodi_products"
truncate(site, config)
time.sleep(1)
inputcsv = converted_files['rakutencodi'].replace("\\", "\\\\")
load_rakuten(inputcsv,site)
tukiawase_rakuten(config,site)



export_result(config, csv_path='result_export.csv')



"""本処理"""