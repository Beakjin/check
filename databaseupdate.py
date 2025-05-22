import pandas as pd
import mysql.connector

# DB接続設定
config = {
    'user': 'root',
    'password': 'proszet6651',
    'host': 'localhost',
    'database': 'tes'
}

# 1. MySQLから既存データを読み込む
def fetch_existing_data():
    conn = mysql.connector.connect(**config)
    df = pd.read_sql("SELECT * FROM rakutencodi_products", conn)
    conn.close()
    return df

# 2. 新CSV読み込み
def load_new_csv(path):
    return pd.read_csv(path)

# 3. 差分を検出してDB更新
def compare_and_update_db(df_existing, df_new):

    
    key = '商品管理番号（商品URL）'

    # カラム名を共通化
    df_existing.columns = df_existing.columns.str.replace('\u3000', '').str.replace('\r|\n', '', regex=True).str.strip()
    df_new.columns = df_new.columns.str.replace('\u3000', '').str.replace('\r|\n', '', regex=True).str.strip()

    df_existing.set_index(key, inplace=True)
    df_new.set_index(key, inplace=True)

    # 差分検出
    df_diff = df_new[df_new.index.isin(df_existing.index)]
    df_updated = df_diff[df_diff.ne(df_existing).any(axis=1)]
    df_insert = df_new[~df_new.index.isin(df_existing.index)]
    df_delete = df_existing[~df_existing.index.isin(df_new.index)]

    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # ✅ update SQL文を定義
    update_sql = """
        UPDATE rakutencodi_products SET
            商品名 = %s,
            販売価格 = %s,
            表示価格 = %s,
            送料 = %s,
            SKU倉庫指定 = %s,
            配送方法セット管理番号 = %s,
            SKU管理番号 = %s,
            倉庫指定 = %s,
            在庫あり時納期管理番号 = %s,
            マルチSKU = %s,
            最終更新日 = NOW()
        WHERE `商品管理番号（商品URL）` = %s
    """

    # 更新処理
    for idx, row in df_insert.iterrows():
        values = [
            idx,
            row["商品名"],
            row["販売価格"],
            row["表示価格"],
            row["送料"],
            row["SKU倉庫指定"],
            row["配送方法セット管理番号"],
            row["SKU管理番号"],
            row["倉庫指定"],
            row["在庫あり時納期管理番号"],
            row["マルチSKU"]
        ]
        
        insert_sql = """
            INSERT INTO rakutencodi_products (
                `商品管理番号（商品URL）`, 商品名, 販売価格, 表示価格, 送料,
                SKU倉庫指定, 配送方法セット管理番号, SKU管理番号, 倉庫指定,
                在庫あり時納期管理番号, マルチSKU, 最終更新日
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        
        cursor.execute(insert_sql, values)

    # 削除処理
    for idx in df_delete.index:
        delete_sql = "DELETE FROM rakutencodi_products WHERE `商品管理番号（商品URL）` = %s"
        cursor.execute(delete_sql, (idx,))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ 更新: {len(df_updated)} 件, 追加: {len(df_insert)} 件, 削除: {len(df_delete)} 件")

# 実行
csv_path = 'C:/Users/PROSZET022/Desktop/rakuten_new.csv'
existing = fetch_existing_data()
new = load_new_csv(csv_path)
compare_and_update_db(existing, new)
