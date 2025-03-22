# Pandas DataFrameを利用した解法

import snowflake.snowpark as snowpark
import pandas as pd
import json

def main(session: snowpark.Session): 

    # 使用するスキーマを指定
    session.sql("USE SCHEMA FROSTYFRIDAY_TEST.DEMO").collect()

    # テーブルのデータを取得し、Pandas DataFrame に変換
    table_name = 'WEEK43'
    pdf = session.table(table_name).to_pandas()  

    # JSONデータを辞書型に変換
    pdf["JSON"] = pdf["JSON"].apply(json.loads)

    # JSONデータをフラット化
    flat_pdf = pd.json_normalize(pdf["JSON"])

    # 必要なカラムを選択し、カラム名を統一
    flat_pdf = flat_pdf[[
        "company_name", "company_website", "location.address",
        "location.city", "location.country", "location.state", "location.zip", "superheroes"
    ]]
    
    flat_pdf.columns = ["company_name", "company_website", "address", "city", "country", "state", "zip", "superheroes"]
    
    # `superheroes` のリストを展開（explode）
    flat_pdf_exploded = flat_pdf.explode("superheroes")
    
    # `superheroes` の詳細情報をフラット化
    hero_pdf = pd.json_normalize(flat_pdf_exploded["superheroes"])

    # インデックスをリセットして重複を防ぐ
    flat_pdf_exploded.reset_index(drop=True, inplace=True)
    hero_pdf.reset_index(drop=True, inplace=True)
    
    # 会社情報とヒーロー情報を結合
    final_pdf = pd.concat([flat_pdf_exploded.drop(columns=["superheroes"]), hero_pdf], axis=1)
    
    # カラム名を大文字に変更
    final_pdf.columns = final_pdf.columns.str.upper()
    
    # idを最初のカラムに移動
    final_pdf = final_pdf[["ID"] + [col for col in final_pdf.columns if col != "ID"]]
    
    # NaN値を補完（1レコード目の値で埋める）
    final_pdf.fillna(method='ffill', axis=0, inplace=True)
    
    # Pandas DataFrame を Snowpark DataFrame に変換
    final_df = session.create_dataframe(final_pdf)
    
    # Snowpark DataFrame を返す
    return final_df
