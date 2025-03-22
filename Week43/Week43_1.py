# SnowPark DataFrameを利用した解法


import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, json_extract_path_text, flatten

def main(session: snowpark.Session): 

    # FROSTYFRIDAY.DEMO.WEEK43テーブルのデータを抽出し、Snowpark DataFrameとして変数dｆにセット
    tableName = 'FROSTYFRIDAY_TEST.DEMO.WEEK43'
    df = session.table(tableName)
   
    # json_extract_path_textでjsonカラムから要素を指定してwithColumnで新規カラムとして追加（ネストされている要素は親要素.子要素で取得）
    df_1 = df.withColumn(
        "company_name", json_extract_path_text("json", lit("company_name"))
    ).withColumn(
        "company_website", json_extract_path_text("json", lit("company_website"))
    ).withColumn(
        "address", json_extract_path_text("json", lit("location.address"))
    ).withColumn(
        "city", json_extract_path_text("json", lit("location.city"))
    ).withColumn(
        "country", json_extract_path_text("json", lit("location.country"))
    ).withColumn(
        "state", json_extract_path_text("json", lit("location.state"))
    ).withColumn(
        "zip", json_extract_path_text("json", lit("location.zip"))
    )

    # join_table_function("flatten", df_1["json"], lit("superheroes"))でsuperheroesの配列を結合(元々1レコードだったものが配列の数の4行に増幅される)
    df_2 = df_1.join_table_function("flatten", df_1["json"], lit("superheroes"))#.drop("json").drop("seq").drop("key").drop("path").drop("index").drop("this")

    # dropで不要なカラムを削除(本来はflattenといっしょに実施で問題なし)
    df_2 = df_2.drop("json").drop("seq").drop("key").drop("path").drop("index").drop("this")

    # valueから要素を取り出してカラムに新規追加
    df_3 = df_2.withColumn(
        "id",json_extract_path_text("value", lit("id"))
    ).withColumn(
        "name",json_extract_path_text("value", lit("name"))
    ).withColumn(
        "powers",json_extract_path_text("value", lit("powers"))
    ).withColumn(
        "real_name",json_extract_path_text("value", lit("real_name"))
    ).withColumn(
        "role",json_extract_path_text("value", lit("role"))
    ).withColumn(
        "years_of_experience",json_extract_path_text("value", lit("years_of_experience"))
    ).drop("value")
    
    # idを先頭に配置。それ以外は元のカラム順を維持。
    df_4 = df_3.select(
        *(["ID"] + [col for col in df_3.columns if col != "ID"])
    )
    
    # 結果を返却
    return df_4