--ACCOUNTADMINロールで実行
--COMPUTE_WHウェアハウスで実行
use role ACCOUNTADMIN;
use warehouse COMPUTE_WH;

--SYSADMINにCOMPUTE_WHの権限を付与
grant usage on warehouse COMPUTE_WH to role SYSADMIN;

--SYSADMINロールで実行
--COMPUTE_WHウェアハウスで実行
use role SYSADMIN;
use warehouse COMPUTE_WH;

--データベースの作成
create or replace database FROSTYFRIDAY;

--スキーマの作成
create or replace schema FROSTYFRIDAY.DEMO;

--外部ステージの作成
CREATE OR REPLACE STAGE week1_basic
URL = 's3://frostyfridaychallenges/challenge_1/';

--外部ステージのファイル一覧を取得
LIST @week1_basic;

--ファイルの中身を検索
select
    $1
    -- ,$2
    -- ,$3
    -- ,$4
    -- ,$5
    ,metadata$filename AS file_name
    ,metadata$file_row_number AS file_row_numer
from
    @week1_basic;

--CTAS
CREATE OR REPLACE TABLE WEEK1_CTAS
AS
SELECT
    $1
    ,metadata$filename AS file_name
    ,metadata$file_row_number AS file_row_numer
FROM
    @week1_basic;

--WEEK1_CTAS検索
SELECT
    *
FROM
    WEEK1_CTAS
ORDER BY
    FILE_NAME
    ,FILE_ROW_NUMER;
    
--CREATE TABLE
CREATE OR REPLACE TABLE WEEK1_COPY
(
    message varchar(20)
    ,file_name varchar(50)
    ,file_row_number number(1)
);

--テーブルのデータ確認
SELECT
    *
FROM
    WEEK1_COPY;


--COPYでロード
COPY INTO
    WEEK1_COPY
FROM
    (
        SELECT
            $1
            ,metadata$filename AS file_name
            ,metadata$file_row_number AS file_row_numer
        FROM
            @week1_basic
    )
FILE_FORMAT = (SKIP_HEADER = 1);

--SELECT WEEK1_COPY
SELECT
    *
FROM
    WEEK1_COPY
ORDER BY
    FILE_NAME
    ,FILE_ROW_NUMBER;
