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
create database FROSTYFRIDAY;

--スキーマの作成
create or replace schema FROSTYFRIDAY.DEMO;

create or replace table FROSTYFRIDAY.DEMO.WEEK43 as select
parse_json('{
  "company_name": "Superhero Staffing Inc.",
  "company_website": "https://www.superherostaffing.com",
  "location": {
    "address": "123 Hero Lane",
    "city": "Metropolis",
    "state": "Superstate",
    "zip": "98765",
    "country": "United Superlands"
  },
  "superheroes": [
    {
      "id": "1",
      "name": "Captain Incredible",
      "real_name": "John Smith",
      "powers": [
        "Super Strength",
        "Flight",
        "Invulnerability"
      ],
      "role": "CEO",
      "years_of_experience": 10
    },
    {
      "id": "2",
      "name": "Mystic Sorceress",
      "real_name": "Jane Doe",
      "powers": [
        "Magic",
        "Teleportation",
        "Telekinesis"
      ],
      "role": "CTO",
      "years_of_experience": 8
    },
    {
      "id": "3",
      "name": "Speedster",
      "real_name": "Jim Brown",
      "powers": [
        "Super Speed",
        "Time Manipulation",
        "Phasing"
      ],
      "role": "COO",
      "years_of_experience": 6
    },
    {
      "id": "4",
      "name": "Telepathic Titan",
      "real_name": "Sarah Johnson",
      "powers": [
        "Telepathy",
        "Mind Control",
        "Telekinesis"
      ],
      "role": "CFO",
      "years_of_experience": 9
    }
  ]
}
') as json;

--WEEK43を検索
select
    *
from
    FROSTYFRIDAY.DEMO.WEEK43;
