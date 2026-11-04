
CREATE DATABASE IF NOT EXISTS foodieanalytics;

USE foodieanalytics;

CREATE EXTERNAL TABLE IF NOT EXISTS restaurants (

    url STRING,
    address STRING,
    name STRING,
    online_order STRING,
    book_table STRING,
    rate STRING,
    votes STRING,
    phone STRING,
    location STRING,
    rest_type STRING,
    dish_liked STRING,
    cuisines STRING,
    approx_cost_for_two_people STRING,
    reviews_list STRING,
    menu_item STRING,
    listed_in_type STRING,
    listed_in_city STRING

)

ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)

STORED AS TEXTFILE

LOCATION '/foodieanalytics/hive_input'

TBLPROPERTIES (
    "skip.header.line.count" = "1"
);
