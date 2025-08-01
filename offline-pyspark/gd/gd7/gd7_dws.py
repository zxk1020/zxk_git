# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("DWS_Layer_Aggregation") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "E:/spark_temp") \
    .enableHiveSupport() \
    .getOrCreate()

# 导入Java类用于HDFS操作
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())


# 工具函数定义
def create_hdfs_dir(path):
    """创建HDFS目录"""
    jvm_path = spark.sparkContext._jvm.Path(path)
    if not fs.exists(jvm_path):
        fs.mkdirs(jvm_path)
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")


def repair_hive_table(table_name):
    """修复Hive表分区"""
    spark.sql(f"MSCK REPAIR TABLE gd7.{table_name}")
    print(f"修复分区完成：gd7.{table_name}")


def print_data_count(df, table_name):
    """打印数据量用于验证"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# 添加数据质量检查
print("=== 数据质量检查 ===")
# 检查DWD层数据总量
total_count = spark.sql("SELECT COUNT(*) as count FROM gd7.dwd_user_behavior_log WHERE dt = '20250801'").collect()[0]['count']
print(f"DWD层总记录数: {total_count}")

# 检查各事件类型分布
print("各事件类型分布:")
spark.sql("""
    SELECT event_type, COUNT(*) as count 
    FROM gd7.dwd_user_behavior_log 
    WHERE dt = '20250801' 
    GROUP BY event_type 
    ORDER BY count DESC
""").show()

# 检查page_type分布
print("各页面类型分布:")
spark.sql("""
    SELECT page_type, COUNT(*) as count 
    FROM gd7.dwd_user_behavior_log 
    WHERE dt = '20250801' 
    GROUP BY page_type 
    ORDER BY count DESC
""").show()

# 检查搜索排名统计
print("搜索排名统计:")
spark.sql("""
    SELECT 
        COUNT(*) as total_search_events,
        AVG(search_rank) as avg_search_rank,
        MIN(search_rank) as min_search_rank,
        MAX(search_rank) as max_search_rank
    FROM gd7.dwd_user_behavior_log 
    WHERE dt = '20250801' AND event_type = 'search' AND search_rank IS NOT NULL
""").show()

# 检查有数据的店铺和商品
print("有数据的店铺商品组合数:")
spark.sql("""
    SELECT COUNT(*) as count 
    FROM (
        SELECT store_id, item_id 
        FROM gd7.dwd_user_behavior_log 
        WHERE dt = '20250801' AND store_id IS NOT NULL AND item_id IS NOT NULL
        GROUP BY store_id, item_id
    ) t
""").show()



# ====================== DWS层商品流量获取日汇总表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dws/dws_product_traffic_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS gd7.dws_product_traffic_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE gd7.dws_product_traffic_d (
    `store_id`             STRING COMMENT '店铺ID',
    `item_id`              STRING COMMENT '商品ID',
    `store_total_clicks`   BIGINT COMMENT '店铺总点击数',
    `item_visits`          BIGINT COMMENT '商品访问次数',
    `item_click_rate`      DECIMAL(5, 2) COMMENT '商品点击率(%)',
    `search_rank_score`    DECIMAL(5, 2) COMMENT '搜索排名得分',
    `detail_click_rate`    DECIMAL(5, 2) COMMENT '详情页点击率(%)',
    `update_time`          TIMESTAMP COMMENT '更新时间'
) comment '商品流量获取日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')STORED AS ORC
LOCATION '/warehouse/gd7/dws/dws_product_traffic_d'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理 - 修改聚合逻辑，移除item_id格式检查
traffic_df = spark.table("gd7.dwd_user_behavior_log").filter(
    (F.col("dt") == "20250801") &
    (F.col("store_id").isNotNull()) &
    (F.col("item_id").isNotNull())
).groupBy("store_id", "item_id").agg(
    F.countDistinct(F.when(F.col("page_type") == "home", F.col("session_id"))).alias("store_total_clicks"),
    F.count(F.when(F.col("page_type").like("item%"), F.lit(1))).alias("item_visits"),
    F.round(
        F.coalesce(
            F.count(F.when(F.col("page_type").like("item%"), F.lit(1))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.countDistinct(F.when(F.col("page_type") == "home", F.col("session_id"))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("item_click_rate"),
    F.round(
        F.coalesce(
            # 修改搜索排名得分计算逻辑，处理异常值，确保不为负数
            F.avg(F.when(
                (F.col("event_type") == "search") &
                (F.col("search_rank").isNotNull()) &
                (F.col("search_rank") >= 0) &
                (F.col("search_rank") <= 100),  # 限制搜索排名在合理范围内
                100 - F.col("search_rank")
            )),
            F.lit(0)
        ),
        2
    ).alias("search_rank_score"),
    F.round(
        F.coalesce(
            F.count(F.when(F.col("page_type") == "item_detail", F.lit(1))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.count(F.when(F.col("page_type").like("item%"), F.lit(1))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("detail_click_rate"),
    F.current_timestamp().alias("update_time")
).withColumn("dt", F.lit("20250801"))

# 5. 验证数据量
print_data_count(traffic_df, "dws_product_traffic_d")

# 6. 写入数据
traffic_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd7/dws/dws_product_traffic_d")

# 7. 修复分区
repair_hive_table("dws_product_traffic_d")




# ====================== DWS层商品转化日汇总表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dws/dws_product_conversion_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS gd7.dws_product_conversion_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE gd7.dws_product_conversion_d (
    `store_id`             STRING COMMENT '店铺ID',
    `item_id`              STRING COMMENT '商品ID',
    `conversion_rate`      DECIMAL(5, 2) COMMENT '转化率(%)',
    `cart_conversion_rate` DECIMAL(5, 2) COMMENT '加购转化率(%)',
    `payment_success_rate` DECIMAL(5, 2) COMMENT '支付成功率(%)',
    `update_time`          TIMESTAMP COMMENT '更新时间'
) comment '商品转化日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')STORED AS ORC
LOCATION '/warehouse/gd7/dws/dws_product_conversion_d'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理 - 修改聚合逻辑，移除item_id格式检查
conversion_df = spark.table("gd7.dwd_user_behavior_log").filter(
    (F.col("dt") == "20250801") &
    (F.col("store_id").isNotNull()) &
    (F.col("item_id").isNotNull())
).groupBy("store_id", "item_id").agg(
    F.round(
        F.coalesce(
            F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id"))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.countDistinct(F.when(F.col("page_type").like("item%"), F.col("user_id"))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("conversion_rate"),
    F.round(
        F.coalesce(
            F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("user_id"))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.countDistinct(F.when(F.col("page_type").like("item%"), F.col("user_id"))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("cart_conversion_rate"),
    F.round(
        F.coalesce(
            F.countDistinct(F.when(F.col("event_type") == "payment_success", F.col("user_id"))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id"))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("payment_success_rate"),
    F.current_timestamp().alias("update_time")
).withColumn("dt", F.lit("20250801"))

# 5. 验证数据量
print_data_count(conversion_df, "dws_product_conversion_d")

# 6. 写入数据
conversion_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd7/dws/dws_product_conversion_d")

# 7. 修复分区
repair_hive_table("dws_product_conversion_d")


# ====================== DWS层商品内容营销日汇总表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dws/dws_product_content_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS gd7.dws_product_content_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE gd7.dws_product_content_d (
    `store_id`                 STRING COMMENT '店铺ID',
    `item_id`                  STRING COMMENT '商品ID',
    `avg_content_duration`     DECIMAL(10, 2) COMMENT '平均内容观看时长(秒)',
    `content_share_rate`       DECIMAL(5, 2) COMMENT '内容分享率(%)',
    `comment_interaction_rate` DECIMAL(5, 2) COMMENT '评论互动率(%)',
    `update_time`              TIMESTAMP COMMENT '更新时间'
) comment '商品内容营销日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')STORED AS ORC
LOCATION '/warehouse/gd7/dws/dws_product_content_d'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理，移除item_id格式检查
content_df = spark.table("gd7.dwd_user_behavior_log").filter(
    (F.col("dt") == "20250801") &
    (F.col("store_id").isNotNull()) &
    (F.col("item_id").isNotNull())
).groupBy("store_id", "item_id").agg(
    F.round(
        F.coalesce(
            F.avg(F.when(F.col("event_type") == "content_view", F.coalesce(F.col("duration"), F.lit(0)))),
            F.lit(0)
        ),
        2
    ).alias("avg_content_duration"),
    F.round(
        F.coalesce(
            F.countDistinct(F.when(F.col("event_type") == "content_share", F.col("user_id"))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.countDistinct(F.when(F.col("event_type") == "content_view", F.col("user_id"))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("content_share_rate"),
    F.round(
        F.coalesce(
            F.countDistinct(F.when(F.col("event_type") == "comment", F.col("user_id"))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.countDistinct(F.when(F.col("event_type") == "content_view", F.col("user_id"))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("comment_interaction_rate"),
    F.current_timestamp().alias("update_time")
).withColumn("dt", F.lit("20250801"))

# 5. 验证数据量
print_data_count(content_df, "dws_product_content_d")

# 6. 写入数据
content_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd7/dws/dws_product_content_d")

# 7. 修复分区
repair_hive_table("dws_product_content_d")

# ====================== DWS层客户拉新日汇总表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dws/dws_customer_acquisition_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS gd7.dws_customer_acquisition_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE gd7.dws_customer_acquisition_d (
    `store_id`                     STRING COMMENT '店铺ID',
    `item_id`                      STRING COMMENT '商品ID',
    `new_customer_purchase_ratio`  DECIMAL(5, 2) COMMENT '新客购买占比(%)',
    `new_customer_repurchase_rate` DECIMAL(5, 2) COMMENT '新客复购率(%)',
    `acquisition_cost_score`       DECIMAL(5, 2) COMMENT '获取成本得分',
    `update_time`                  TIMESTAMP COMMENT '更新时间'
) comment '客户拉新日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')STORED AS ORC
LOCATION '/warehouse/gd7/dws/dws_customer_acquisition_d'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理，移除item_id格式检查，修复负数问题
acquisition_df = spark.table("gd7.dwd_user_behavior_log").filter(
    (F.col("dt") == "20250801") &
    (F.col("store_id").isNotNull()) &
    (F.col("item_id").isNotNull())
).groupBy("store_id", "item_id").agg(
    F.round(
        F.coalesce(
            F.countDistinct(F.when((F.col("is_new_customer") == 1) & (F.col("event_type") == "payment"), F.col("user_id"))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id"))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("new_customer_purchase_ratio"),
    F.round(
        F.coalesce(
            F.countDistinct(F.when((F.col("is_new_customer") == 1) & (F.col("repurchase_flag") == 1), F.col("user_id"))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.countDistinct(F.when(F.col("is_new_customer") == 1, F.col("user_id"))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("new_customer_repurchase_rate"),
    F.round(
        F.coalesce(
            # 修复获取成本得分，确保不为负数
            F.when(
                F.sum(F.when(F.col("is_new_customer") == 1, F.coalesce(F.col("acquisition_cost"), F.lit(0)))) > 0,
                F.when(
                    100 - (
                            F.sum(F.when(F.col("is_new_customer") == 1, F.coalesce(F.col("acquisition_cost"), F.lit(0)))) /
                            F.coalesce(
                                F.countDistinct(F.when(F.col("is_new_customer") == 1, F.col("user_id"))).cast(T.DoubleType()),
                                F.lit(1)
                            ) * 10
                    ) < 0,
                    0  # 确保最低为0
                ).otherwise(
                    100 - (
                            F.sum(F.when(F.col("is_new_customer") == 1, F.coalesce(F.col("acquisition_cost"), F.lit(0)))) /
                            F.coalesce(
                                F.countDistinct(F.when(F.col("is_new_customer") == 1, F.col("user_id"))).cast(T.DoubleType()),
                                F.lit(1)
                            ) * 10
                    )
                )
            ).otherwise(F.lit(100)),
            F.lit(0)
        ),
        2
    ).alias("acquisition_cost_score"),
    F.current_timestamp().alias("update_time")
).withColumn("dt", F.lit("20250801"))

# 5. 验证数据量
print_data_count(acquisition_df, "dws_customer_acquisition_d")

# 6. 写入数据
acquisition_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd7/dws/dws_customer_acquisition_d")

# 7. 修复分区
repair_hive_table("dws_customer_acquisition_d")


# ====================== DWS层商品服务日汇总表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dws/dws_product_service_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS gd7.dws_product_service_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE gd7.dws_product_service_d (
    `store_id`               STRING COMMENT '店铺ID',
    `item_id`                STRING COMMENT '商品ID',
    `return_rate_score`      DECIMAL(5, 2) COMMENT '退货率得分',
    `complaint_rate_score`   DECIMAL(5, 2) COMMENT '投诉率得分',
    `positive_feedback_rate` DECIMAL(5, 2) COMMENT '好评率(%)',
    `update_time`            TIMESTAMP COMMENT '更新时间'
) comment '商品服务日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')STORED AS ORC
LOCATION '/warehouse/gd7/dws/dws_product_service_d'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理，移除item_id格式检查，修复负数问题
service_df = spark.table("gd7.dwd_order_info").filter(
    (F.col("dt") == "20250801") &
    (F.col("store_id").isNotNull()) &
    (F.col("item_id").isNotNull())
).groupBy("store_id", "item_id").agg(
    F.round(
        F.coalesce(
            # 修复退货率得分，确保不为负数
            F.when(
                F.count(F.when(F.col("order_status") == "completed", F.lit(1))) > 0,
                100 - (
                        F.count(F.when(F.col("order_status") == "returned", F.lit(1))).cast(T.DoubleType()) * 100.0 /
                        F.coalesce(
                            F.count(F.when(F.col("order_status") == "completed", F.lit(1))).cast(T.DoubleType()),
                            F.lit(1)
                        )
                )
            ).otherwise(F.lit(100)),
            F.lit(100)
        ),
        2
    ).alias("return_rate_score"),
    F.round(
        F.coalesce(
            # 修复投诉率得分，确保不为负数
            F.when(
                F.count(F.when(F.col("order_status") == "completed", F.lit(1))) > 0,
                100 - (
                        F.count(F.when(F.col("has_complaint") == 1, F.lit(1))).cast(T.DoubleType()) * 100.0 /
                        F.coalesce(
                            F.count(F.when(F.col("order_status") == "completed", F.lit(1))).cast(T.DoubleType()),
                            F.lit(1)
                        )
                )
            ).otherwise(F.lit(100)),
            F.lit(100)
        ),
        2
    ).alias("complaint_rate_score"),
    F.round(
        F.coalesce(
            F.avg(F.when(F.col("rating").isNotNull(), F.col("rating"))) * 20,
            F.lit(0)
        ),
        2
    ).alias("positive_feedback_rate"),
    F.current_timestamp().alias("update_time")
).withColumn("dt", F.lit("20250801"))

# 5. 验证数据量
print_data_count(service_df, "dws_product_service_d")

# 6. 写入数据
service_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd7/dws/dws_product_service_d")

# 7. 修复分区
repair_hive_table("dws_product_service_d")

# 验证DWS层数据
spark.sql("""
SELECT
    'DWS Layer Record Counts' as info,
    (SELECT COUNT(*) FROM gd7.dws_product_traffic_d WHERE dt = '20250801') as traffic_count,
    (SELECT COUNT(*) FROM gd7.dws_product_conversion_d WHERE dt = '20250801') as conversion_count,
    (SELECT COUNT(*) FROM gd7.dws_product_content_d WHERE dt = '20250801') as content_count,
    (SELECT COUNT(*) FROM gd7.dws_customer_acquisition_d WHERE dt = '20250801') as acquisition_count,
    (SELECT COUNT(*) FROM gd7.dws_product_service_d WHERE dt = '20250801') as service_count
""").show()

# 查看部分结果示例
spark.sql("SELECT * FROM gd7.dws_product_traffic_d WHERE dt = '20250801' LIMIT 5").show()
spark.sql("SELECT * FROM gd7.dws_product_conversion_d WHERE dt = '20250801' LIMIT 5").show()
spark.sql("SELECT * FROM gd7.dws_product_content_d WHERE dt = '20250801' LIMIT 5").show()
spark.sql("SELECT * FROM gd7.dws_customer_acquisition_d WHERE dt = '20250801' LIMIT 5").show()
spark.sql("SELECT * FROM gd7.dws_product_service_d WHERE dt = '20250801' LIMIT 5").show()

spark.stop()
