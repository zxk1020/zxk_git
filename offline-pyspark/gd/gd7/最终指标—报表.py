# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("GD7 ADS Metrics") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "E:/spark_temp") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("WARN")

# 设置当前数据库
spark.sql("USE gd7")

# 设置分区日期
dt = '20250801'
print(f"开始处理GD7 ADS指标数据，日期分区: {dt}")

# 工具函数定义
def create_hdfs_dir(path):
    """创建HDFS目录"""
    from py4j.java_gateway import java_import
    java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
    java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
    fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
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

# 检查源表是否存在
print("检查源表是否存在:")
try:
    # 检查行为日志表
    behavior_count = spark.sql(f"SELECT COUNT(*) as count FROM gd7.dwd_user_behavior_log WHERE dt = '{dt}'").collect()[0]['count']
    print(f"dwd_user_behavior_log表记录数: {behavior_count}")
    # 检查订单信息表
    order_count = spark.sql(f"SELECT COUNT(*) as count FROM gd7.dwd_order_info WHERE dt = '{dt}'").collect()[0]['count']
    print(f"dwd_order_info表记录数: {order_count}")
    if behavior_count == 0 and order_count == 0:
        print("警告: 源表没有数据，请检查DWD层数据是否已正确生成")
        spark.stop()
        exit(1)
except Exception as e:
    print(f"检查源表时出错: {e}")
    # 列出数据库中的所有表
    print("当前数据库中的表:")
    spark.sql("SHOW TABLES").show()
    spark.stop()
    exit(1)

# ====================== 流量获取维度指标结果表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/ads/ads_traffic_metrics_result")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_traffic_metrics_result")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE ads_traffic_metrics_result (
    `store_id`           STRING COMMENT '店铺ID',
    `item_id`            STRING COMMENT '商品ID',
    `store_total_clicks` BIGINT COMMENT '店铺总点击数',
    `item_visits`        BIGINT COMMENT '商品访问次数',
    `item_click_rate`    DOUBLE COMMENT '商品点击率(%)',
    `search_rank_score`  DOUBLE COMMENT '搜索排名得分',
    `detail_click_rate`  DOUBLE COMMENT '详情页点击率(%)',
    `update_time`        TIMESTAMP COMMENT '更新时间'
) COMMENT '流量获取指标结果表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/ads/ads_traffic_metrics_result'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理流量获取维度指标结果表数据...")
# 读取行为日志数据(使用完整表名)
behavior_log = spark.sql(f"SELECT * FROM gd7.dwd_user_behavior_log WHERE dt = '{dt}'")

# 计算流量获取指标（修复search_rank_score类型）
traffic_metrics = behavior_log.groupBy("store_id", "item_id").agg(
    F.countDistinct(F.when(F.col("page_type") == "home", F.col("session_id"))).alias("store_total_clicks"),
    F.count(F.when(F.col("page_type").like("item%"), F.lit(1))).alias("item_visits"),
    F.round(
        F.count(F.when(F.col("page_type").like("item%"), F.lit(1))) * 100.0 /
        F.when(
            F.countDistinct(F.when(F.col("page_type") == "home", F.col("session_id"))) != 0,
            F.countDistinct(F.when(F.col("page_type") == "home", F.col("session_id")))
        ).otherwise(1),
        2
    ).cast("double").alias("item_click_rate"),
    # 关键修复：将计算结果显式转换为double类型
    F.max(F.when(F.col("event_type") == "search", 100 - F.col("search_rank")).otherwise(0))
    .cast("double").alias("search_rank_score"),
    F.round(
        F.count(F.when(F.col("page_type") == "item_detail", F.lit(1))) * 100.0 /
        F.when(
            F.count(F.when(F.col("page_type").like("item%"), F.lit(1))) != 0,
            F.count(F.when(F.col("page_type").like("item%"), F.lit(1)))
        ).otherwise(1),
        2
    ).cast("double").alias("detail_click_rate"),
    F.current_timestamp().alias("update_time"),
    F.lit(dt).alias("dt")
)

# 验证数据量
print_data_count(traffic_metrics, "ads_traffic_metrics_result")

# 写入数据
traffic_metrics.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/ads/ads_traffic_metrics_result")

# 修复分区
repair_hive_table("ads_traffic_metrics_result")

# ====================== 转化维度指标结果表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/ads/ads_conversion_metrics_result")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_conversion_metrics_result")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE ads_conversion_metrics_result (
    `store_id`             STRING COMMENT '店铺ID',
    `item_id`              STRING COMMENT '商品ID',
    `conversion_rate`      DOUBLE COMMENT '转化率(%)',
    `cart_conversion_rate` DOUBLE COMMENT '加购转化率(%)',
    `payment_success_rate` DOUBLE COMMENT '支付成功率(%)',
    `update_time`          TIMESTAMP COMMENT '更新时间'
) COMMENT '转化指标结果表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/ads/ads_conversion_metrics_result'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理转化维度指标结果表数据...")
# 计算转化指标
conversion_metrics = behavior_log.groupBy("store_id", "item_id").agg(
    F.round(
        F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id"))) * 100.0 /
        F.when(
            F.countDistinct(F.when(F.col("page_type") == "item_detail", F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("page_type") == "item_detail", F.col("user_id")))
        ).otherwise(1),
        2
    ).cast("double").alias("conversion_rate"),
    F.round(
        F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("user_id"))) * 100.0 /
        F.when(
            F.countDistinct(F.when(F.col("page_type") == "item_detail", F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("page_type") == "item_detail", F.col("user_id")))
        ).otherwise(1),
        2
    ).cast("double").alias("cart_conversion_rate"),
    F.round(
        F.countDistinct(F.when(F.col("event_type") == "payment_success", F.col("user_id"))) * 100.0 /
        F.when(
            F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id")))
        ).otherwise(1),
        2
    ).cast("double").alias("payment_success_rate"),
    F.current_timestamp().alias("update_time"),
    F.lit(dt).alias("dt")
)

# 验证数据量
print_data_count(conversion_metrics, "ads_conversion_metrics_result")

# 写入数据
conversion_metrics.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/ads/ads_conversion_metrics_result")

# 修复分区
repair_hive_table("ads_conversion_metrics_result")

# ====================== 内容营销维度指标结果表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/ads/ads_content_metrics_result")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_content_metrics_result")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE ads_content_metrics_result (
    `store_id`                 STRING COMMENT '店铺ID',
    `item_id`                  STRING COMMENT '商品ID',
    `avg_content_duration`     DOUBLE COMMENT '平均内容观看时长(秒)',
    `content_share_rate`       DOUBLE COMMENT '内容分享率(%)',
    `comment_interaction_rate` DOUBLE COMMENT '评论互动率(%)',
    `update_time`              TIMESTAMP COMMENT '更新时间'
) COMMENT '内容营销指标结果表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/ads/ads_content_metrics_result'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理内容营销维度指标结果表数据...")
# 计算内容营销指标
content_metrics = behavior_log.groupBy("store_id", "item_id").agg(
    F.round(
        F.avg(F.when(F.col("event_type") == "content_view", F.col("duration")).otherwise(0)),
        2
    ).cast("double").alias("avg_content_duration"),
    F.round(
        F.countDistinct(F.when(F.col("event_type") == "content_share", F.col("user_id"))) * 100.0 /
        F.when(
            F.countDistinct(F.when(F.col("event_type") == "content_view", F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("event_type") == "content_view", F.col("user_id")))
        ).otherwise(1),
        2
    ).cast("double").alias("content_share_rate"),
    F.round(
        F.countDistinct(F.when(F.col("event_type") == "comment", F.col("user_id"))) * 100.0 /
        F.when(
            F.countDistinct(F.when(F.col("event_type") == "content_view", F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("event_type") == "content_view", F.col("user_id")))
        ).otherwise(1),
        2
    ).cast("double").alias("comment_interaction_rate"),
    F.current_timestamp().alias("update_time"),
    F.lit(dt).alias("dt")
)

# 验证数据量
print_data_count(content_metrics, "ads_content_metrics_result")

# 写入数据
content_metrics.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/ads/ads_content_metrics_result")

# 修复分区
repair_hive_table("ads_content_metrics_result")

# ====================== 客户拉新维度指标结果表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/ads/ads_acquisition_metrics_result")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_acquisition_metrics_result")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE ads_acquisition_metrics_result (
    `store_id`                     STRING COMMENT '店铺ID',
    `item_id`                      STRING COMMENT '商品ID',
    `new_customer_purchase_ratio`  DOUBLE COMMENT '新客购买占比(%)',
    `new_customer_repurchase_rate` DOUBLE COMMENT '新客复购率(%)',
    `acquisition_cost_score`       DOUBLE COMMENT '获取成本得分',
    `update_time`                  TIMESTAMP COMMENT '更新时间'
) COMMENT '客户拉新指标结果表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/ads/ads_acquisition_metrics_result'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理客户拉新维度指标结果表数据...")
# 计算客户拉新指标
acquisition_metrics = behavior_log.groupBy("store_id", "item_id").agg(
    F.round(
        F.countDistinct(F.when((F.col("is_new_customer") == 1) & (F.col("event_type") == "payment"), F.col("user_id"))) * 100.0 /
        F.when(
            F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id")))
        ).otherwise(1),
        2
    ).cast("double").alias("new_customer_purchase_ratio"),
    F.round(
        F.countDistinct(F.when((F.col("is_new_customer") == 1) & (F.col("repurchase_flag") == 1), F.col("user_id"))) * 100.0 /
        F.when(
            F.countDistinct(F.when(F.col("is_new_customer") == 1, F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("is_new_customer") == 1, F.col("user_id")))
        ).otherwise(1),
        2
    ).cast("double").alias("new_customer_repurchase_rate"),
    F.round(
        F.when(
            F.sum(F.when(F.col("is_new_customer") == 1, F.col("acquisition_cost")).otherwise(0)) > 0,
            100 - (
                    F.sum(F.when(F.col("is_new_customer") == 1, F.col("acquisition_cost")).otherwise(0)) /
                    F.when(
                        F.countDistinct(F.when(F.col("is_new_customer") == 1, F.col("user_id"))) != 0,
                        F.countDistinct(F.when(F.col("is_new_customer") == 1, F.col("user_id")))
                    ).otherwise(1)
            ) * 10
        ).otherwise(F.lit(100)),
        2
    ).cast("double").alias("acquisition_cost_score"),
    F.current_timestamp().alias("update_time"),
    F.lit(dt).alias("dt")
)

# 验证数据量
print_data_count(acquisition_metrics, "ads_acquisition_metrics_result")

# 写入数据
acquisition_metrics.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/ads/ads_acquisition_metrics_result")

# 修复分区
repair_hive_table("ads_acquisition_metrics_result")

# ====================== 服务质量维度指标结果表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/ads/ads_service_metrics_result")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_service_metrics_result")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE ads_service_metrics_result (
    `store_id`               STRING COMMENT '店铺ID',
    `item_id`                STRING COMMENT '商品ID',
    `return_rate_score`      DOUBLE COMMENT '退货率得分',
    `complaint_rate_score`   DOUBLE COMMENT '投诉率得分',
    `positive_feedback_rate` DOUBLE COMMENT '好评率(%)',
    `update_time`            TIMESTAMP COMMENT '更新时间'
) COMMENT '服务质量指标结果表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/ads/ads_service_metrics_result'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理服务质量维度指标结果表数据...")
# 读取订单信息数据(使用完整表名)
order_info = spark.sql(f"SELECT * FROM gd7.dwd_order_info WHERE dt = '{dt}'")

# 计算服务质量指标
service_metrics = order_info.groupBy("store_id", "item_id").agg(
    F.round(
        F.when(
            F.count(F.when(F.col("order_status") == "completed", F.lit(1))) > 0,
            100 - (
                    F.count(F.when(F.col("order_status") == "returned", F.lit(1))) * 100.0 /
                    F.when(
                        F.count(F.when(F.col("order_status") == "completed", F.lit(1))) != 0,
                        F.count(F.when(F.col("order_status") == "completed", F.lit(1)))
                    ).otherwise(1)
            )
        ).otherwise(F.lit(100)),
        2
    ).cast("double").alias("return_rate_score"),
    F.round(
        F.when(
            F.count(F.when(F.col("order_status") == "completed", F.lit(1))) > 0,
            100 - (
                    F.count(F.when(F.col("has_complaint") == 1, F.lit(1))) * 100.0 /
                    F.when(
                        F.count(F.when(F.col("order_status") == "completed", F.lit(1))) != 0,
                        F.count(F.when(F.col("order_status") == "completed", F.lit(1)))
                    ).otherwise(1)
            )
        ).otherwise(F.lit(100)),
        2
    ).cast("double").alias("complaint_rate_score"),
    F.round(
        F.avg(F.when(F.col("rating").isNotNull(), F.col("rating")).otherwise(0)) * 20,
        2
    ).cast("double").alias("positive_feedback_rate"),
    F.current_timestamp().alias("update_time"),
    F.lit(dt).alias("dt")
)

# 验证数据量
print_data_count(service_metrics, "ads_service_metrics_result")

# 写入数据
service_metrics.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/ads/ads_service_metrics_result")

# 修复分区
repair_hive_table("ads_service_metrics_result")

# ====================== 综合评分指标结果表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/ads/ads_comprehensive_score_result")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_comprehensive_score_result")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE ads_comprehensive_score_result (
    `store_id`          STRING COMMENT '店铺ID',
    `item_id`           STRING COMMENT '商品ID',
    `traffic_score`     DOUBLE COMMENT '流量获取得分(0-100)',
    `conversion_score`  DOUBLE COMMENT '转化得分(0-100)',
    `content_score`     DOUBLE COMMENT '内容运营得分(0-100)',
    `acquisition_score` DOUBLE COMMENT '客户获取得分(0-100)',
    `service_score`     DOUBLE COMMENT '服务得分(0-100)',
    `total_score`       DOUBLE COMMENT '总分(0-100)',
    `grade`             STRING COMMENT '等级(A/B/C/D)',
    `update_time`       TIMESTAMP COMMENT '更新时间'
) COMMENT '综合评分指标结果表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/ads/ads_comprehensive_score_result'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理综合评分指标结果表数据...")
# 读取各维度指标数据
traffic_df = spark.sql(f"SELECT * FROM gd7.ads_traffic_metrics_result WHERE dt = '{dt}'")
conversion_df = spark.sql(f"SELECT * FROM gd7.ads_conversion_metrics_result WHERE dt = '{dt}'")
content_df = spark.sql(f"SELECT * FROM gd7.ads_content_metrics_result WHERE dt = '{dt}'")
acquisition_df = spark.sql(f"SELECT * FROM gd7.ads_acquisition_metrics_result WHERE dt = '{dt}'")
service_df = spark.sql(f"SELECT * FROM gd7.ads_service_metrics_result WHERE dt = '{dt}'")

# 合并各维度指标数据
combined_metrics = traffic_df.alias("t1") \
    .join(conversion_df.alias("t2"),
          (F.col("t1.store_id") == F.col("t2.store_id")) &
          (F.col("t1.item_id") == F.col("t2.item_id")),
          "left") \
    .join(content_df.alias("t3"),
          (F.col("t1.store_id") == F.col("t3.store_id")) &
          (F.col("t1.item_id") == F.col("t3.item_id")),
          "left") \
    .join(acquisition_df.alias("t4"),
          (F.col("t1.store_id") == F.col("t4.store_id")) &
          (F.col("t1.item_id") == F.col("t4.item_id")),
          "left") \
    .join(service_df.alias("t5"),
          (F.col("t1.store_id") == F.col("t5.store_id")) &
          (F.col("t1.item_id") == F.col("t5.item_id")),
          "left") \
    .select(
    F.col("t1.store_id").alias("store_id"),
    F.col("t1.item_id").alias("item_id"),
    F.col("t1.item_click_rate").alias("item_click_rate"),
    F.col("t1.search_rank_score").alias("search_rank_score"),
    F.col("t1.detail_click_rate").alias("detail_click_rate"),
    F.col("t2.conversion_rate").alias("conversion_rate"),
    F.col("t2.cart_conversion_rate").alias("cart_conversion_rate"),
    F.col("t2.payment_success_rate").alias("payment_success_rate"),
    F.col("t3.avg_content_duration").alias("avg_content_duration"),
    F.col("t3.content_share_rate").alias("content_share_rate"),
    F.col("t3.comment_interaction_rate").alias("comment_interaction_rate"),
    F.col("t4.new_customer_purchase_ratio").alias("new_customer_purchase_ratio"),
    F.col("t4.new_customer_repurchase_rate").alias("new_customer_repurchase_rate"),
    F.col("t4.acquisition_cost_score").alias("acquisition_cost_score"),
    F.col("t5.return_rate_score").alias("return_rate_score"),
    F.col("t5.complaint_rate_score").alias("complaint_rate_score"),
    F.col("t5.positive_feedback_rate").alias("positive_feedback_rate")
)

# 计算综合评分
comprehensive_score = combined_metrics.select(
    F.col("store_id"),
    F.col("item_id"),
    # 流量获取得分
    F.when(
        (F.col("item_click_rate") > 0) | (F.col("search_rank_score") > 0) | (F.col("detail_click_rate") > 0),
        F.round(
            F.least(F.coalesce(F.col("item_click_rate"), F.lit(0)) * 2, F.lit(100)) * 0.4 +  # 点击率权重
            F.least(F.coalesce(F.col("search_rank_score"), F.lit(0)), F.lit(100)) * 0.3 +  # 搜索排名权重
            F.least(F.coalesce(F.col("detail_click_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3,  # 详情页点击权重
            2
        )
    ).otherwise(F.lit(20)).cast("double").alias("traffic_score"),  # 无数据给基础分20

    # 转化得分
    F.when(
        (F.col("conversion_rate") > 0) | (F.col("cart_conversion_rate") > 0) | (F.col("payment_success_rate") > 0),
        F.round(
            F.least(F.coalesce(F.col("conversion_rate"), F.lit(0)) * 2, F.lit(100)) * 0.5 +
            F.least(F.coalesce(F.col("cart_conversion_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3 +
            F.least(F.coalesce(F.col("payment_success_rate"), F.lit(0)) * 2, F.lit(100)) * 0.2,
            2
        )
    ).otherwise(F.lit(20)).cast("double").alias("conversion_score"),  # 无数据给基础分20

    # 内容运营得分
    F.when(
        (F.col("avg_content_duration") > 0) | (F.col("content_share_rate") > 0) | (F.col("comment_interaction_rate") > 0),
        F.round(
            F.least(F.least(F.coalesce(F.col("avg_content_duration"), F.lit(0)), F.lit(300)) / 3 * 2, F.lit(100)) * 0.4 +
            F.least(F.coalesce(F.col("content_share_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3 +
            F.least(F.coalesce(F.col("comment_interaction_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3,
            2
        )
    ).otherwise(F.lit(20)).cast("double").alias("content_score"),  # 无数据给基础分20

    # 客户获取得分
    F.when(
        (F.col("new_customer_purchase_ratio") > 0) | (F.col("new_customer_repurchase_rate") > 0) | (F.col("acquisition_cost_score") > 0),
        F.round(
            F.least(F.coalesce(F.col("new_customer_purchase_ratio"), F.lit(0)) * 2, F.lit(100)) * 0.4 +
            F.least(F.coalesce(F.col("new_customer_repurchase_rate"), F.lit(0)) * 2, F.lit(100)) * 0.4 +
            F.least(F.coalesce(F.col("acquisition_cost_score"), F.lit(0)), F.lit(100)) * 0.2,
            2
        )
    ).otherwise(F.lit(20)).cast("double").alias("acquisition_score"),  # 无数据给基础分20

    # 服务得分
    F.when(
        (F.col("return_rate_score") > 0) | (F.col("complaint_rate_score") > 0) | (F.col("positive_feedback_rate") > 0),
        F.round(
            F.least(F.coalesce(F.col("return_rate_score"), F.lit(0)), F.lit(100)) * 0.3 +
            F.least(F.coalesce(F.col("complaint_rate_score"), F.lit(0)), F.lit(100)) * 0.2 +
            F.least(F.coalesce(F.col("positive_feedback_rate"), F.lit(0)), F.lit(100)) * 0.5,
            2
        )
    ).otherwise(F.lit(20)).cast("double").alias("service_score"),  # 无数据给基础分20

    # 总分
    F.round(
        F.coalesce(
            F.when(
                (F.col("item_click_rate") > 0) | (F.col("search_rank_score") > 0) | (F.col("detail_click_rate") > 0),
                F.least(F.coalesce(F.col("item_click_rate"), F.lit(0)) * 2, F.lit(100)) * 0.4 +
                F.least(F.coalesce(F.col("search_rank_score"), F.lit(0)), F.lit(100)) * 0.3 +
                F.least(F.coalesce(F.col("detail_click_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3
            ).otherwise(F.lit(20)),
            F.lit(20)
        ) * 0.25 +
        F.coalesce(
            F.when(
                (F.col("conversion_rate") > 0) | (F.col("cart_conversion_rate") > 0) | (F.col("payment_success_rate") > 0),
                F.least(F.coalesce(F.col("conversion_rate"), F.lit(0)) * 2, F.lit(100)) * 0.5 +
                F.least(F.coalesce(F.col("cart_conversion_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3 +
                F.least(F.coalesce(F.col("payment_success_rate"), F.lit(0)) * 2, F.lit(100)) * 0.2
            ).otherwise(F.lit(20)),
            F.lit(20)
        ) * 0.25 +
        F.coalesce(
            F.when(
                (F.col("avg_content_duration") > 0) | (F.col("content_share_rate") > 0) | (F.col("comment_interaction_rate") > 0),
                F.least(F.least(F.coalesce(F.col("avg_content_duration"), F.lit(0)), F.lit(300)) / 3 * 2, F.lit(100)) * 0.4 +
                F.least(F.coalesce(F.col("content_share_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3 +
                F.least(F.coalesce(F.col("comment_interaction_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3
            ).otherwise(F.lit(20)),
            F.lit(20)
        ) * 0.15 +
        F.coalesce(
            F.when(
                (F.col("new_customer_purchase_ratio") > 0) | (F.col("new_customer_repurchase_rate") > 0) | (F.col("acquisition_cost_score") > 0),
                F.least(F.coalesce(F.col("new_customer_purchase_ratio"), F.lit(0)) * 2, F.lit(100)) * 0.4 +
                F.least(F.coalesce(F.col("new_customer_repurchase_rate"), F.lit(0)) * 2, F.lit(100)) * 0.4 +
                F.least(F.coalesce(F.col("acquisition_cost_score"), F.lit(0)), F.lit(100)) * 0.2
            ).otherwise(F.lit(20)),
            F.lit(20)
        ) * 0.2 +
        F.coalesce(
            F.when(
                (F.col("return_rate_score") > 0) | (F.col("complaint_rate_score") > 0) | (F.col("positive_feedback_rate") > 0),
                F.least(F.coalesce(F.col("return_rate_score"), F.lit(0)), F.lit(100)) * 0.3 +
                F.least(F.coalesce(F.col("complaint_rate_score"), F.lit(0)), F.lit(100)) * 0.2 +
                F.least(F.coalesce(F.col("positive_feedback_rate"), F.lit(0)), F.lit(100)) * 0.5
            ).otherwise(F.lit(20)),
            F.lit(20)
        ) * 0.15,
        2
    ).cast("double").alias("total_score"),
    F.lit(dt).alias("dt")
)

# 添加等级字段和更新时间
comprehensive_score_with_grade = comprehensive_score.withColumn(
    "grade",
    F.when(F.col("total_score") >= 85, "A")
    .when(F.col("total_score") >= 70, "B")
    .when(F.col("total_score") >= 50, "C")
    .otherwise("D")
).withColumn(
    "update_time",
    F.current_timestamp()
)

# 验证数据量
print_data_count(comprehensive_score_with_grade, "ads_comprehensive_score_result")

# 写入数据
comprehensive_score_with_grade.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/ads/ads_comprehensive_score_result")

# 修复分区
repair_hive_table("ads_comprehensive_score_result")

# ====================== 竞品对比指标结果表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/ads/ads_competitor_comparison_result")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_competitor_comparison_result")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE ads_competitor_comparison_result (
    `store_id`                   STRING COMMENT '店铺ID',
    `item_id`                    STRING COMMENT '商品ID',
    `competitor_avg_traffic`     DOUBLE COMMENT '竞品平均流量得分',
    `competitor_avg_conversion`  DOUBLE COMMENT '竞品平均转化得分',
    `competitor_avg_content`     DOUBLE COMMENT '竞品平均内容得分',
    `competitor_avg_acquisition` DOUBLE COMMENT '竞品平均拉新得分',
    `competitor_avg_service`     DOUBLE COMMENT '竞品平均服务得分',
    `competitor_avg_total`       DOUBLE COMMENT '竞品平均总分',
    `traffic_gap`                DOUBLE COMMENT '流量获取分差(本品-竞品)',
    `conversion_gap`             DOUBLE COMMENT '转化分差(本品-竞品)',
    `content_gap`                DOUBLE COMMENT '内容分差(本品-竞品)',
    `acquisition_gap`            DOUBLE COMMENT '拉新分差(本品-竞品)',
    `service_gap`                DOUBLE COMMENT '服务分差(本品-竞品)',
    `total_gap`                  DOUBLE COMMENT '总分差(本品-竞品)',
    `update_time`                TIMESTAMP COMMENT '更新时间'
) COMMENT '竞品对比指标结果表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/ads/ads_competitor_comparison_result'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理竞品对比指标结果表数据...")
# 读取综合评分数据
comprehensive_scores = spark.sql(f"SELECT * FROM gd7.ads_comprehensive_score_result WHERE dt = '{dt}'")

# 使用窗口函数计算各店铺内其他商品的平均得分
from pyspark.sql.window import Window

# 计算每个店铺各维度的平均值
window_spec = Window.partitionBy("store_id")
competitor_avg = comprehensive_scores.select(
    F.col("store_id"),
    F.col("item_id"),
    F.col("traffic_score"),
    F.col("conversion_score"),
    F.col("content_score"),
    F.col("acquisition_score"),
    F.col("service_score"),
    F.col("total_score"),
    # 计算竞品平均分
    F.avg("traffic_score").over(window_spec).alias("avg_traffic_score"),
    F.avg("conversion_score").over(window_spec).alias("avg_conversion_score"),
    F.avg("content_score").over(window_spec).alias("avg_content_score"),
    F.avg("acquisition_score").over(window_spec).alias("avg_acquisition_score"),
    F.avg("service_score").over(window_spec).alias("avg_service_score"),
    F.avg("total_score").over(window_spec).alias("avg_total_score")
)

# 计算与竞品的差值
competitor_comparison = competitor_avg.select(
    F.col("store_id"),
    F.col("item_id"),
    F.round(F.col("avg_traffic_score"), 2).cast("double").alias("competitor_avg_traffic"),
    F.round(F.col("avg_conversion_score"), 2).cast("double").alias("competitor_avg_conversion"),
    F.round(F.col("avg_content_score"), 2).cast("double").alias("competitor_avg_content"),
    F.round(F.col("avg_acquisition_score"), 2).cast("double").alias("competitor_avg_acquisition"),
    F.round(F.col("avg_service_score"), 2).cast("double").alias("competitor_avg_service"),
    F.round(F.col("avg_total_score"), 2).cast("double").alias("competitor_avg_total"),
    # 计算差值
    F.round(F.col("traffic_score") - F.col("avg_traffic_score"), 2).cast("double").alias("traffic_gap"),
    F.round(F.col("conversion_score") - F.col("avg_conversion_score"), 2).cast("double").alias("conversion_gap"),
    F.round(F.col("content_score") - F.col("avg_content_score"), 2).cast("double").alias("content_gap"),
    F.round(F.col("acquisition_score") - F.col("avg_acquisition_score"), 2).cast("double").alias("acquisition_gap"),
    F.round(F.col("service_score") - F.col("avg_service_score"), 2).cast("double").alias("service_gap"),
    F.round(F.col("total_score") - F.col("avg_total_score"), 2).cast("double").alias("total_gap"),
    F.current_timestamp().alias("update_time"),
    F.lit(dt).alias("dt")
)

# 验证数据量
print_data_count(competitor_comparison, "ads_competitor_comparison_result")

# 写入数据
competitor_comparison.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/ads/ads_competitor_comparison_result")

# 修复分区
repair_hive_table("ads_competitor_comparison_result")

# 验证最终结果
print("验证最终结果:")
try:
    traffic_count = spark.sql(f"SELECT COUNT(*) as count FROM gd7.ads_traffic_metrics_result WHERE dt = '{dt}'").collect()[0]['count']
    conversion_count = spark.sql(f"SELECT COUNT(*) as count FROM gd7.ads_conversion_metrics_result WHERE dt = '{dt}'").collect()[0]['count']
    content_count = spark.sql(f"SELECT COUNT(*) as count FROM gd7.ads_content_metrics_result WHERE dt = '{dt}'").collect()[0]['count']
    acquisition_count = spark.sql(f"SELECT COUNT(*) as count FROM gd7.ads_acquisition_metrics_result WHERE dt = '{dt}'").collect()[0]['count']
    service_count = spark.sql(f"SELECT COUNT(*) as count FROM gd7.ads_service_metrics_result WHERE dt = '{dt}'").collect()[0]['count']
    comprehensive_count = spark.sql(f"SELECT COUNT(*) as count FROM gd7.ads_comprehensive_score_result WHERE dt = '{dt}'").collect()[0]['count']
    competitor_count = spark.sql(f"SELECT COUNT(*) as count FROM gd7.ads_competitor_comparison_result WHERE dt = '{dt}'").collect()[0]['count']
    print(f"流量获取指标表记录数: {traffic_count}")
    print(f"转化指标表记录数: {conversion_count}")
    print(f"内容营销指标表记录数: {content_count}")
    print(f"客户拉新指标表记录数: {acquisition_count}")
    print(f"服务质量指标表记录数: {service_count}")
    print(f"综合评分指标表记录数: {comprehensive_count}")
    print(f"竞品对比指标表记录数: {competitor_count}")
except Exception as e:
    print(f"验证结果时出错: {e}")

print("GD7 ADS指标数据处理完成!")
spark.stop()
