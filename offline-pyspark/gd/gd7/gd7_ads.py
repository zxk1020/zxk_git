# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("GD7 ADS Layer") \
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

print(f"开始处理GD7 ADS层数据，日期分区: {dt}")

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

# 检查DWS层数据
print("检查DWS层数据:")
try:
    traffic_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_product_traffic_d WHERE dt = '{dt}'").collect()[0]['count']
    conversion_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_product_conversion_d WHERE dt = '{dt}'").collect()[0]['count']
    content_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_product_content_d WHERE dt = '{dt}'").collect()[0]['count']
    acquisition_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_customer_acquisition_d WHERE dt = '{dt}'").collect()[0]['count']
    service_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_product_service_d WHERE dt = '{dt}'").collect()[0]['count']

    print(f"Traffic表记录数: {traffic_count}")
    print(f"Conversion表记录数: {conversion_count}")
    print(f"Content表记录数: {content_count}")
    print(f"Acquisition表记录数: {acquisition_count}")
    print(f"Service表记录数: {service_count}")

    if traffic_count == 0:
        print("警告: Traffic表没有数据，请检查DWS层数据是否已正确生成")
        spark.stop()
        exit(1)

except Exception as e:
    print(f"检查DWS层数据时出错: {e}")
    spark.stop()
    exit(1)

# ====================== 商品评分主表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/ads/ads_product_score_main")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_product_score_main")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE ads_product_score_main
(
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
) COMMENT '商品评分主表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/ads/ads_product_score_main'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理商品评分主表数据...")

# 读取DWS层数据并填充空值
dws_traffic = spark.sql(f"SELECT * FROM dws_product_traffic_d WHERE dt = '{dt}'").fillna(0)
dws_conversion = spark.sql(f"SELECT * FROM dws_product_conversion_d WHERE dt = '{dt}'").fillna(0)
dws_content = spark.sql(f"SELECT * FROM dws_product_content_d WHERE dt = '{dt}'").fillna(0)
dws_acquisition = spark.sql(f"SELECT * FROM dws_customer_acquisition_d WHERE dt = '{dt}'").fillna(0)
dws_service = spark.sql(f"SELECT * FROM dws_product_service_d WHERE dt = '{dt}'").fillna(0)

# 合并所有维度数据
combined_data = dws_traffic.alias("t1") \
    .join(dws_conversion.alias("t2"),
          (F.col("t1.store_id") == F.col("t2.store_id")) &
          (F.col("t1.item_id") == F.col("t2.item_id")),
          "left") \
    .join(dws_content.alias("t3"),
          (F.col("t1.store_id") == F.col("t3.store_id")) &
          (F.col("t1.item_id") == F.col("t3.item_id")),
          "left") \
    .join(dws_acquisition.alias("t4"),
          (F.col("t1.store_id") == F.col("t4.store_id")) &
          (F.col("t1.item_id") == F.col("t4.item_id")),
          "left") \
    .join(dws_service.alias("t5"),
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
).fillna(0)

# 显示部分原始数据以检查
print("原始指标数据样本:")
combined_data.show(5)

# 计算各项得分（改进版 - 标准化得分，确保无负数，为0的默认改为20）
ads_scores = combined_data.select(
    F.col("store_id"),
    F.col("item_id"),
    # 流量获取得分 (0-100) - 标准化处理
    F.when(
        (F.col("item_click_rate") > 0) | (F.col("search_rank_score") > 0) | (F.col("detail_click_rate") > 0),
        F.round(
            F.least(F.coalesce(F.col("item_click_rate"), F.lit(0)) * 2, F.lit(100)) * 0.4 +  # 点击率权重
            F.least(F.coalesce(F.col("search_rank_score"), F.lit(0)), F.lit(100)) * 0.3 +  # 搜索排名权重
            F.least(F.coalesce(F.col("detail_click_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3,  # 详情页点击权重
            2
        )
    ).otherwise(F.lit(20)).cast("double").alias("traffic_score"),  # 无数据给基础分20

    # 转化得分 (0-100) - 标准化处理
    F.when(
        (F.col("conversion_rate") > 0) | (F.col("cart_conversion_rate") > 0) | (F.col("payment_success_rate") > 0),
        F.round(
            F.least(F.coalesce(F.col("conversion_rate"), F.lit(0)) * 2, F.lit(100)) * 0.5 +
            F.least(F.coalesce(F.col("cart_conversion_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3 +
            F.least(F.coalesce(F.col("payment_success_rate"), F.lit(0)) * 2, F.lit(100)) * 0.2,
            2
        )
    ).otherwise(F.lit(20)).cast("double").alias("conversion_score"),  # 无数据给基础分20

    # 内容运营得分 (0-100) - 标准化处理
    F.when(
        (F.col("avg_content_duration") > 0) | (F.col("content_share_rate") > 0) | (F.col("comment_interaction_rate") > 0),
        F.round(
            F.least(F.least(F.coalesce(F.col("avg_content_duration"), F.lit(0)), F.lit(300)) / 3 * 2, F.lit(100)) * 0.4 +
            F.least(F.coalesce(F.col("content_share_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3 +
            F.least(F.coalesce(F.col("comment_interaction_rate"), F.lit(0)) * 2, F.lit(100)) * 0.3,
            2
        )
    ).otherwise(F.lit(20)).cast("double").alias("content_score"),  # 无数据给基础分20

    # 客户获取得分 (0-100) - 标准化处理
    F.when(
        (F.col("new_customer_purchase_ratio") > 0) | (F.col("new_customer_repurchase_rate") > 0) | (F.col("acquisition_cost_score") > 0),
        F.round(
            F.least(F.coalesce(F.col("new_customer_purchase_ratio"), F.lit(0)) * 2, F.lit(100)) * 0.4 +
            F.least(F.coalesce(F.col("new_customer_repurchase_rate"), F.lit(0)) * 2, F.lit(100)) * 0.4 +
            F.least(F.coalesce(F.col("acquisition_cost_score"), F.lit(0)), F.lit(100)) * 0.2,
            2
        )
    ).otherwise(F.lit(20)).cast("double").alias("acquisition_score"),  # 无数据给基础分20

    # 服务得分 (0-100) - 标准化处理
    F.when(
        (F.col("return_rate_score") > 0) | (F.col("complaint_rate_score") > 0) | (F.col("positive_feedback_rate") > 0),
        F.round(
            F.least(F.coalesce(F.col("return_rate_score"), F.lit(0)), F.lit(100)) * 0.3 +
            F.least(F.coalesce(F.col("complaint_rate_score"), F.lit(0)), F.lit(100)) * 0.2 +
            F.least(F.coalesce(F.col("positive_feedback_rate"), F.lit(0)), F.lit(100)) * 0.5,
            2
        )
    ).otherwise(F.lit(20)).cast("double").alias("service_score"),  # 无数据给基础分20

    # 分区字段
    F.lit(dt).alias("dt")
)

# 计算总分 - 调整权重分配，更加平衡
comprehensive_score = ads_scores.select(
    F.col("store_id"),
    F.col("item_id"),
    F.col("traffic_score"),
    F.col("conversion_score"),
    F.col("content_score"),
    F.col("acquisition_score"),
    F.col("service_score"),
    # 总分计算 - 调整权重，更加平衡
    F.round(
        F.coalesce(F.col("traffic_score"), F.lit(20)) * 0.25 +
        F.coalesce(F.col("conversion_score"), F.lit(20)) * 0.25 +
        F.coalesce(F.col("content_score"), F.lit(20)) * 0.15 +
        F.coalesce(F.col("acquisition_score"), F.lit(20)) * 0.2 +
        F.coalesce(F.col("service_score"), F.lit(20)) * 0.15,
        2
    ).cast("double").alias("total_score"),
    F.lit(dt).alias("dt")
)

# 添加等级字段和更新时间 - 调整评级标准，更加合理
ads_scores_with_grade = comprehensive_score.withColumn(
    "grade",
    F.when(F.col("total_score") >= 85, "A")        # A级：优秀
    .when(F.col("total_score") >= 70, "B")         # B级：良好
    .when(F.col("total_score") >= 50, "C")         # C级：一般
    .otherwise("D")                                # D级：待改进
).withColumn(
    "update_time",
    F.current_timestamp()
)

# 验证数据量
print_data_count(ads_scores_with_grade, "ads_product_score_main")

# 显示部分数据以检查结果
print("ADS层商品评分主表前10条数据:")
ads_scores_with_grade.show(10)

# 统计得分分布
print("各项得分统计信息:")
ads_scores_with_grade.select(
    F.min("traffic_score").alias("min_traffic_score"),
    F.max("traffic_score").alias("max_traffic_score"),
    F.avg("traffic_score").alias("avg_traffic_score"),
    F.min("conversion_score").alias("min_conversion_score"),
    F.max("conversion_score").alias("max_conversion_score"),
    F.avg("conversion_score").alias("avg_conversion_score"),
    F.min("total_score").alias("min_total_score"),
    F.max("total_score").alias("max_total_score"),
    F.avg("total_score").alias("avg_total_score")
).show()

# 显示等级分布
print("等级分布:")
ads_scores_with_grade.groupBy("grade").count().orderBy("grade").show()

# 显示各等级的得分范围
print("各等级得分详情:")
ads_scores_with_grade.groupBy("grade").agg(
    F.min("total_score").alias("min_score"),
    F.max("total_score").alias("max_score"),
    F.avg("total_score").alias("avg_score"),
    F.count("*").alias("count")
).orderBy("grade").show()

# 写入数据
ads_scores_with_grade.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/ads/ads_product_score_main")

# 修复分区
repair_hive_table("ads_product_score_main")

# ====================== 商品竞品对比表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/ads/ads_product_diagnosis_compare")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_product_diagnosis_compare")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE ads_product_diagnosis_compare
(
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
) COMMENT '商品竞品对比表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/ads/ads_product_diagnosis_compare'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理商品竞品对比表数据...")

# 读取主表数据用于计算竞品对比
main_scores = spark.sql(f"SELECT * FROM ads_product_score_main WHERE dt = '{dt}'")

# 使用窗口函数计算各店铺内其他商品的平均得分
# 计算每个店铺各维度的平均值
window_spec = Window.partitionBy("store_id")

competitor_avg = main_scores.select(
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
ads_diagnosis_compare = competitor_avg.select(
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
print_data_count(ads_diagnosis_compare, "ads_product_diagnosis_compare")

# 写入数据
ads_diagnosis_compare.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/ads/ads_product_diagnosis_compare")

# 修复分区
repair_hive_table("ads_product_diagnosis_compare")

# 验证最终结果
print("验证最终结果:")
main_count = spark.sql(f"SELECT COUNT(*) as count FROM ads_product_score_main WHERE dt = '{dt}'").collect()[0]['count']
compare_count = spark.sql(f"SELECT COUNT(*) as count FROM ads_product_diagnosis_compare WHERE dt = '{dt}'").collect()[0]['count']
print(f"ADS主表记录数: {main_count}")
print(f"竞品对比表记录数: {compare_count}")

# 显示部分结果
print("竞品对比表前5条数据:")
spark.sql(f"SELECT * FROM ads_product_diagnosis_compare WHERE dt = '{dt}' LIMIT 5").show()

print("GD7 ADS层数据处理完成!")
spark.stop()
