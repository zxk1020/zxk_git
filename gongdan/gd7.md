电商数仓项目:
商品主题 商品诊断看板 任务工单

大数据-电商数仓-07-商品主题商品诊断看板设计文档
一、项目背景与目标
基于电商数仓项目需求，为商家提供商品全维度诊断能力，通过"流量获取、流量转化、内容营销、客户拉新、服务质量"
五大维度评估商品表现，实现商品分级（A-D级）和单品竞争力诊断功能。

# GD7 ADS层表结构分析

## -- 第一张表
## -- 1. ads_traffic_metrics_result
## -- 统计主题：流量获取指标结果表
## -- 统计内容：按统计日期、店铺ID及商品ID，统计：
## -- 店铺总点击数、商品访问次数、商品点击率、搜索排名得分、详情页点击率
## -- 数据来源：从用户行为日志表（dwd_user_behavior_log）聚合处理后生成。
## -- 底层：ads_traffic_metrics_result
## -- dwd_user_behavior_log
## -- ods_user_behavior_log

## -- 第二张表
## -- 2. ads_conversion_metrics_result
## -- 统计主题：转化指标结果表
## -- 统计内容：按统计日期、店铺ID及商品ID，统计：
## -- 转化率、加购转化率、支付成功率
## -- 数据来源：从用户行为日志表（dwd_user_behavior_log）聚合处理后生成。
## -- 底层：ads_conversion_metrics_result
## -- dwd_user_behavior_log
## -- ods_user_behavior_log

## -- 第三张表
## -- 3. ads_content_metrics_result
## -- 统计主题：内容营销指标结果表
## -- 统计内容：按统计日期、店铺ID及商品ID，统计：
## -- 平均内容观看时长、内容分享率、评论互动率
## -- 数据来源：从用户行为日志表（dwd_user_behavior_log）聚合处理后生成。
## -- 底层：ads_content_metrics_result
## -- dwd_user_behavior_log
## -- ods_user_behavior_log

## -- 第四张表
## -- 4. ads_acquisition_metrics_result
## -- 统计主题：客户拉新指标结果表
## -- 统计内容：按统计日期、店铺ID及商品ID，统计：
## -- 新客购买占比、新客复购率、获取成本得分
## -- 数据来源：从用户行为日志表（dwd_user_behavior_log）聚合处理后生成。
## -- 底层：ads_acquisition_metrics_result
## -- dwd_user_behavior_log
## -- ods_user_behavior_log

## -- 第五张表
## -- 5. ads_service_metrics_result
## -- 统计主题：服务质量指标结果表
## -- 统计内容：按统计日期、店铺ID及商品ID，统计：
## -- 退货率得分、投诉率得分、好评率
## -- 数据来源：从订单信息表（dwd_order_info）聚合处理后生成。
## -- 底层：ads_service_metrics_result
## -- dwd_order_info
## -- ods_order_info

## -- 第六张表
## -- 6. ads_comprehensive_score_result
## -- 统计主题：综合评分指标结果表
## -- 统计内容：按统计日期、店铺ID及商品ID，统计：
## -- 流量获取得分、转化得分、内容运营得分、客户获取得分、服务得分、总分、等级
## -- 数据来源：从流量、转化、内容、拉新、服务五个维度的ADS指标表合并计算生成。
## -- 底层：ads_comprehensive_score_result
## -- ads_traffic_metrics_result/ads_conversion_metrics_result/ads_content_metrics_result/ads_acquisition_metrics_result/ads_service_metrics_result
## -- dwd_user_behavior_log/dwd_order_info
## -- ods_user_behavior_log/ods_order_info

## -- 第七张表
## -- 7. ads_competitor_comparison_result
## -- 统计主题：竞品对比指标结果表
## -- 统计内容：按统计日期、店铺ID及商品ID，统计：
## -- 各维度竞品平均得分、各维度得分差值、总分差值
## -- 数据来源：从综合评分指标结果表（ads_comprehensive_score_result）使用窗口函数计算竞品平均值后生成。
## -- 底层：ads_competitor_comparison_result
## -- ads_comprehensive_score_result
## -- ads_traffic_metrics_result/ads_conversion_metrics_result/ads_content_metrics_result/ads_acquisition_metrics_result/ads_service_metrics_result
## -- dwd_user_behavior_log/dwd_order_info
## -- ods_user_behavior_log/ods_order_info

## -- 第八张表
## -- 8. ads_product_score_main
## -- 统计主题：商品评分主表
## -- 统计内容：按统计日期、店铺ID及商品ID，统计：
## -- 流量获取得分、转化得分、内容运营得分、客户获取得分、服务得分、总分、等级
## -- 数据来源：从商品流量、转化、内容、拉新、服务五个维度的DWS表合并计算生成。
## -- 底层：ads_product_score_main
## -- dws_product_traffic_d/dws_product_conversion_d/dws_product_content_d/dws_customer_acquisition_d/dws_product_service_d
## -- dwd_user_behavior_log/dwd_order_info
## -- ods_user_behavior_log/ods_order_info

## -- 第九张表
## -- 9. ads_product_diagnosis_compare
## -- 统计主题：商品竞品对比表
## -- 统计内容：按统计日期、店铺ID及商品ID，统计：
## -- 各维度竞品平均得分、各维度得分差值、总分差值
## -- 数据来源：从商品评分主表（ads_product_score_main）使用窗口函数计算竞品平均值后生成。
## -- 底层：ads_product_diagnosis_compare
## -- ads_product_score_main
## -- dws_product_traffic_d/dws_product_conversion_d/dws_product_content_d/dws_customer_acquisition_d/dws_product_service_d
## -- dwd_user_behavior_log/dwd_order_info
## -- ods_user_behavior_log/ods_order_info

二、ADS层表设计方案

1. 商品评分主表(product_score_main)
   表结构设计：
   字段名 类型 注释
   dt STRING 日期分区
   store_id STRING 店铺ID
   item_id STRING 商品ID
   traffic_score DECIMAL(5,2)    流量获取得分(0-100)
   conversion_score DECIMAL(5,2)    流量转化得分(0-100)
   content_score DECIMAL(5,2)    内容营销得分(0-100)
   acquisition_score DECIMAL(5,2)    客户拉新得分(0-100)
   service_score DECIMAL(5,2)    服务质量得分(0-100)
   total_score DECIMAL(5,2)    加权总分(0-100)
   grade STRING 等级(A/B/C/D)
   update_time TIMESTAMP 更新时间
   分区策略：按日期分区(bdp_day)

   商品评分主表(product_score_main):
   CREATE TABLE ads.product_score_main (
   dt STRING COMMENT '日期',
   store_id STRING COMMENT '店铺ID',
   item_id STRING COMMENT '商品ID',
   traffic_score DECIMAL(5,2) COMMENT '流量获取得分',
   conversion_score DECIMAL(5,2) COMMENT '流量转化得分',
   content_score DECIMAL(5,2) COMMENT '内容营销得分',
   acquisition_score DECIMAL(5,2) COMMENT '客户拉新得分',
   service_score DECIMAL(5,2) COMMENT '服务质量得分',·
   total_score DECIMAL(5,2) COMMENT '总分',
   grade STRING COMMENT '等级(A-D)',
   update_time TIMESTAMP COMMENT '更新时间'
   )
   PARTITIONED BY (bdp_day STRING COMMENT '分区字段')
   STORED AS PARQUET;

2. 商品诊断对比表(product_diagnosis_compare)
   表结构设计：
   字段名 类型 注释
   dt STRING 日期分区
   item_id STRING 商品ID
   competitor_avg_score DECIMAL(5,2)         竞品平均分
   score_gap DECIMAL(5,2)         分差(本品-竞品)
   traffic_gap DECIMAL(5,2)         流量获取分差
   conversion_gap DECIMAL(5,2)         转化分差
   content_gap DECIMAL(5,2)         内容分差
   acquisition_gap DECIMAL(5,2)         拉新分差
   service_gap DECIMAL(5,2)         服务分差
   update_time TIMESTAMP 更新时间
   分区策略：按日期分区(bdp_day)

   商品诊断对比表(product_diagnosis_compare):
   CREATE TABLE ads.product_diagnosis_compare (
   dt STRING COMMENT '日期',
   item_id STRING COMMENT '商品ID',
   competitor_avg_score DECIMAL(5,2) COMMENT '竞品平均分',
   score_gap DECIMAL(5,2) COMMENT '分差(本品-竞品)',
   traffic_gap DECIMAL(5,2) COMMENT '流量获取分差',
   conversion_gap DECIMAL(5,2) COMMENT '转化分差',
   content_gap DECIMAL(5,2) COMMENT '内容分差',
   acquisition_gap DECIMAL(5,2) COMMENT '拉新分差',
   service_gap DECIMAL(5,2) COMMENT '服务分差',
   update_time TIMESTAMP COMMENT '更新时间'
   )
   PARTITIONED BY (bdp_day STRING COMMENT '分区字段')
   STORED AS PARQUET;


1. 关键指标实现方案
   五维评分指标体系
   流量获取维度(25%)：

商品浏览量占比（店铺内）
搜索排名位置得分
详情页点击率
流量转化维度(30%)：

转化率（UV→支付）
加购转化率
支付成功率
内容营销维度(20%)：

内容浏览时长
内容分享率
评论互动率
客户拉新维度(15%)：

新客购买占比
新客复购率
新客获客成本得分
服务质量维度(10%)：

退货率逆向得分
投诉率逆向得分
好评率

4.评分计算逻辑
标准化处理：
各子指标采用min-max标准化到0-100分范围：
标准分 = (原始值 - 行业最小值) / (行业最大值 - 行业最小值) * 100
维度得分计算：
维度得分 = Σ(子指标标准分 × 子指标权重)
总分计算：
总分 = 流量获取得分×0.25 + 流量转化得分×0.3 +
内容营销得分×0.2 + 客户拉新得分×0.15 +
服务质量得分×0.1

分级逻辑实现
采用CASE WHEN语句实现
CASE
WHEN total_score >= 85 THEN 'A'
WHEN total_score >= 70 THEN 'B'
WHEN total_score >= 50 THEN 'C'
ELSE 'D'
END AS grade

四、数据流转设计
数据来源层：

用户行为日志（流量、转化数据）
交易订单数据（转化、服务质量）
内容互动数据（内容营销）
客户画像数据（客户拉新）
数据处理流程：ODS → DWD(维度建模) → DWS(轻度聚合) → ADS(应用层)

更新策略：
每日全量更新近30天数据
历史数据每月归档

五.实现验证方案（sql）
-- 检查总分计算准确性
SELECT item_id, total_score,
(traffic_score*0.25 + conversion_score*0.3 +
content_score*0.2 + acquisition_score*0.15 +
service_score*0.1) AS calculated_score
FROM ads.product_score_main
WHERE ABS(total_score - calculated_score) > 0.01;

-- 检查分级逻辑准确性
SELECT item_id, total_score, grade
FROM ads.product_score_main
WHERE (total_score >= 85 AND grade != 'A') OR
(total_score >= 70 AND total_score < 85 AND grade != 'B') OR
(total_score >= 50 AND total_score < 70 AND grade != 'C') OR
(total_score < 50 AND grade != 'D');

业务逻辑验证
测试用例：

极端值测试：输入各维度极值验证评分边界
权重敏感性测试：调整权重验证结果变化是否符合预期
竞品对比测试：验证本品与竞品分差计算逻辑
六、性能优化措施
分区优化：采用双级分区（dt + store_id）
索引优化：为item_id建立ZSTD压缩编码
计算优化：使用Spark缓存中间结果
资源调配：设置动态资源分配参数：
spark.dynamicAllocation.enabled=true
spark.shuffle.service.enabled=true

七、交付物清单
本设计文档（含ADS层表设计、指标实现方案）
PySpark实现代码（含不分层和分层两种实现）
测试文档（含测试SQL和验证结果）
系统上线截图（含时间戳和指标数值）

PySpark实现方案:
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ProductDiagnosis").enableHiveSupport().getOrCreate()

# 读取各维度原始数据

traffic_df = spark.table("dwd.product_traffic").filter(F.col("dt") == "${date}")
conversion_df = spark.table("dwd.product_conversion").filter(F.col("dt") == "${date}")
content_df = spark.table("dwd.product_content").filter(F.col("dt") == "${date}")
acquisition_df = spark.table("dwd.customer_acquisition").filter(F.col("dt") == "${date}")
service_df = spark.table("dwd.product_service").filter(F.col("dt") == "${date}")

# 计算各维度得分(0-100分)

traffic_score = traffic_df.groupBy("store_id", "item_id").agg(
F.expr("(SUM(page_views)/MAX(industry_avg)*50 + (1/rank)*50)").alias("traffic_score")
)

conversion_score = conversion_df.groupBy("store_id", "item_id").agg(
F.expr("(conversion_rate/MAX(industry_avg)*60 + add_to_cart_rate/MAX(industry_avg)*40)").alias("conversion_score")
)

# 其他维度得分计算类似...

# 合并各维度得分

final_score = traffic_score.join(conversion_score, ["store_id", "item_id"]) \
.join(content_score, ["store_id", "item_id"]) \
.join(acquisition_score, ["store_id", "item_id"]) \
.join(service_score, ["store_id", "item_id"])

# 计算总分和等级

result = final_score.withColumn(
"total_score",
F.col("traffic_score")*0.25 + F.col("conversion_score")*0.3 +
F.col("content_score")*0.2 + F.col("acquisition_score")*0.15 +
F.col("service_score")*0.1
).withColumn(
"grade",
F.when(F.col("total_score") >= 85, "A")
.when(F.col("total_score") >= 70, "B")
.when(F.col("total_score") >= 50, "C")
.otherwise("D")
)

# 保存结果

result.write.mode("overwrite").saveAsTable("ads.product_score_main")

DWD层：构建各维度事实表

# 示例：流量获取事实表

dwd_traffic = spark.sql("""
SELECT
dt,
store_id,
item_id,
session_id,
page_type,
event_time,
-- 其他字段...
FROM ods.user_behavior_log
WHERE dt = '${date}'
""")
dwd_traffic.write.mode("overwrite").saveAsTable("dwd.dwd_product_traffic_di")

DWS层：构建商品维度宽表
dws_product = spark.sql("""
SELECT
t1.dt,
t1.store_id,
t1.item_id,
t1.traffic_metrics,
t2.conversion_metrics,
-- 其他维度指标...
FROM dws.dws_product_traffic_d t1
JOIN dws.dws_product_conversion_d t2 ON t1.item_id = t2.item_id AND t1.dt = t2.dt
-- 其他JOIN...
""")

ADS层：最终聚合计算
ads_result = spark.sql("""
SELECT
dt,
store_id,
item_id,
-- 各维度得分计算
(traffic_indicator1 * weight1 + ...) AS traffic_score,
-- 其他维度得分...
-- 总分计算
(traffic_score*0.25 + conversion_score*0.3 + ...) AS total_score,
-- 等级计算
CASE
WHEN total_score >= 85 THEN 'A'
WHEN total_score >= 70 THEN 'B'
WHEN total_score >= 50 THEN 'C'
ELSE 'D'
END AS grade
FROM dws.dws_product_wide
WHERE dt = '${date}'
""")

电商商品诊断看板五维指标SQL实现方案
一、五维指标标准化SQL实现

1. 流量获取维度指标
   sql
   Copy Code
   -- 流量获取指标SQL
   SELECT
   store_id,
   item_id,
   -- 店铺总点击量(基于session_id去重)
   COUNT(DISTINCT CASE WHEN page_type = 'home' THEN session_id END) AS store_total_clicks,
   -- 商品访问量
   COUNT(CASE WHEN page_type LIKE 'item%' THEN 1 END) AS item_visits,
   -- 商品点击率(占店铺总点击)
   COUNT(CASE WHEN page_type LIKE 'item%' THEN 1 END) * 100.0 /
   NULLIF(COUNT(DISTINCT CASE WHEN page_type = 'home' THEN session_id END), 0) AS item_click_rate,
   -- 搜索排名得分(假设有search_rank字段)
   MAX(CASE WHEN event_type = 'search' THEN 100 - search_rank ELSE 0 END) AS search_rank_score,
   -- 详情页点击率
   COUNT(CASE WHEN page_type = 'item_detail' THEN 1 END) * 100.0 /
   NULLIF(COUNT(CASE WHEN page_type LIKE 'item%' THEN 1 END), 0) AS detail_click_rate
   FROM user_behavior_log
   WHERE dt = '${date}'
   GROUP BY store_id, item_id
2. 流量转化维度指标
   sql
   Copy Code
   -- 流量转化指标SQL
   SELECT
   store_id,
   item_id,
   -- 转化率(UV→支付)
   COUNT(DISTINCT CASE WHEN event_type = 'payment' THEN user_id END) * 100.0 /
   NULLIF(COUNT(DISTINCT CASE WHEN page_type = 'item_detail' THEN user_id END), 0) AS conversion_rate,
   -- 加购转化率
   COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) * 100.0 /
   NULLIF(COUNT(DISTINCT CASE WHEN page_type = 'item_detail' THEN user_id END), 0) AS cart_conversion_rate,
   -- 支付成功率
   COUNT(DISTINCT CASE WHEN event_type = 'payment_success' THEN user_id END) * 100.0 /
   NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'payment' THEN user_id END), 0) AS payment_success_rate
   FROM user_behavior_log
   WHERE dt = '${date}'
   GROUP BY store_id, item_id
3. 内容营销维度指标
   sql
   Copy Code
   -- 内容营销指标SQL
   SELECT
   store_id,
   item_id,
   -- 内容浏览时长(秒)
   AVG(CASE WHEN event_type = 'content_view' THEN duration ELSE 0 END) AS avg_content_duration,
   -- 内容分享率
   COUNT(DISTINCT CASE WHEN event_type = 'content_share' THEN user_id END) * 100.0 /
   NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'content_view' THEN user_id END), 0) AS content_share_rate,
   -- 评论互动率
   COUNT(DISTINCT CASE WHEN event_type = 'comment' THEN user_id END) * 100.0 /
   NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'content_view' THEN user_id END), 0) AS comment_interaction_rate
   FROM user_behavior_log
   WHERE dt = '${date}'
   GROUP BY store_id, item_id
4. 客户拉新维度指标
   sql
   Copy Code
   -- 客户拉新指标SQL
   SELECT
   store_id,
   item_id,
   -- 新客购买占比
   COUNT(DISTINCT CASE WHEN is_new_customer = 1 AND event_type = 'payment' THEN user_id END) * 100.0 /
   NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'payment' THEN user_id END), 0) AS new_customer_purchase_ratio,
   -- 新客复购率
   COUNT(DISTINCT CASE WHEN is_new_customer = 1 AND repurchase_flag = 1 THEN user_id END) * 100.0 /
   NULLIF(COUNT(DISTINCT CASE WHEN is_new_customer = 1 THEN user_id END), 0) AS new_customer_repurchase_rate,
   -- 新客获客成本得分(逆向指标)
   100 - (SUM(CASE WHEN is_new_customer = 1 THEN acquisition_cost ELSE 0 END) /
   NULLIF(COUNT(DISTINCT CASE WHEN is_new_customer = 1 THEN user_id END), 0)) * 10 AS acquisition_cost_score
   FROM user_behavior_log
   WHERE dt = '${date}'
   GROUP BY store_id, item_id
5. 服务质量维度指标
   sql
   Copy Code
   -- 服务质量指标SQL
   SELECT
   store_id,
   item_id,
   -- 退货率逆向得分
   100 - (COUNT(CASE WHEN order_status = 'returned' THEN 1 END) * 100.0 /
   NULLIF(COUNT(CASE WHEN order_status = 'completed' THEN 1 END), 0)) AS return_rate_score,
   -- 投诉率逆向得分
   100 - (COUNT(CASE WHEN has_complaint = 1 THEN 1 END) * 100.0 /
   NULLIF(COUNT(CASE WHEN order_status = 'completed' THEN 1 END), 0)) AS complaint_rate_score,
   -- 好评率
   AVG(CASE WHEN rating IS NOT NULL THEN rating ELSE 0 END) * 20 AS positive_feedback_rate
   FROM order_info
   WHERE dt = '${date}'
   GROUP BY store_id, item_id
   二、ADS层建表语句
1. 商品五维评分主表
   sql
   Copy Code
   CREATE TABLE IF NOT EXISTS ads.product_dimension_scores (
   dt STRING COMMENT '日期分区',
   store_id STRING COMMENT '店铺ID',
   item_id STRING COMMENT '商品ID',
   -- 流量获取维度
   store_total_clicks BIGINT COMMENT '店铺总点击量',
   item_visits BIGINT COMMENT '商品访问量',
   item_click_rate DECIMAL(5,2) COMMENT '商品点击率(%)',
   search_rank_score DECIMAL(5,2) COMMENT '搜索排名得分',
   detail_click_rate DECIMAL(5,2) COMMENT '详情页点击率(%)',
   -- 流量转化维度
   conversion_rate DECIMAL(5,2) COMMENT '转化率(%)',
   cart_conversion_rate DECIMAL(5,2) COMMENT '加购转化率(%)',
   payment_success_rate DECIMAL(5,2) COMMENT '支付成功率(%)',
   -- 内容营销维度
   avg_content_duration DECIMAL(10,2) COMMENT '平均内容浏览时长(秒)',
   content_share_rate DECIMAL(5,2) COMMENT '内容分享率(%)',
   comment_interaction_rate DECIMAL(5,2) COMMENT '评论互动率(%)',
   -- 客户拉新维度
   new_customer_purchase_ratio DECIMAL(5,2) COMMENT '新客购买占比(%)',
   new_customer_repurchase_rate DECIMAL(5,2) COMMENT '新客复购率(%)',
   acquisition_cost_score DECIMAL(5,2) COMMENT '获客成本得分',
   -- 服务质量维度
   return_rate_score DECIMAL(5,2) COMMENT '退货率逆向得分',
   complaint_rate_score DECIMAL(5,2) COMMENT '投诉率逆向得分',
   positive_feedback_rate DECIMAL(5,2) COMMENT '好评率得分',
   -- 综合评分
   traffic_score DECIMAL(5,2) COMMENT '流量获取得分(0-100)',
   conversion_score DECIMAL(5,2) COMMENT '流量转化得分(0-100)',
   content_score DECIMAL(5,2) COMMENT '内容营销得分(0-100)',
   acquisition_score DECIMAL(5,2) COMMENT '客户拉新得分(0-100)',
   service_score DECIMAL(5,2) COMMENT '服务质量得分(0-100)',
   total_score DECIMAL(5,2) COMMENT '综合评分(0-100)',
   grade STRING COMMENT '等级(A/B/C/D)',
   update_time TIMESTAMP COMMENT '更新时间'
   )
   PARTITIONED BY (bdp_day STRING COMMENT '业务日期分区')
   STORED AS PARQUET
   TBLPROPERTIES (
   'parquet.compression'='SNAPPY',
   'transient_lastDdlTime'='${timestamp}'
   );
2. 商品诊断对比表
   sql
   Copy Code
   CREATE TABLE IF NOT EXISTS ads.product_diagnosis_comparison (
   dt STRING COMMENT '日期分区',
   store_id STRING COMMENT '店铺ID',
   item_id STRING COMMENT '商品ID',
   category_id STRING COMMENT '商品类目ID',
   -- 本品指标
   self_traffic_score DECIMAL(5,2) COMMENT '本品流量获取得分',
   self_conversion_score DECIMAL(5,2) COMMENT '本品流量转化得分',
   self_content_score DECIMAL(5,2) COMMENT '本品内容营销得分',
   self_acquisition_score DECIMAL(5,2) COMMENT '本品客户拉新得分',
   self_service_score DECIMAL(5,2) COMMENT '本品服务质量得分',
   self_total_score DECIMAL(5,2) COMMENT '本品综合评分',
   -- 竞品平均指标
   competitor_avg_traffic DECIMAL(5,2) COMMENT '竞品平均流量得分',
   competitor_avg_conversion DECIMAL(5,2) COMMENT '竞品平均转化得分',
   competitor_avg_content DECIMAL(5,2) COMMENT '竞品平均内容得分',
   competitor_avg_acquisition DECIMAL(5,2) COMMENT '竞品平均拉新得分',
   competitor_avg_service DECIMAL(5,2) COMMENT '竞品平均服务得分',
   competitor_avg_total DECIMAL(5,2) COMMENT '竞品平均综合评分',
   -- 分差
   traffic_gap DECIMAL(5,2) COMMENT '流量获取分差(本品-竞品)',
   conversion_gap DECIMAL(5,2) COMMENT '流量转化分差',
   content_gap DECIMAL(5,2) COMMENT '内容营销分差',
   acquisition_gap DECIMAL(5,2) COMMENT '客户拉新分差',
   service_gap DECIMAL(5,2) COMMENT '服务质量分差',
   total_gap DECIMAL(5,2) COMMENT '综合评分分差',
   update_time TIMESTAMP COMMENT '更新时间'
   )
   PARTITIONED BY (bdp_day STRING COMMENT '业务日期分区')
   STORED AS PARQUET
   TBLPROPERTIES (
   'parquet.compression'='SNAPPY',
   'transient_lastDdlTime'='${timestamp}'
   );
   三、五维评分计算逻辑
1. 各维度得分计算
   sql
   Copy Code
   -- 综合评分计算SQL
   INSERT OVERWRITE TABLE ads.product_dimension_scores PARTITION (bdp_day='${date}')
   SELECT
   dt,
   store_id,
   item_id,
   -- 原始指标(省略部分字段展示)
   store_total_clicks,
   item_visits,
   item_click_rate,
   -- 综合评分计算SQL
   INSERT OVERWRITE TABLE ads.product_dimension_scores PARTITION (bdp_day='${date}')
   SELECT
   dt,
   store_id,
   item_id,
   -- 原始指标(省略部分字段展示)
   store_total_clicks,
   item_visits,
   item_click_rate,
   -- 各维度得分计算
   -- 流量获取得分(权重:点击率40%+搜索排名30%+详情点击率30%)
   (item_click_rate * 0.4 + search_rank_score * 0.3 + detail_click_rate * 0.3) AS traffic_score,
   -- 流量转化得分(权重:转化率50%+加购率30%+支付成功率20%)
   (conversion_rate * 0.5 + cart_conversion_rate * 0.3 + payment_success_rate * 0.2) AS conversion_score,
   -- 内容营销得分(权重:浏览时长40%+分享率30%+互动率30%)
   (LEAST(avg_content_duration, 300)/3 * 0.4 + content_share_rate * 0.3 + comment_interaction_rate * 0.3) AS
   content_score,
   -- 客户拉新得分(权重:新客占比40%+复购率40%+成本得分20%)
   (new_customer_purchase_ratio * 0.4 + new_customer_repurchase_rate * 0.4 + acquisition_cost_score * 0.2) AS
   acquisition_score,
   -- 服务质量得分(权重:退货得分30%+投诉得分20%+好评率50%)
   (return_rate_score * 0.3 + complaint_rate_score * 0.2 + positive_feedback_rate * 0.5) AS service_score,
   -- 综合评分(加权计算)
   ((item_click_rate * 0.4 + search_rank_score * 0.3 + detail_click_rate * 0.3) * 0.25 +
   (conversion_rate * 0.5 + cart_conversion_rate * 0.3 + payment_success_rate * 0.2) * 0.3 +
   (LEAST(avg_content_duration, 300)/3 * 0.4 + content_share_rate * 0.3 + comment_interaction_rate * 0.3) * 0.2 +
   (new_customer_purchase_ratio * 0.4 + new_customer_repurchase_rate * 0.4 + acquisition_cost_score * 0.2) * 0.15 +
   (return_rate_score * 0.3 + complaint_rate_score * 0.2 + positive_feedback_rate * 0.5) * 0.1) AS total_score,
   -- 等级划分
   CASE
   WHEN ((item_click_rate * 0.4 + search_rank_score * 0.3 + detail_click_rate * 0.3) * 0.25 +
   (conversion_rate * 0.5 + cart_conversion_rate * 0.3 + payment_success_rate * 0.2) * 0.3 +
   (LEAST(avg_content_duration, 300)/3 * 0.4 + content_share_rate * 0.3 + comment_interaction_rate * 0.3) * 0.2 +
   (new_customer_purchase_ratio * 0.4 + new_customer_repurchase_rate * 0.4 + acquisition_cost_score * 0.2) * 0.15 +
   (return_rate_score * 0.3 + complaint_rate_score * 0.2 + positive_feedback_rate * 0.5) * 0.1) >= 85 THEN 'A'
   WHEN ((item_click_rate * 0.4 + search_rank_score * 0.3 + detail_click_rate * 0.3) * 0.25 +
   (conversion_rate * 0.5 + cart_conversion_rate * 0.3 + payment_success_rate * 0.2) * 0.3 +
   (LEAST(avg_content_duration, 300)/3 * 0.4 + content_share_rate * 0.3 + comment_interaction_rate * 0.3) * 0.2 +
   (new_customer_purchase_ratio * 0.4 + new_customer_repurchase_rate * 0.4 + acquisition_cost_score * 0.2) * 0.15 +
   (return_rate_score * 0.3 + complaint_rate_score * 0.2 + positive_feedback_rate * 0.5) * 0.1) >= 70 THEN 'B'
   WHEN ((item_click_rate * 0.4 + search_rank_score * 0.3 + detail_click_rate * 0.3) * 0.25 +
   (conversion_rate * 0.5 + cart_conversion_rate * 0.3 + payment_success_rate * 0.2) * 0.3 +
   (LEAST(avg_content_duration, 300)/3 * 0.4 + content_share_rate * 0.3 + comment_interaction_rate * 0.3) * 0.2 +
   (new_customer_purchase_ratio * 0.4 + new_customer_repurchase_rate * 0.4 + acquisition_cost_score * 0.2) * 0.15 +
   (return_rate_score * 0.3 + complaint_rate_score * 0.2 + positive_feedback_rate * 0.5) * 0.1) >= 50 THEN 'C'
   ELSE 'D'
   END AS grade,
   CURRENT_TIMESTAMP() AS update_time
   FROM (
   -- 此处合并五个维度的子查询
   -- 流量获取维度
   SELECT t1.*, t2.conversion_rate, t2.cart_conversion_rate, t2.payment_success_rate,
   t3.avg_content_duration, t3.content_share_rate, t3.comment_interaction_rate,
   t4.new_customer_purchase_ratio, t4.new_customer_repurchase_rate, t4.acquisition_cost_score,
   t5.return_rate_score, t5.complaint_rate_score, t5.positive_feedback_rate
   FROM (
   SELECT dt, store_id, item_id,
   COUNT(DISTINCT CASE WHEN page_type = 'home' THEN session_id END) AS store_total_clicks,
   COUNT(CASE WHEN page_type LIKE 'item%' THEN 1 END) AS item_visits,
   COUNT(CASE WHEN page_type LIKE 'item%' THEN 1 END) * 100.0 /
   NULLIF(COUNT(DISTINCT CASE WHEN page_type = 'home' THEN session_id END), 0) AS item_click_rate,
   MAX(CASE WHEN event_type = 'search' THEN 100 - search_rank ELSE 0 END) AS search_rank_score,
   COUNT(CASE WHEN page_type = 'item_detail' THEN 1 END) * 100.0 /
   NULLIF(COUNT(CASE WHEN page_type LIKE 'item%' THEN 1 END), 0) AS detail_click_rate
   FROM user_behavior_log
   WHERE dt = '${date}'
   GROUP BY dt, store_id, item_id
   ) t1
   LEFT JOIN (
   -- 流量转化维度(省略子查询)
   ) t2 ON t1.store_id = t2.store_id AND t1.item_id = t2.item_id
   LEFT JOIN (
   -- 内容营销维度(省略子查询)
   ) t3 ON t1.store_id = t

:::ml-data{name=citationList}



设计文档:
1. 关键指标实现方案
   3.1 评分模型设计
   根据五个维度设计商品评分模型：
   流量获取(25%)：衡量商品获取流量的能力
   转化(30%)：衡量商品将流量转化为订单的能力
   内容营销(20%)：衡量商品内容营销效果
   客户拉新(15%)：衡量商品吸引新客户的能力
   服务质量(10%)：衡量商品服务质量
   3.2 等级划分标准
   A级：总分≥85分
   B级：70分≤总分<85分
   C级：50分≤总分<70分
   D级：总分<50分
   3.3 商品竞争力诊断逻辑
   通过比较商品在各维度得分与竞品平均得分的差值，识别商品优势和劣势维度。
2. 实现思路
   4.1 数据来源
   用户行为日志数据
   订单数据
   商品信息数据
   店铺信息数据
   4.2 数据处理流程
   DWD层：清洗和标准化原始数据，构建各维度基础指标
   DWS层：汇总各维度指标，计算标准化得分
   ADS层：计算综合得分和等级，生成商品竞争力诊断数据
   4.3 核心计算逻辑
   各维度得分标准化处理(0-100分)
   加权计算综合得分
   根据综合得分划分等级
   与竞品对比，计算各维度差值
3. 看板功能设计
   5.1 全品价值评估模块
   商品分布雷达图：展示各等级商品数量分布
   价值维度分析：按评分-金额/价格-销量/访客-销量三个维度分析商品价值
   核心商品识别：识别高分高销量商品
   潜力商品识别：识别高分低销量或低分高销量商品
   5.2 单品竞争力诊断模块
   商品综合评分详情：展示商品在各维度的具体得分
   竞争力对比分析：与竞品在各维度上的对比
   优势劣势识别：明确商品的优势和劣势维度
   优化建议：针对劣势维度提供优化建议
4. 数据验证方案
   6.1 数据一致性验证
   分层与不分层实现方式结果对比
   总分计算准确性验证
   等级划分准确性验证
   6.2 业务逻辑验证
   各维度得分合理性验证
   竞品对比逻辑验证
   商品分布符合业务预期
5. 性能优化方案
   7.1 数据存储优化
   使用Parquet格式存储数据
   合理设计分区字段
   采用ZSTD压缩算法
   7.2 计算性能优化
   启用Spark动态资源分配
   合理设置并行度
   使用缓存机制提高重复计算效率









