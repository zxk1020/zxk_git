一、数据流向总览
ODS 层：数据存储于 Kafka（topic_log、topic_db），为原始数据。
DIM 层：数据存储于 HBase，通过配置表 table_process_dim 从 ODS 层 topic_db 同步维度数据（如 activity_info→dim_activity_info 等）。
DWD 层：数据存储于 Kafka，基于 ODS 层数据清洗、关联 DIM 层维度后生成明细事实表。
DWS 层：数据存储于 Doris，基于 DWD 层数据按维度和时间窗口汇总。
ADS 层：数据存储于 Doris，基于 DWS 层汇总结果计算最终业务指标。
二、DWD 层（明细事实表，存储于 Kafka）
1. 流量域
   dwd_traffic_start（启动日志）
   来源：ODS 层 topic_log 的启动行为数据，关联 dim_user_info（用户）、dim_base_province（地区）。
   字段：mid（设备 ID）、user_id（用户 ID）、channel（渠道）、start_type（启动类型）、ts（时间戳）、is_new（是否新访客）。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_traffic_start', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
   dwd_traffic_page（页面浏览）
   来源：ODS 层 topic_log 的页面浏览数据，关联 dim_user_info、dim_base_province。
   字段：mid、user_id、page_id（页面 ID）、during_time（停留时长）、channel、ts、is_new。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_traffic_page', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
   dwd_traffic_action（用户动作）
   来源：ODS 层 topic_log 的用户动作数据，关联 dim_sku_info（商品）。
   字段：mid、user_id、action_id（动作 ID）、item（商品 ID）、item_type（类型）、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_traffic_action', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
   dwd_traffic_display（曝光日志）
   来源：ODS 层 topic_log 的曝光数据，关联 dim_sku_info。
   字段：mid、user_id、item（商品 ID）、item_type、pos_id（位置）、pos_seq（序列）、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_traffic_display', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
   dwd_traffic_err（错误日志）
   来源：ODS 层 topic_log 的错误数据。
   字段：mid、user_id、err_code（错误码）、err_msg（错误信息）、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_traffic_err', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
2. 交易域
   dwd_trade_order_detail（下单明细）
   来源：ODS 层 topic_db 的 order_detail 表 insert 数据，关联 dim_sku_info、dim_activity_info（活动）、dim_coupon_info（优惠券）。
   字段：id（明细 ID）、order_id（订单 ID）、user_id、sku_id、sku_name、sku_num（数量）、split_original_amount（原始金额）、split_activity_amount（活动减免）、split_coupon_amount（优惠券减免）、create_time（下单时间）、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_trade_order_detail', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
   dwd_trade_cart_add（加购）
   来源：ODS 层 topic_db 的 cart_info 表 insert/update（加购）数据，关联 dim_sku_info。
   字段：id、user_id、sku_id、sku_num（加购数量）、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_trade_cart_add', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
   dwd_trade_order_cancel（取消订单）
   来源：ODS 层 topic_db 的 order_info 表 update（取消）数据，关联 dwd_trade_order_detail。
   字段：id（订单 ID）、user_id、cancel_time（取消时间）、sku_id、split_total_amount（订单金额）、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_trade_order_cancel', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
   dwd_trade_order_payment_success（支付成功）
   来源：ODS 层 topic_db 的 payment_info 表 update（支付成功）数据，关联 dim_base_dic（支付方式）。
   字段：id（支付 ID）、order_id、user_id、payment_type（支付方式）、callback_time（支付时间）、split_payment_amount（支付金额）、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_trade_order_payment_success', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
   dwd_trade_order_refund（退单）
   来源：ODS 层 topic_db 的 order_refund_info 表 insert 数据，关联 dim_sku_info。
   字段：id（退单 ID）、order_id、user_id、sku_id、refund_num（退单数）、refund_amount（退款金额）、create_time（退单时间）、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_trade_order_refund', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
3. 互动域
   dwd_interaction_comment_info（评论）
   来源：ODS 层 topic_db 的 comment_info 表 insert 数据，关联 dim_sku_info、dim_base_dic（评价等级）。
   字段：id（评论 ID）、user_id、sku_id、appraise（评价等级）、appraise_name（等级名称）、comment_txt（评论内容）、create_time、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_interaction_comment_info', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
4. 用户域
   dwd_user_register（用户注册）
   来源：ODS 层 topic_db 的 user_info 表 insert 数据，关联 dim_base_province。
   字段：id（用户 ID）、login_name、register_time（注册时间）、province_id、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_user_register', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
5. 工具域
   dwd_tool_coupon_get（领券）
   来源：ODS 层 topic_db 的 coupon_use 表 insert（领取）数据，关联 dim_coupon_info。
   字段：id、user_id、coupon_id、get_time（领取时间）、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_tool_coupon_get', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
   dwd_tool_coupon_use（用券）
   来源：ODS 层 topic_db 的 coupon_use 表 update（使用）数据，关联 dim_coupon_info、dwd_trade_order_detail。
   字段：id、user_id、coupon_id、order_id、use_time（使用时间）、benefit_amount（优惠金额）、ts。
   Kafka 配置：'connector' = 'kafka', 'topic' = 'dwd_tool_coupon_use', 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', 'format' = 'json'
   三、DWS 层（汇总表，存储于 Doris）
1. 流量域
   dws_traffic_source_keyword_page_view_window（搜索关键词页面浏览）
   来源：dwd_traffic_page 的搜索行为数据。
   字段：stt（窗口起始）、edt（窗口结束）、cur_date（日期）、keyword（关键词）、keyword_count（出现次数）。
   建表语句：
   sql
   CREATE TABLE dws_traffic_source_keyword_page_view_window (
   stt DATETIME COMMENT '窗口起始时间',
   edt DATETIME COMMENT '窗口结束时间',
   cur_date DATE COMMENT '当天日期',
   keyword VARCHAR(128) COMMENT '搜索关键词',
   keyword_count BIGINT REPLACE COMMENT '关键词出现次数'
   ) ENGINE=OLAP
   AGGREGATE KEY(stt, edt, cur_date, keyword)
   PARTITION BY RANGE(cur_date) ()
   DISTRIBUTED BY HASH(stt) BUCKETS 10
   PROPERTIES (
   'replication_num' = '3',
   'dynamic_partition.enable' = 'true',
   'dynamic_partition.time_unit' = 'DAY',
   'dynamic_partition.end' = '3'
   );

2. 交易域
   dws_trade_sku_order_window（SKU 粒度下单）
   来源：dwd_trade_order_detail。
   字段：stt、edt、cur_date、sku_id、sku_name、original_amount（原始金额）、activity_reduce_amount（活动减免）、coupon_reduce_amount（优惠券减免）、order_amount（下单金额）。
   建表语句：
   sql
   CREATE TABLE dws_trade_sku_order_window (
   stt DATETIME COMMENT '窗口起始时间',
   edt DATETIME COMMENT '窗口结束时间',
   cur_date DATE COMMENT '当天日期',
   sku_id INT COMMENT 'SKU_ID',
   sku_name CHAR(255) COMMENT 'SKU名称',
   original_amount DECIMAL(16,2) REPLACE COMMENT '原始金额',
   activity_reduce_amount DECIMAL(16,2) REPLACE COMMENT '活动减免金额',
   coupon_reduce_amount DECIMAL(16,2) REPLACE COMMENT '优惠券减免金额',
   order_amount DECIMAL(16,2) REPLACE COMMENT '下单金额'
   ) ENGINE=OLAP
   AGGREGATE KEY(stt, edt, cur_date, sku_id, sku_name)
   PARTITION BY RANGE(cur_date) ()
   DISTRIBUTED BY HASH(stt) BUCKETS 10
   PROPERTIES (
   'replication_num' = '3',
   'dynamic_partition.enable' = 'true',
   'dynamic_partition.time_unit' = 'DAY'
   );

3. 用户域
   dws_user_user_login_window（用户登录）
   来源：dwd_traffic_page 的登录行为数据。
   字段：stt、edt、cur_date、back_ct（回流用户数）、uu_ct（独立用户数）。
   建表语句：
   sql
   CREATE TABLE dws_user_user_login_window (
   stt DATETIME COMMENT '窗口起始时间',
   edt DATETIME COMMENT '窗口结束时间',
   cur_date DATE COMMENT '当天日期',
   back_ct BIGINT REPLACE COMMENT '回流用户数',
   uu_ct BIGINT REPLACE COMMENT '独立用户数'
   ) ENGINE=OLAP
   AGGREGATE KEY(stt, edt, cur_date)
   PARTITION BY RANGE(cur_date) ()
   DISTRIBUTED BY HASH(stt) BUCKETS 10
   PROPERTIES (
   'replication_num' = '3',
   'dynamic_partition.enable' = 'true'
   );

四、ADS 层（应用表，存储于 Doris）
1. 流量主题
   ads_traffic_summary（流量汇总）
   来源：dws_traffic_vc_ch_ar_is_new_page_view_window。
   字段：stat_date（统计日期）、channel（渠道）、pv（浏览量）、uv（访客数）、sv（会话数）、avg_duration（平均停留时长）。
   建表语句：
   sql
   CREATE TABLE ads_traffic_summary (
   stat_date DATE COMMENT '统计日期',
   channel VARCHAR(100) COMMENT '渠道',
   pv BIGINT COMMENT '页面浏览量',
   uv BIGINT COMMENT '独立访客数',
   sv BIGINT COMMENT '会话数',
   avg_duration DECIMAL(10,2) COMMENT '平均停留时长'
   ) ENGINE=OLAP
   DUPLICATE KEY(stat_date, channel)
   PARTITION BY RANGE(stat_date) ()
   DISTRIBUTED BY HASH(stat_date) BUCKETS 1
   PROPERTIES ('replication_num' = '3');

2. 交易主题
   ads_trade_sales_top10（销售 TOP10 商品）
   来源：dws_trade_sku_order_window。
   字段：stat_date、sku_id、sku_name、sale_amount（销售额）、rank（排名）。
   建表语句：
   sql
   CREATE TABLE ads_trade_sales_top10 (
   stat_date DATE COMMENT '统计日期',
   sku_id INT COMMENT '商品ID',
   sku_name CHAR(255) COMMENT '商品名称',
   sale_amount DECIMAL(20,2) COMMENT '销售额',
   rank INT COMMENT '排名'
   ) ENGINE=OLAP
   DUPLICATE KEY(stat_date, rank)
   PARTITION BY RANGE(stat_date) ()
   DISTRIBUTED BY HASH(stat_date) BUCKETS 1
   PROPERTIES ('replication_num' = '3');

3. 用户主题
   ads_user_active_stats（用户活跃度）
   来源：dws_user_user_login_window、dws_user_register_window。
   字段：stat_date、new_user_ct（新用户数）、active_user_ct（活跃用户数）、active_rate（活跃度）。
   建表语句：
   sql
   CREATE TABLE ads_user_active_stats (
   stat_date DATE COMMENT '统计日期',
   new_user_ct BIGINT COMMENT '新注册用户数',
   active_user_ct BIGINT COMMENT '活跃用户数',
   active_rate DECIMAL(5,2) COMMENT '活跃度(活跃/总用户)'
   ) ENGINE=OLAP
   DUPLICATE KEY(stat_date)
   PARTITION BY RANGE(stat_date) ()
   DISTRIBUTED BY HASH(stat_date) BUCKETS 1
   PROPERTIES ('replication_num' = '3');