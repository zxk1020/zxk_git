大数据 - 电商实时数仓 - 08 - 用户主题用户价值诊断看板设计文档
一、项目背景与目标
基于电商实时数仓项目技术架构（ODS→DWD→DWS→DIM），为运营团队提供用户全维度价值诊断能力，通过 **
“活跃贡献、消费能力、复购忠诚、互动参与、流失风险”** 五大维度评估用户价值等级，实现用户分层（高价值 / 中价值 / 潜力用户 /
流失预警）和个性化运营策略匹配功能。
核心目标：解决 “用户价值模糊、运营策略无针对性” 问题，助力运营团队精准触达高价值用户、激活潜力用户、挽回流失风险用户，提升用户生命周期总价值（LTV）。
二、ADS 层表结构分析

1. ads_user_active_metrics_result（活跃贡献指标结果表）
   统计主题：用户活跃贡献维度核心指标
   统计内容：按统计日期（dt）、用户 ID（user_id）、用户等级（user_level），统计：月活跃天数、周活跃频率、访问深度（页面数 / 会话）、活跃时段分布
   数据来源：从用户行为日志事实表（dwd_traffic_page）、用户启动日志表（dwd_traffic_start）聚合处理生成
   底层表链路：ads_user_active_metrics_result → dws_user_active_d（每日活跃轻度聚合表）→ dwd_traffic_page/dwd_traffic_start
   → ods_topic_log（Kafka 用户行为日志主题）
2. ads_user_consume_metrics_result（消费能力指标结果表）
   统计主题：用户消费能力维度核心指标
   统计内容：按统计日期（dt）、用户 ID（user_id），统计：近 30 天消费总额、客单价（消费总额 / 订单数）、高价值商品（客单价
   TOP30%）购买占比、支付方式偏好
   数据来源：从交易订单明细事实表（dwd_trade_order_detail）、支付事实表（dwd_trade_order_payment_success）聚合处理生成
   底层表链路：ads_user_consume_metrics_result → dws_user_consume_d（每日消费轻度聚合表）→
   dwd_trade_order_detail/dwd_trade_order_payment_success → ods_topic_db（Kafka 业务数据主题）
3. ads_user_repurchase_metrics_result（复购忠诚指标结果表）
   统计主题：用户复购与忠诚度维度核心指标
   统计内容：按统计日期（dt）、用户 ID（user_id），统计：近 90 天复购次数、平均复购间隔（天）、会员等级（1-5 级）、专属权益使用频率
   数据来源：从交易订单表（dwd_trade_order_detail）、会员信息表（dim_user_info）聚合处理生成
   底层表链路：ads_user_repurchase_metrics_result → dws_user_repurchase_d（每日复购轻度聚合表）→
   dwd_trade_order_detail/dim_user_info → ods_topic_db/ods_mysql_user（MySQL 用户业务表）
4. ads_user_interaction_metrics_result（互动参与指标结果表）
   统计主题：用户互动参与维度核心指标
   统计内容：按统计日期（dt）、用户 ID（user_id），统计：近 30 天评论次数、内容分享次数、优惠券使用频率、收藏商品数
   数据来源：从互动行为事实表（dwd_interaction_comment_info/dwd_interaction_favor_add）、工具域优惠券表（dwd_tool_coupon_use）聚合处理生成
   底层表链路：ads_user_interaction_metrics_result → dws_user_interaction_d（每日互动轻度聚合表）→
   dwd_interaction_comment_info/dwd_tool_coupon_use → ods_topic_db
5. ads_user_churn_metrics_result（流失风险指标结果表）
   统计主题：用户流失风险维度核心指标
   统计内容：按统计日期（dt）、用户 ID（user_id），统计：最近活跃间隔（天）、消费频率下降幅度（与上月比）、互动衰减率（与上月比）、流失预警得分
   数据来源：从用户行为日志表（dwd_traffic_page）、交易表（dwd_trade_order_detail）聚合对比生成
   底层表链路：ads_user_churn_metrics_result → dws_user_churn_d（每日流失风险轻度聚合表）→
   dwd_traffic_page/dwd_trade_order_detail → ods_topic_log/ods_topic_db
6. ads_user_comprehensive_score_result（用户综合价值得分表）
   统计主题：用户五维价值综合评分
   统计内容：按统计日期（dt）、用户 ID（user_id），统计：活跃贡献得分、消费能力得分、复购忠诚得分、互动参与得分、流失风险逆向得分、综合总分、用户等级
   数据来源：从上述 5 个维度 ADS 表合并计算生成（流失风险指标逆向转换为 “留存潜力得分”）
   底层表链路：ads_user_comprehensive_score_result → 上述 5 个 ADS 表 → 对应 DWS/DWD/ODS 层表
7. ads_user_group_comparison_result（用户群体对比表）
   统计主题：用户与同群体（同等级 / 同消费区间）的差异对比
   统计内容：按统计日期（dt）、用户 ID（user_id）、用户群体标签，统计：同群体各维度平均分、本品与群体分差、优势 / 劣势维度标识
   数据来源：从综合价值得分表（ads_user_comprehensive_score_result）用 Flink 窗口函数（PARTITION BY 群体标签）计算群体平均值生成
   底层表链路：ads_user_group_comparison_result → ads_user_comprehensive_score_result → 上述 5 个 ADS 表
8. ads_user_value_main（用户价值主表）
   统计主题：用户价值核心指标汇总（面向看板展示）
   统计内容：按统计日期（dt）、用户 ID（user_id），统计：五维得分、综合总分、用户等级、运营策略建议（高价值：专属权益；流失预警：召回券）
   数据来源：从综合价值得分表（ads_user_comprehensive_score_result）、运营策略配置表（dim_operation_strategy）关联生成
   底层表链路：ads_user_value_main → ads_user_comprehensive_score_result/dim_operation_strategy → 上述 5 个 ADS 表 /dim
   层配置表
   三、ADS 层表设计方案
1. 用户价值主表（ads_user_value_main）
   表结构设计
   字段名 类型 注释
   dt STRING 统计日期（格式：yyyy-MM-dd）
   user_id STRING 用户唯一标识
   user_name STRING 用户昵称（从 dim_user_info 关联）
   user_level INT 用户等级（1-5 级）
   active_score DECIMAL(5,2)    活跃贡献得分（0-100）
   consume_score DECIMAL(5,2)    消费能力得分（0-100）
   repurchase_score DECIMAL(5,2)    复购忠诚得分（0-100）
   interaction_score DECIMAL(5,2)    互动参与得分（0-100）
   retention_score DECIMAL(5,2)    留存潜力得分（流失风险逆向得分，0-100）
   total_score DECIMAL(5,2)    综合总分（0-100）
   user_grade STRING 用户等级（高价值 / A、中价值 / B、潜力 / C、流失预警 / D）
   operation_strategy STRING 运营策略建议
   update_time TIMESTAMP 数据更新时间
   分区策略
   按 “统计日期（dt）+ 用户等级（user_level）” 双分区，避免单分区数据量过大，提升查询效率。
   建表 SQL
   sql
   CREATE TABLE ads.ads_user_value_main (
   dt STRING COMMENT '统计日期',
   user_id STRING COMMENT '用户唯一标识',
   user_name STRING COMMENT '用户昵称',
   user_level INT COMMENT '用户等级（1-5级）',
   active_score DECIMAL(5,2) COMMENT '活跃贡献得分（0-100）',
   consume_score DECIMAL(5,2) COMMENT '消费能力得分（0-100）',
   repurchase_score DECIMAL(5,2) COMMENT '复购忠诚得分（0-100）',
   interaction_score DECIMAL(5,2) COMMENT '互动参与得分（0-100）',
   retention_score DECIMAL(5,2) COMMENT '留存潜力得分（0-100）',
   total_score DECIMAL(5,2) COMMENT '综合总分（0-100）',
   user_grade STRING COMMENT '用户等级（高价值/A、中价值/B、潜力/C、流失预警/D）',
   operation_strategy STRING COMMENT '运营策略建议',
   update_time TIMESTAMP COMMENT '数据更新时间'
   )
   PARTITIONED BY (dt STRING COMMENT '统计日期分区', user_level INT COMMENT '用户等级分区')
   STORED AS PARQUET
   TBLPROPERTIES (
   'parquet.compression' = 'ZSTD', -- 启用ZSTD压缩，减少存储占用
   'dynamic.partition.mode' = 'nonstrict' -- 动态分区非严格模式
   );
2. 用户群体对比表（ads_user_group_comparison_result）
   表结构设计
   字段名 类型 注释
   dt STRING 统计日期（格式：yyyy-MM-dd）
   user_id STRING 用户唯一标识
   group_tag STRING 群体标签（如 “等级 3 - 消费 500-1000”）
   avg_active_score DECIMAL(5,2)    同群体活跃贡献平均分
   avg_consume_score DECIMAL(5,2)    同群体消费能力平均分
   avg_repurchase_score DECIMAL(5,2)    同群体复购忠诚平均分
   avg_interaction_score DECIMAL(5,2)    同群体互动参与平均分
   avg_retention_score DECIMAL(5,2)    同群体留存潜力平均分
   active_gap DECIMAL(5,2)    活跃得分分差（本品 - 群体平均）
   consume_gap DECIMAL(5,2)    消费得分分差（本品 - 群体平均）
   repurchase_gap DECIMAL(5,2)    复购得分分差（本品 - 群体平均）
   interaction_gap DECIMAL(5,2)    互动得分分差（本品 - 群体平均）
   retention_gap DECIMAL(5,2)    留存得分分差（本品 - 群体平均）
   advantage_dimension STRING 优势维度（得分高于群体 10 分以上的维度）
   disadvantage_dimension STRING 劣势维度（得分低于群体 10 分以上的维度）
   update_time TIMESTAMP 数据更新时间
   分区策略
   按 “统计日期（dt）+ 群体标签（group_tag）” 分区，便于按群体查询对比结果。
   建表 SQL
   sql
   CREATE TABLE ads.ads_user_group_comparison_result (
   dt STRING COMMENT '统计日期',
   user_id STRING COMMENT '用户唯一标识',
   group_tag STRING COMMENT '群体标签（如“等级3-消费500-1000”）',
   avg_active_score DECIMAL(5,2) COMMENT '同群体活跃贡献平均分',
   avg_consume_score DECIMAL(5,2) COMMENT '同群体消费能力平均分',
   avg_repurchase_score DECIMAL(5,2) COMMENT '同群体复购忠诚平均分',
   avg_interaction_score DECIMAL(5,2) COMMENT '同群体互动参与平均分',
   avg_retention_score DECIMAL(5,2) COMMENT '同群体留存潜力平均分',
   active_gap DECIMAL(5,2) COMMENT '活跃得分分差（本品-群体平均）',
   consume_gap DECIMAL(5,2) COMMENT '消费得分分差（本品-群体平均）',
   repurchase_gap DECIMAL(5,2) COMMENT '复购得分分差（本品-群体平均）',
   interaction_gap DECIMAL(5,2) COMMENT '互动得分分差（本品-群体平均）',
   retention_gap DECIMAL(5,2) COMMENT '留存得分分差（本品-群体平均）',
   advantage_dimension STRING COMMENT '优势维度',
   disadvantage_dimension STRING COMMENT '劣势维度',
   update_time TIMESTAMP COMMENT '数据更新时间'
   )
   PARTITIONED BY (dt STRING COMMENT '统计日期分区', group_tag STRING COMMENT '群体标签分区')
   STORED AS PARQUET
   TBLPROPERTIES (
   'parquet.compression' = 'ZSTD',
   'dynamic.partition.mode' = 'nonstrict'
   );
   四、关键指标实现方案
1. 五维价值指标体系（含权重）
   维度 权重 核心子指标 子指标权重 指标说明
   活跃贡献 25% 月活跃天数 40% 近 30 天活跃天数（活跃定义：有页面访问 / 启动行为）
   周活跃频率 30% 近 7 天活跃次数 / 7
   访问深度 30% 平均每会话访问页面数
   消费能力 30% 近 30 天消费总额 50% 实际支付金额（扣除退款）
   客单价 30% 消费总额 / 订单数
   高价值商品购买占比 20% 高价值商品（客单价 TOP30%）消费金额 / 总消费金额
   复购忠诚 20% 近 90 天复购次数 40% 同一用户重复购买次数
   平均复购间隔 30% 复购订单时间差平均值（间隔越短得分越高）
   会员等级 30% 1-5 级映射为 20-100 分
   互动参与 15% 近 30 天评论次数 30% 用户对商品 / 内容的评论次数
   内容分享次数 30% 用户分享商品 / 活动页面的次数
   优惠券使用频率 40% 领取优惠券使用次数 / 领取总次数
   留存潜力（逆向流失风险） 10% 最近活跃间隔 40% 间隔越短得分越高（逆向转换：100 - 间隔 ×5）
   消费频率下降幅度 30% 与上月比下降幅度越小得分越高
   互动衰减率 30% 与上月比衰减率越小得分越高
2. 评分计算逻辑
   （1）子指标标准化（Min-Max 标准化）
   将原始指标值映射到 0-100 分，消除量纲影响：
   sql
   标准分 = (原始值 - 行业最小值) / (行业最大值 - 行业最小值) * 100
   -- 特殊处理：逆向指标（如“最近活跃间隔”）需先取反再标准化
   逆向标准分 = (行业最大值 - 原始值) / (行业最大值 - 行业最小值) * 100
   （2）维度得分计算
   维度得分 = Σ（子指标标准分 × 子指标权重）
   示例（活跃贡献得分）：
   sql
   active_score = (active_days_std * 0.4) + (active_freq_std * 0.3) + (visit_depth_std * 0.3)
   （3）综合总分计算
   sql
   total_score = active_score * 0.25 + consume_score * 0.3 +
   repurchase_score * 0.2 + interaction_score * 0.15 +
   retention_score * 0.1
   （4）用户分级逻辑
   sql
   CASE
   WHEN total_score >= 85 THEN '高价值/A'
   WHEN total_score >= 70 THEN '中价值/B'
   WHEN total_score >= 50 THEN '潜力用户/C'
   ELSE '流失预警/D'
   END AS user_grade
   五、数据流转设计
1. 数据来源层
   用户行为数据：页面浏览、启动、互动（评论 / 分享）日志（来自 Kafka topic_log）
   交易数据：订单明细、支付、退款记录（来自 Kafka topic_db）
   用户基础数据：用户信息、会员等级（来自 MySQL 业务库，同步到 HBase dim_user_info）
   运营配置数据：用户群体标签规则、运营策略（来自 MySQL 配置库，同步到 dim_operation_strategy）
2. 数据处理流程（基于 Flink 实时计算）
   ODS 层：从 Kafka topic_log/topic_db 读取原始数据，按 “原样存储 + 格式校验” 原则落地，保留原始字段（如用户 ID、事件类型、时间戳）。
   DWD 层：基于维度建模（星型模型），构建各业务域事实表（如 dwd_traffic_page、dwd_trade_order_detail），补充维度外键（如
   user_id、product_id）。
   DWS 层：按 “每日轻度聚合” 原则，构建用户各维度日度宽表（如 dws_user_active_d、dws_user_consume_d），聚合粒度为
   “dt+user_id”。
   ADS 层：基于 DWS 层宽表，计算五维得分及综合总分，关联维度表补充用户属性和运营策略，最终写入 ADS 层表（存储于
   Doris，支持看板查询）。
3. 更新策略
   增量更新：实时消费 Kafka 增量数据，更新 DWD/DWS 层；每日凌晨 1 点全量重算近 90 天 ADS 层数据（处理历史数据补录场景）。
   历史归档：每月末将当月 ADS 层数据归档至 HDFS 冷存储（路径：/user/hive/warehouse/ads_archive/），保留 3 年。
   六、实现验证方案（Flink SQL）
1. 总分计算准确性验证
   sql
   SELECT
   user_id,
   total_score,
   -- 重新计算总分用于对比
   (active_score * 0.25 + consume_score * 0.3 +
   repurchase_score * 0.2 + interaction_score * 0.15 +
   retention_score * 0.1) AS calculated_score
   FROM ads.ads_user_value_main
   WHERE dt = '${current_date}'
   -- 允许0.01的精度误差（浮点计算偏差）
   AND ABS(total_score - calculated_score) > 0.01;
2. 分级逻辑准确性验证
   sql
   SELECT
   user_id,
   total_score,
   user_grade
   FROM ads.ads_user_value_main
   WHERE dt = '${current_date}'
   AND (
   (total_score >= 85 AND user_grade != '高价值/A')
   OR (total_score >= 70 AND total_score < 85 AND user_grade != '中价值/B')
   OR (total_score >= 50 AND total_score < 70 AND user_grade != '潜力用户/C')
   OR (total_score < 50 AND user_grade != '流失预警/D')
   );
3. 业务逻辑验证测试用例
   测试类型 测试场景 预期结果
   极端值测试 某用户近 30 天活跃 30 天、消费 10000 元、复购 10 次 综合总分≥90，等级为 “高价值 / A”
   权重敏感性测试 消费能力权重从 30% 调整为 40% 高消费用户总分提升 5-10 分，等级不变或升级
   群体对比测试 同群体（等级 3 - 消费 500-1000）平均分 75 本品得分 80 时，分差为 5，优势维度为 “消费能力”
   七、性能优化措施
1. 分区优化
   采用 “双级分区”（dt+user_level/group_tag），避免单分区数据量超过 100GB；
   对历史分区（超过 90 天）执行 “分区合并”（ALTER TABLE ... MERGE PARTITIONS），减少元数据数量。
2. 存储与索引优化
   启用 ZSTD 压缩编码（压缩比 3-5 倍），降低 ADS 层存储占用；
   为 “user_id”“dt” 字段建立 Doris Bitmap 索引（适用于高基数过滤场景），查询效率提升 50%+。
3. 计算优化
   Flink 状态管理：使用 HashMapStateBackend，Checkpoint 间隔设置为 5 分钟，超时时间 10 分钟，避免状态膨胀；
   中间结果缓存：DWS 层宽表缓存至 Flink Managed Memory，减少重复读取 HDFS 次数；
   并行度调优：ADS 层计算并行度与 Kafka 分区数（4 个）保持一致，避免数据倾斜。
4. 资源调配
   java
   // Flink动态资源配置（提交脚本）
   env.setParallelism(4);
   env.setStateBackend(new HashMapStateBackend());
   env.enableCheckpointing(300000); // 5分钟Checkpoint
   env.getCheckpointConfig().setCheckpointTimeout(600000); // 10分钟超时
   env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
   八、交付物清单
   设计文档：本用户价值诊断看板设计文档（含 ADS 层表结构、指标逻辑、数据流转）；
   Flink 实现代码：
   五维得分计算 Flink SQL 脚本（ads_user_score_calc.sql）；
   用户群体对比 Flink DataStream 代码（UserGroupComparison.java）；
   测试文档：含验证 SQL、测试用例、测试结果报告（用户价值看板测试报告.md）；
   上线截图：含数据更新时间戳、关键指标数值、看板界面截图（2 份：生产环境 / 测试环境）。
   九、核心代码示例（Flink SQL）
1. DWS 层用户活跃宽表构建
   sql
   CREATE TABLE dws.dws_user_active_d (
   dt STRING COMMENT '统计日期',
   user_id STRING COMMENT '用户ID',
   active_days INT COMMENT '近30天活跃天数',
   active_freq DECIMAL(5,2) COMMENT '周活跃频率',
   visit_depth DECIMAL(5,2) COMMENT '平均访问深度',
   update_time TIMESTAMP COMMENT '更新时间'
   ) PARTITIONED BY (dt STRING) STORED AS PARQUET;

INSERT OVERWRITE TABLE dws.dws_user_active_d PARTITION (dt = '${current_date}')
SELECT
'${current_date}' AS dt,
user_id,
COUNT(DISTINCT CASE WHEN dt BETWEEN DATE_SUB('${current_date}', 29) AND '${current_date}' THEN dt END) AS active_days,
COUNT(CASE WHEN dt BETWEEN DATE_SUB('${current_date}', 6) AND '${current_date}' THEN 1 END) / 7 AS active_freq,
AVG(CASE WHEN page_type = 'item_detail' THEN page_count ELSE 0 END) AS visit_depth,
CURRENT_TIMESTAMP() AS update_time
FROM dwd.dwd_traffic_page
WHERE dt BETWEEN DATE_SUB('${current_date}', 29) AND '${current_date}'
GROUP BY user_id;

2. ADS 层综合得分计算
   sql
   INSERT OVERWRITE TABLE ads.ads_user_value_main PARTITION (dt = '${current_date}', user_level)
   SELECT
   '${current_date}' AS dt,
   a.user_id,
   b.user_name,
   b.user_level,
   -- 活跃贡献得分（标准化后加权）
   (a.active_days_std * 0.4 + a.active_freq_std * 0.3 + a.visit_depth_std * 0.3) AS active_score,
   -- 其他维度得分计算（省略，逻辑同上）
   c.consume_score,
   d.repurchase_score,
   e.interaction_score,
   f.retention_score,
   -- 综合总分
   (active_score * 0.25 + c.consume_score * 0.3 + d.repurchase_score * 0.2 + e.interaction_score * 0.15 +
   f.retention_score * 0.1) AS total_score,
   -- 用户分级
   CASE
   WHEN total_score >= 85 THEN '高价值/A'
   WHEN total_score >= 70 THEN '中价值/B'
   WHEN total_score >= 50 THEN '潜力用户/C'
   ELSE '流失预警/D'
   END AS user_grade,
   g.operation_strategy,
   CURRENT_TIMESTAMP() AS update_time,
   b.user_level AS user_level -- 分区字段
   FROM dws_user_active_d a
   JOIN dim_user_info b ON a.user_id = b.user_id
   JOIN ads_user_consume_metrics_result c ON a.user_id = c.user_id AND a.dt = c.dt
   JOIN ads_user_repurchase_metrics_result d ON a.user_id = d.user_id AND a.dt = d.dt
   JOIN ads_user_interaction_metrics_result e ON a.user_id = e.user_id AND a.dt = e.dt
   JOIN ads_user_churn_metrics_result f ON a.user_id = f.user_id AND a.dt = f.dt
   JOIN dim_operation_strategy g ON user_grade = g.user_grade
   WHERE a.dt = '${current_date}';