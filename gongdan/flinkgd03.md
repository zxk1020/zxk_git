DWS层建表分析
1. dws_traffic_overview（流量总览主题表）
   建表语句分析：
   基于dwd_user_visit_detail、dwd_product_interaction_detail、dwd_pay_detail等表聚合
   聚合维度：stat_time、terminal_type、shop_id
   移除了依赖DIM的字段，仅使用ODS原始字段
   包含多个核心指标：访客数、PV、新老访客数、支付买家数、支付金额等
2. dws_traffic_source_ranking（流量来源排行主题表）
   建表语句分析：
   基于dwd_user_visit_detail表聚合
   聚合维度：stat_time、terminal_type、shop_id、traffic_source_first、traffic_source_second
   移除了source_desc、related_product_name等DIM字段
   保留了related_product_id（ODS原始字段）
   包含访客数、细分来源列表、导流商品效果等指标
3. dws_keyword_ranking（关键词排行主题表）
   建表语句分析：
   基于dwd_search_detail表聚合
   聚合维度：stat_time、stat_dimension、terminal_type、shop_id、keyword
   无DIM依赖，保持原结构
   包含搜索访客数、点击结果访客数等指标
4. dws_product_traffic（商品流量主题表）
   建表语句分析：
   基于dwd_user_visit_detail、dwd_product_interaction_detail、dwd_pay_detail等表聚合
   聚合维度：stat_time、terminal_type、shop_id、product_id
   移除了product_name、product_title等DIM字段
   保留product_id（ODS原始字段）
   包含访客数、PV、平均停留时间、加购数、收藏数、支付买家数、支付金额等指标
5. dws_page_traffic（页面流量主题表）
   建表语句分析：
   基于dwd_user_visit_detail、dwd_page_click_detail等表聚合
   聚合维度：stat_time、terminal_type、shop_id、page_id
   移除了page_name、page_url、page_type等DIM字段
   保留page_id（ODS原始字段）
   包含访客数、PV、点击数、平均停留时间等指标
   module_click_json和guide_product_json仅保留ODS原始ID
6. dws_crowd_feature（人群特征主题表）
   建表语句分析：
   基于dwd_crowd_attribute_detail表聚合
   聚合维度：stat_time、terminal_type、shop_id、crowd_type、gender、age_range、city、taoqi_value
   移除了city_level、taoqi_range等DIM映射字段
   保留city、taoqi_value（ODS原始字段）
   包含人群数量、总人数、人群占比等指标


根据代码分析，该代码从 FlinkGd03_dws_traffic_overview Kafka 主题读取数据，
并将数据直接写入到 MySQL 的 ads_traffic_overview 表中，没有进行额外的聚合计算。让我详细解释一下：
读取的DWS层指标（来自Kafka消息）：
stat_time - 统计时间（时间戳格式，会被转换为字符串）
terminal_type - 终端类型（overall/pc/wireless）
shop_id - 店铺ID
shop_visitor_count - 访问店铺访客数
shop_page_view - 访问店铺页面浏览量
new_visitor_count - 新访客数
old_visitor_count - 老访客数
product_visitor_count - 访问商品访客数
product_page_view - 访问商品页面浏览量
add_collection_count - 加购收藏人数
pay_buyer_count - 支付买家数
pay_amount - 支付金额

