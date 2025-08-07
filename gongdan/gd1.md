-- ====================== ODS层表（带外键关联）======================


CREATE_TABLE_SQL_LIST = [
"""
CREATE TABLE user_info (
`user_id` BIGINT AUTO_INCREMENT COMMENT '用户ID',
`user_name` VARCHAR(50) COMMENT '用户姓名',
`gender` VARCHAR(10) COMMENT '性别',
`level` VARCHAR(20) COMMENT '用户级别',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`user_id`)
) COMMENT='用户信息表';
""",
"""
CREATE TABLE product_info (
`product_id` BIGINT AUTO_INCREMENT COMMENT '商品ID',
`product_name` VARCHAR(255) COMMENT '商品名称',
`category_name` VARCHAR(100) COMMENT '商品品类',
`brand` VARCHAR(100) COMMENT '品牌',
`price` DECIMAL(10,2) COMMENT '商品价格',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`product_id`)
) COMMENT='商品信息表';
""",
"""
CREATE TABLE product_view_log (
`id` BIGINT AUTO_INCREMENT COMMENT '日志ID',
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`visit_time` DATETIME COMMENT '访问时间',
`platform` VARCHAR(20) DEFAULT NULL COMMENT '平台（PC/无线）',
`session_id` VARCHAR(100) COMMENT '会话ID',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`id`),
CONSTRAINT `fk_view_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_view_user` FOREIGN KEY (`user_id`) REFERENCES  `user_info`  (`user_id`)
) COMMENT='商品访问日志表';
""",
"""
CREATE TABLE product_collect_log (
`id` BIGINT AUTO_INCREMENT COMMENT '日志ID',
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`collect_time` DATETIME COMMENT '收藏时间',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`id`),
CONSTRAINT `fk_collect_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_collect_user` FOREIGN KEY (`user_id`) REFERENCES `user_info` (`user_id`)
) COMMENT='商品收藏日志表';
""",
"""
CREATE TABLE product_cart_log (
`id` BIGINT AUTO_INCREMENT COMMENT '日志ID',
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`quantity` INT COMMENT '加购件数',
`add_time` DATETIME COMMENT '加购时间',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`id`),
CONSTRAINT `fk_cart_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_cart_user` FOREIGN KEY (`user_id`) REFERENCES `user_info`  (`user_id`)
) COMMENT='商品加购日志表';
""",
"""
CREATE TABLE product_order_log (
`order_id` BIGINT AUTO_INCREMENT COMMENT '订单ID',
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`quantity` INT COMMENT '下单件数',
`amount` DECIMAL(10,2) COMMENT '下单金额',
`order_time` DATETIME COMMENT '下单时间',
`is_paid` INT COMMENT '是否支付(0:是 1:否)',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`order_id`),
CONSTRAINT `fk_order_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_order_user` FOREIGN KEY (`user_id`) REFERENCES  `user_info`  (`user_id`)
) COMMENT='商品订单日志表';
"""
]


-- 用户信息表（新增）
CREATE TABLE user_info (
`user_id` BIGINT AUTO_INCREMENT COMMENT '用户ID',
`user_name` VARCHAR(50) COMMENT '用户姓名',
`gender` VARCHAR(10) COMMENT '性别',
`level` VARCHAR(20) COMMENT '用户级别',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`user_id`)
) COMMENT='用户信息表';

-- 商品信息表（新增）
CREATE TABLE product_info (
`product_id` BIGINT AUTO_INCREMENT COMMENT '商品ID',
`product_name` VARCHAR(255) COMMENT '商品名称',
`category_name` VARCHAR(100) COMMENT '商品品类',
`brand` VARCHAR(100) COMMENT '品牌',
`price` DECIMAL(10,2) COMMENT '商品价格',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`product_id`)
) COMMENT='商品信息表';

-- 商品访问日志表（关联用户和商品）
CREATE TABLE product_view_log (
`id` BIGINT AUTO_INCREMENT COMMENT '日志ID',
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`visit_time` DATETIME COMMENT '访问时间',
`platform` VARCHAR(20) DEFAULT NULL COMMENT '平台（PC/无线）',
`session_id` VARCHAR(100) COMMENT '会话ID',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`id`),
CONSTRAINT `fk_view_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_view_user` FOREIGN KEY (`user_id`) REFERENCES [user_info](file://D:\zg6\zxk_git\offline-pyspark\tms\py_spark\dim.py#L481-L484) (`user_id`)
) COMMENT='商品访问日志表';

-- 商品收藏日志表（关联用户和商品）
CREATE TABLE product_collect_log (
`id` BIGINT AUTO_INCREMENT COMMENT '日志ID',
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`collect_time` DATETIME COMMENT '收藏时间',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`id`),
CONSTRAINT `fk_collect_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_collect_user` FOREIGN KEY (`user_id`) REFERENCES [user_info](file://D:\zg6\zxk_git\offline-pyspark\tms\py_spark\dim.py#L481-L484) (`user_id`)
) COMMENT='商品收藏日志表';

-- 商品加购日志表（关联用户和商品）
CREATE TABLE product_cart_log (
`id` BIGINT AUTO_INCREMENT COMMENT '日志ID',
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`quantity` INT COMMENT '加购件数',
`add_time` DATETIME COMMENT '加购时间',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`id`),
CONSTRAINT `fk_cart_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_cart_user` FOREIGN KEY (`user_id`) REFERENCES [user_info](file://D:\zg6\zxk_git\offline-pyspark\tms\py_spark\dim.py#L481-L484) (`user_id`)
) COMMENT='商品加购日志表';

-- 商品订单日志表（关联用户和商品）
CREATE TABLE product_order_log (
`order_id` BIGINT AUTO_INCREMENT COMMENT '订单ID',
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`quantity` INT COMMENT '下单件数',
`amount` DECIMAL(10,2) COMMENT '下单金额',
`order_time` DATETIME COMMENT '下单时间',
`is_paid` INT COMMENT '是否支付(0:是 1:否)',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`operate_time` DATETIME DEFAULT NULL COMMENT '修改时间',
PRIMARY KEY (`order_id`),
CONSTRAINT `fk_order_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_order_user` FOREIGN KEY (`user_id`) REFERENCES [user_info](file://D:\zg6\zxk_git\offline-pyspark\tms\py_spark\dim.py#L481-L484) (`user_id`)
) COMMENT='商品订单日志表';

-- ====================== DIM层表 ======================

-- 商品维度表
CREATE TABLE dim_product (
`product_id` BIGINT COMMENT '商品ID',
`product_name` VARCHAR(255) COMMENT '商品名称',
`category_name` VARCHAR(100) COMMENT '商品品类',
`brand` VARCHAR(100) COMMENT '品牌',
`description` VARCHAR(500) COMMENT '商品描述',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`update_time` DATETIME DEFAULT NULL COMMENT '更新时间'
) COMMENT='商品维度表';

-- 用户维度表
CREATE TABLE dim_user (
`user_id` BIGINT COMMENT '用户ID',
`user_name` VARCHAR(50) COMMENT '用户名称',
`gender` VARCHAR(10) COMMENT '性别',
`level` VARCHAR(20) COMMENT '用户级别',
`create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
`update_time` DATETIME DEFAULT NULL COMMENT '更新时间'
) COMMENT='用户维度表';

-- 平台维度表
CREATE TABLE dim_platform (
`platform_id` INT COMMENT '平台ID',
`platform_name` VARCHAR(20) COMMENT '平台名称',
`platform_desc` VARCHAR(100) COMMENT '平台描述'
) COMMENT='平台维度表';

-- 支付状态维度表
CREATE TABLE dim_payment_status (
`status_id` INT COMMENT '支付状态ID',
`status_name` VARCHAR(20) COMMENT '支付状态名称',
`status_desc` VARCHAR(100) COMMENT '状态描述'
) COMMENT='支付状态维度表';




-- 商品访问事实表
CREATE TABLE dwd_product_view (
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`visit_date` DATE COMMENT '访问日期',
`platform` VARCHAR(20) COMMENT '平台（PC/无线）',
`view_count` BIGINT COMMENT '浏览量',
`visitor_count` BIGINT COMMENT '访客数'
) COMMENT='商品访问事实表';

-- 商品收藏事实表
CREATE TABLE dwd_product_collect (
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`collect_date` DATE COMMENT '收藏日期',
`collector_count` BIGINT COMMENT '收藏人数'
) COMMENT='商品收藏事实表';

-- 商品加购事实表
CREATE TABLE dwd_product_cart (
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`cart_date` DATE COMMENT '加购日期',
`cart_quantity` BIGINT COMMENT '加购件数',
`cart_user_count` BIGINT COMMENT '加购人数'
) COMMENT='商品加购事实表';

-- 商品订单事实表
CREATE TABLE dwd_product_order (
`product_id` BIGINT COMMENT '商品ID',
`user_id` BIGINT COMMENT '用户ID',
`order_date` DATE COMMENT '下单日期',
`quantity` BIGINT COMMENT '下单件数',
`amount` DECIMAL(10,2) COMMENT '下单金额',
`paid_quantity` BIGINT COMMENT '支付件数',
`paid_amount` DECIMAL(10,2) COMMENT '支付金额',
`order_user_count` BIGINT COMMENT '下单买家数',
`paid_user_count` BIGINT COMMENT '支付买家数'
) COMMENT='商品订单事实表';



-- 商品效率汇总表
CREATE TABLE dws_product_efficiency (
`product_id` BIGINT COMMENT '商品ID',
`stat_date` DATE COMMENT '统计日期',
`view_count` BIGINT COMMENT '商品浏览量',
`visitor_count` BIGINT COMMENT '商品访客数',
`collect_count` BIGINT COMMENT '商品收藏人数',
`cart_quantity` BIGINT COMMENT '商品加购件数',
`cart_user_count` BIGINT COMMENT '商品加购人数',
`order_quantity` BIGINT COMMENT '下单件数',
`order_amount` DECIMAL(10,2) COMMENT '下单金额',
`order_user_count` BIGINT COMMENT '下单买家数',
`paid_quantity` BIGINT COMMENT '支付件数',
`paid_amount` DECIMAL(10,2) COMMENT '支付金额',
`paid_user_count` BIGINT COMMENT '支付买家数'
) COMMENT='商品效率汇总表';

-- 商品区间分析表
CREATE TABLE dws_product_range_analysis (
`category_id` BIGINT COMMENT '类目ID',
`price_range` VARCHAR(50) COMMENT '价格区间',
`quantity_range` VARCHAR(50) COMMENT '支付件数区间',
`amount_range` VARCHAR(50) COMMENT '支付金额区间',
`product_count` BIGINT COMMENT '商品数',
`paid_amount` DECIMAL(10,2) COMMENT '支付金额',
`paid_quantity` BIGINT COMMENT '支付件数',
`avg_price` DECIMAL(10,2) COMMENT '件单价'
) COMMENT='商品区间分析表';



-- 商品效率监控表
CREATE TABLE ads_product_efficiency_monitor (
`stat_date` DATE COMMENT '统计日期',
`time_dimension` VARCHAR(20) COMMENT '时间维度（日/周/月）',
`view_count` BIGINT COMMENT '商品浏览量',
`visitor_count` BIGINT COMMENT '商品访客数',
`collect_count` BIGINT COMMENT '商品收藏人数',
`cart_quantity` BIGINT COMMENT '商品加购件数',
`cart_user_count` BIGINT COMMENT '商品加购人数',
`order_quantity` BIGINT COMMENT '下单件数',
`order_amount` DECIMAL(10,2) COMMENT '下单金额',
`order_user_count` BIGINT COMMENT '下单买家数',
`paid_quantity` BIGINT COMMENT '支付件数',
`paid_amount` DECIMAL(10,2) COMMENT '支付金额',
`paid_user_count` BIGINT COMMENT '支付买家数',
`conversion_rate` DECIMAL(5,4) COMMENT '支付转化率'
) COMMENT='商品效率监控表';

-- 商品区间分析结果表
CREATE TABLE ads_product_range_analysis (
`category_id` BIGINT COMMENT '类目ID',
`category_name` VARCHAR(100) COMMENT '类目名称',
`stat_date` DATE COMMENT '统计日期',
`time_dimension` VARCHAR(20) COMMENT '时间维度（日/周/月）',
`range_type` VARCHAR(20) COMMENT '区间类型（价格/件数/金额）',
`range_value` VARCHAR(50) COMMENT '区间值',
`product_count` BIGINT COMMENT '商品数',
`paid_amount` DECIMAL(10,2) COMMENT '支付金额',
`paid_quantity` BIGINT COMMENT '支付件数',
`avg_price` DECIMAL(10,2) COMMENT '件单价',
`sort_metric` VARCHAR(50) COMMENT '排序指标'
) COMMENT='商品区间分析结果表';



# 电商数仓项目 - 商品主题宏观监控看板设计文档

**工单编号**: 大数据-电商数仓-01-商品主题宏观监控看板  
**项目名称**: 电商数仓  
**创建时间**: 2025.1.14  
**创建人**: 郭洵  
**文档版本**: v1.0

---

## 1. 商品效率监控分析

### 1.1 商品访问与交互情况分析
可以查询店铺内商品的访问、收藏、加购等数据情况，帮助商家了解商品受欢迎程度

- 维度：每个店铺(store_id)、商品(item_id)
    - 访问：对店铺内商品的访问情况
    - 交互：商品收藏、加购等用户行为
    - 销售：商品的下单、支付情况

```sql
-- 示例SQL：统计店铺商品各项指标
SELECT 
    store_id,
    item_id,
    COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) AS visitor_count,
    COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS view_count,
    COUNT(DISTINCT CASE WHEN event_type = 'collect' THEN user_id END) AS collect  or_count,
    COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS cart_user_count,
    SUM(CASE WHEN event_type = 'cart' THEN quantity ELSE 0 END) AS cart_quantity,
    COUNT(DISTINCT CASE WHEN event_type = 'order' THEN user_id END) AS order_user_count,
    SUM(CASE WHEN event_type = 'order' THEN quantity ELSE 0 END) AS order_quantity,
    SUM(CASE WHEN event_type = 'order' THEN amount ELSE 0 END) AS order_amount,
    COUNT(DISTINCT CASE WHEN event_type = 'payment' THEN user_id END) AS paid_user_count,
    SUM(CASE WHEN event_type = 'payment' THEN quantity ELSE 0 END) AS paid_quantity,
    SUM(CASE WHEN event_type = 'payment' THEN amount ELSE 0 END) AS paid_amount
FROM dwd_product_events
GROUP BY store_id, item_id
```


### 1.2 商品销售转化分析
用于分析商品从曝光到最终支付的转化效果，优化商品运营策略

#### 1.2.1 商品各环节转化率分析
针对商品从访客到最终支付的各个环节进行转化率统计分析，优化商品展示和营销策略

- 维度：店铺(store_id)、商品(item_id)、时间周期
    - 每个商品在统计周期内的各项用户行为数据
        - 1. 商品访客数（访问过商品详情页的去重人数）
        - 2. 商品收藏人数（新增点击收藏商品的去重人数）
        - 3. 商品加购人数（新增加购商品的去重人数）
        - 4. 商品下单人数（拍下商品的去重买家人数）
        - 5. 商品支付人数（完成支付的去重买家人数）

- 指标计算：
    - 1. 访问收藏转化率 = 商品收藏人数 / 商品访客数 × 100%
    - 2. 访问加购转化率 = 商品加购人数 / 商品访客数 × 100%
    - 3. 下单转化率 = 商品下单人数 / 商品访客数 × 100%
    - 4. 支付转化率 = 商品支付人数 / 商品访客数 × 100%

```sql
-- 示例SQL：计算商品各环节转化率
SELECT 
    store_id,
    item_id,
    visitor_count,
    collector_count,
    cart_user_count,
    order_user_count,
    paid_user_count,
    CASE 
        WHEN visitor_count > 0 
        THEN ROUND(collector_count * 100.0 / visitor_count, 2) 
        ELSE 0 
    END AS view_to_collect_rate,
    CASE 
        WHEN visitor_count > 0 
        THEN ROUND(cart_user_count * 100.0 / visitor_count, 2) 
        ELSE 0 
    END AS view_to_cart_rate,
    CASE 
        WHEN visitor_count > 0 
        THEN ROUND(order_user_count * 100.0 / visitor_count, 2) 
        ELSE 0 
    END AS view_to_order_rate,
    CASE 
        WHEN visitor_count > 0 
        THEN ROUND(paid_user_count * 100.0 / visitor_count, 2) 
        ELSE 0 
    END AS view_to_payment_rate
FROM (
    SELECT 
        store_id,
        item_id,
        COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) AS visitor_count,
        COUNT(DISTINCT CASE WHEN event_type = 'collect' THEN user_id END) AS collector_count,
        COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS cart_user_count,
        COUNT(DISTINCT CASE WHEN event_type = 'order' THEN user_id END) AS order_user_count,
        COUNT(DISTINCT CASE WHEN event_type = 'payment' THEN user_id END) AS paid_user_count
    FROM dwd_product_events
    GROUP BY store_id, item_id
) t
```


## 2. 商品区间分层分析

### 2.1 商品价格带分层分析
可以按照商品价格区间对商品进行分层，分析不同价格带商品的销售表现

- 维度：店铺(store_id)、类目(category_id)、价格区间(price_range)
    - 每个价格区间内的商品销售数据
        - 1. 动销商品数（有支付行为的商品数）
        - 2. 支付金额
        - 3. 支付件数
        - 4. 件单价（支付金额/支付件数）

- 价格区间划分（示例）：
    - 低价位：0-50元
    - 中低价位：51-100元
    - 中价位：101-200元
    - 中高价位：201-500元
    - 高价位：500元以上

```sql
-- 示例SQL：按价格带分析商品销售情况
SELECT 
    store_id,
    category_id,
    price_range,
    COUNT(DISTINCT item_id) AS active_item_count,
    SUM(paid_amount) AS total_paid_amount,
    SUM(paid_quantity) AS total_paid_quantity,
    CASE 
        WHEN SUM(paid_quantity) > 0 
        THEN ROUND(SUM(paid_amount) / SUM(paid_quantity), 2) 
        ELSE 0 
    END AS avg_price_per_item
FROM (
    SELECT 
        pe.store_id,
        pe.item_id,
        p.category_id,
        p.price,
        pe.paid_amount,
        pe.paid_quantity,
        CASE 
            WHEN p.price <= 50 THEN '0-50元'
            WHEN p.price <= 100 THEN '51-100元'
            WHEN p.price <= 200 THEN '101-200元'
            WHEN p.price <= 500 THEN '201-500元'
            ELSE '500元以上'
        END AS price_range
    FROM dwd_product_events pe
    JOIN dim_product p ON pe.item_id = p.product_id
    WHERE pe.event_type = 'payment'
) t
GROUP BY store_id, category_id, price_range
ORDER BY store_id, category_id, 
    CASE price_range
        WHEN '0-50元' THEN 1
        WHEN '51-100元' THEN 2
        WHEN '101-200元' THEN 3
        WHEN '201-500元' THEN 4
        WHEN '500元以上' THEN 5
    END
```


### 2.2 商品支付件数分层分析
按照商品支付件数对商品进行分层，了解不同销量层级商品的分布情况

#### 2.2.1 支付件数区间分析
针对不同支付件数区间的商品进行统计分析，为商品运营提供参考

- 维度：店铺(store_id)、类目(category_id)、支付件数区间(sales_volume_range)
    - 每个支付件数区间内的商品销售数据
        - 1. 动销商品数
        - 2. 支付金额
        - 3. 支付件数
        - 4. 件单价
        - 5. 占总支付金额的比例

- 支付件数区间划分（示例）：
    - 低销量：0-50件
    - 中低销量：51-100件
    - 中销量：101-150件
    - 中高销量：151-300件
    - 高销量：300件以上

```sql
-- 示例SQL：按支付件数分析商品销售情况
SELECT 
    store_id,
    category_id,
    sales_volume_range,
    COUNT(DISTINCT item_id) AS active_item_count,
    SUM(paid_amount) AS total_paid_amount,
    SUM(paid_quantity) AS total_paid_quantity,
    CASE 
        WHEN SUM(paid_quantity) > 0 
        THEN ROUND(SUM(paid_amount) / SUM(paid_quantity), 2) 
        ELSE 0 
    END AS avg_price_per_item,
    ROUND(SUM(paid_amount) * 100.0 / SUM(SUM(paid_amount)) OVER (PARTITION BY store_id), 2) AS percentage_of_total
FROM (
    SELECT 
        pe.store_id,
        pe.item_id,
        p.category_id,
        SUM(pe.paid_amount) AS paid_amount,
        SUM(pe.paid_quantity) AS paid_quantity,
        CASE 
            WHEN SUM(pe.paid_quantity) <= 50 THEN '0-50件'
            WHEN SUM(pe.paid_quantity) <= 100 THEN '51-100件'
            WHEN SUM(pe.paid_quantity) <= 150 THEN '101-150件'
            WHEN SUM(pe.paid_quantity) <= 300 THEN '151-300件'
            ELSE '300件以上'
        END AS sales_volume_range
    FROM dwd_product_events pe
    JOIN dim_product p ON pe.item_id = p.product_id
    WHERE pe.event_type = 'payment'
    GROUP BY pe.store_id, pe.item_id, p.category_id
) t
GROUP BY store_id, category_id, sales_volume_range
ORDER BY store_id, category_id, 
    CASE sales_volume_range
        WHEN '0-50件' THEN 1
        WHEN '51-100件' THEN 2
        WHEN '101-150件' THEN 3
        WHEN '151-300件' THEN 4
        WHEN '300件以上' THEN 5
    END
```


## 3. 核心指标字典

### 3.1 流量指标
| 指标名称 | 定义 | 计算逻辑 |
|---------|------|----------|
| 商品访客数 | 统计周期内访问商品详情页的去重人数 | COUNT(DISTINCT user_id) WHERE event_type = 'view' |
| 商品浏览量 | 统计周期内商品详情页被浏览的次数 | COUNT(*) WHERE event_type = 'view' |
| 商品平均停留时长 | 所有访客总的停留时长/访客数 | 总停留时长/COUNT(DISTINCT user_id) |
| 商品详情页跳出率 | 没有发生点击行为的人数/访客数 | (访客数-点击用户数)/访客数 |

### 3.2 互动指标
| 指标名称 | 定义 | 计算逻辑 |
|---------|------|----------|
| 商品收藏人数 | 统计周期内新增点击收藏商品的去重人数 | COUNT(DISTINCT user_id) WHERE event_type = 'collect' |
| 商品加购件数 | 统计周期内新增加入购物车的商品件数总和 | SUM(quantity) WHERE event_type = 'cart' |
| 商品加购人数 | 统计周期内新增加购商品的去重人数 | COUNT(DISTINCT user_id) WHERE event_type = 'cart' |
| 访问收藏转化率 | 收藏人数/访客数 | 商品收藏人数/商品访客数 |
| 访问加购转化率 | 加购人数/访客数 | 商品加购人数/商品访客数 |

### 3.3 交易指标
| 指标名称 | 定义 | 计算逻辑 |
|---------|------|----------|
| 下单买家数 | 拍下商品的去重买家人数 | COUNT(DISTINCT user_id) WHERE event_type = 'order' |
| 下单件数 | 商品被拍下的累计件数 | SUM(quantity) WHERE event_type = 'order' |
| 下单金额 | 商品被拍下的累计金额 | SUM(amount) WHERE event_type = 'order' |
| 下单转化率 | 下单买家数/访客数 | 下单买家数/商品访客数 |
| 支付买家数 | 完成支付的去重买家人数 | COUNT(DISTINCT user_id) WHERE event_type = 'payment' |
| 支付件数 | 买家完成支付的商品数量 | SUM(quantity) WHERE event_type = 'payment' |
| 支付金额 | 买家支付的金额 | SUM(amount) WHERE event_type = 'payment' |
| 支付转化率 | 支付买家数/访客数 | 支付买家数/商品访客数 |
| 客单价 | 平均每个支付买家的支付金额 | 支付金额/支付买家数 |
| 访客平均价值 | 平均每个访客可能带来的支付金额 | 支付金额/商品访客数 |

---

## 4. 总结

本文档按照重构后的模板，详细描述了商品主题宏观监控看板的核心分析维度和指标计算逻辑。
通过商品效率监控和商品区间分层分析两大模块，能够帮助电商商家全面了解商品运营状况，
及时发现异常并进行调整。文档中的SQL示例为后续开发提供了具体的实现思路。