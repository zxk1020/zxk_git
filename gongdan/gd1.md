-- ====================== ODS��������������======================


CREATE_TABLE_SQL_LIST = [
"""
CREATE TABLE user_info (
`user_id` BIGINT AUTO_INCREMENT COMMENT '�û�ID',
`user_name` VARCHAR(50) COMMENT '�û�����',
`gender` VARCHAR(10) COMMENT '�Ա�',
`level` VARCHAR(20) COMMENT '�û�����',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`user_id`)
) COMMENT='�û���Ϣ��';
""",
"""
CREATE TABLE product_info (
`product_id` BIGINT AUTO_INCREMENT COMMENT '��ƷID',
`product_name` VARCHAR(255) COMMENT '��Ʒ����',
`category_name` VARCHAR(100) COMMENT '��ƷƷ��',
`brand` VARCHAR(100) COMMENT 'Ʒ��',
`price` DECIMAL(10,2) COMMENT '��Ʒ�۸�',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`product_id`)
) COMMENT='��Ʒ��Ϣ��';
""",
"""
CREATE TABLE product_view_log (
`id` BIGINT AUTO_INCREMENT COMMENT '��־ID',
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`visit_time` DATETIME COMMENT '����ʱ��',
`platform` VARCHAR(20) DEFAULT NULL COMMENT 'ƽ̨��PC/���ߣ�',
`session_id` VARCHAR(100) COMMENT '�ỰID',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`id`),
CONSTRAINT `fk_view_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_view_user` FOREIGN KEY (`user_id`) REFERENCES  `user_info`  (`user_id`)
) COMMENT='��Ʒ������־��';
""",
"""
CREATE TABLE product_collect_log (
`id` BIGINT AUTO_INCREMENT COMMENT '��־ID',
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`collect_time` DATETIME COMMENT '�ղ�ʱ��',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`id`),
CONSTRAINT `fk_collect_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_collect_user` FOREIGN KEY (`user_id`) REFERENCES `user_info` (`user_id`)
) COMMENT='��Ʒ�ղ���־��';
""",
"""
CREATE TABLE product_cart_log (
`id` BIGINT AUTO_INCREMENT COMMENT '��־ID',
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`quantity` INT COMMENT '�ӹ�����',
`add_time` DATETIME COMMENT '�ӹ�ʱ��',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`id`),
CONSTRAINT `fk_cart_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_cart_user` FOREIGN KEY (`user_id`) REFERENCES `user_info`  (`user_id`)
) COMMENT='��Ʒ�ӹ���־��';
""",
"""
CREATE TABLE product_order_log (
`order_id` BIGINT AUTO_INCREMENT COMMENT '����ID',
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`quantity` INT COMMENT '�µ�����',
`amount` DECIMAL(10,2) COMMENT '�µ����',
`order_time` DATETIME COMMENT '�µ�ʱ��',
`is_paid` INT COMMENT '�Ƿ�֧��(0:�� 1:��)',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`order_id`),
CONSTRAINT `fk_order_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_order_user` FOREIGN KEY (`user_id`) REFERENCES  `user_info`  (`user_id`)
) COMMENT='��Ʒ������־��';
"""
]


-- �û���Ϣ��������
CREATE TABLE user_info (
`user_id` BIGINT AUTO_INCREMENT COMMENT '�û�ID',
`user_name` VARCHAR(50) COMMENT '�û�����',
`gender` VARCHAR(10) COMMENT '�Ա�',
`level` VARCHAR(20) COMMENT '�û�����',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`user_id`)
) COMMENT='�û���Ϣ��';

-- ��Ʒ��Ϣ��������
CREATE TABLE product_info (
`product_id` BIGINT AUTO_INCREMENT COMMENT '��ƷID',
`product_name` VARCHAR(255) COMMENT '��Ʒ����',
`category_name` VARCHAR(100) COMMENT '��ƷƷ��',
`brand` VARCHAR(100) COMMENT 'Ʒ��',
`price` DECIMAL(10,2) COMMENT '��Ʒ�۸�',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`product_id`)
) COMMENT='��Ʒ��Ϣ��';

-- ��Ʒ������־�������û�����Ʒ��
CREATE TABLE product_view_log (
`id` BIGINT AUTO_INCREMENT COMMENT '��־ID',
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`visit_time` DATETIME COMMENT '����ʱ��',
`platform` VARCHAR(20) DEFAULT NULL COMMENT 'ƽ̨��PC/���ߣ�',
`session_id` VARCHAR(100) COMMENT '�ỰID',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`id`),
CONSTRAINT `fk_view_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_view_user` FOREIGN KEY (`user_id`) REFERENCES [user_info](file://D:\zg6\zxk_git\offline-pyspark\tms\py_spark\dim.py#L481-L484) (`user_id`)
) COMMENT='��Ʒ������־��';

-- ��Ʒ�ղ���־�������û�����Ʒ��
CREATE TABLE product_collect_log (
`id` BIGINT AUTO_INCREMENT COMMENT '��־ID',
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`collect_time` DATETIME COMMENT '�ղ�ʱ��',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`id`),
CONSTRAINT `fk_collect_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_collect_user` FOREIGN KEY (`user_id`) REFERENCES [user_info](file://D:\zg6\zxk_git\offline-pyspark\tms\py_spark\dim.py#L481-L484) (`user_id`)
) COMMENT='��Ʒ�ղ���־��';

-- ��Ʒ�ӹ���־�������û�����Ʒ��
CREATE TABLE product_cart_log (
`id` BIGINT AUTO_INCREMENT COMMENT '��־ID',
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`quantity` INT COMMENT '�ӹ�����',
`add_time` DATETIME COMMENT '�ӹ�ʱ��',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`id`),
CONSTRAINT `fk_cart_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_cart_user` FOREIGN KEY (`user_id`) REFERENCES [user_info](file://D:\zg6\zxk_git\offline-pyspark\tms\py_spark\dim.py#L481-L484) (`user_id`)
) COMMENT='��Ʒ�ӹ���־��';

-- ��Ʒ������־�������û�����Ʒ��
CREATE TABLE product_order_log (
`order_id` BIGINT AUTO_INCREMENT COMMENT '����ID',
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`quantity` INT COMMENT '�µ�����',
`amount` DECIMAL(10,2) COMMENT '�µ����',
`order_time` DATETIME COMMENT '�µ�ʱ��',
`is_paid` INT COMMENT '�Ƿ�֧��(0:�� 1:��)',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`operate_time` DATETIME DEFAULT NULL COMMENT '�޸�ʱ��',
PRIMARY KEY (`order_id`),
CONSTRAINT `fk_order_product` FOREIGN KEY (`product_id`) REFERENCES `product_info` (`product_id`),
CONSTRAINT `fk_order_user` FOREIGN KEY (`user_id`) REFERENCES [user_info](file://D:\zg6\zxk_git\offline-pyspark\tms\py_spark\dim.py#L481-L484) (`user_id`)
) COMMENT='��Ʒ������־��';

-- ====================== DIM��� ======================

-- ��Ʒά�ȱ�
CREATE TABLE dim_product (
`product_id` BIGINT COMMENT '��ƷID',
`product_name` VARCHAR(255) COMMENT '��Ʒ����',
`category_name` VARCHAR(100) COMMENT '��ƷƷ��',
`brand` VARCHAR(100) COMMENT 'Ʒ��',
`description` VARCHAR(500) COMMENT '��Ʒ����',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`update_time` DATETIME DEFAULT NULL COMMENT '����ʱ��'
) COMMENT='��Ʒά�ȱ�';

-- �û�ά�ȱ�
CREATE TABLE dim_user (
`user_id` BIGINT COMMENT '�û�ID',
`user_name` VARCHAR(50) COMMENT '�û�����',
`gender` VARCHAR(10) COMMENT '�Ա�',
`level` VARCHAR(20) COMMENT '�û�����',
`create_time` DATETIME DEFAULT NULL COMMENT '����ʱ��',
`update_time` DATETIME DEFAULT NULL COMMENT '����ʱ��'
) COMMENT='�û�ά�ȱ�';

-- ƽ̨ά�ȱ�
CREATE TABLE dim_platform (
`platform_id` INT COMMENT 'ƽ̨ID',
`platform_name` VARCHAR(20) COMMENT 'ƽ̨����',
`platform_desc` VARCHAR(100) COMMENT 'ƽ̨����'
) COMMENT='ƽ̨ά�ȱ�';

-- ֧��״̬ά�ȱ�
CREATE TABLE dim_payment_status (
`status_id` INT COMMENT '֧��״̬ID',
`status_name` VARCHAR(20) COMMENT '֧��״̬����',
`status_desc` VARCHAR(100) COMMENT '״̬����'
) COMMENT='֧��״̬ά�ȱ�';




-- ��Ʒ������ʵ��
CREATE TABLE dwd_product_view (
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`visit_date` DATE COMMENT '��������',
`platform` VARCHAR(20) COMMENT 'ƽ̨��PC/���ߣ�',
`view_count` BIGINT COMMENT '�����',
`visitor_count` BIGINT COMMENT '�ÿ���'
) COMMENT='��Ʒ������ʵ��';

-- ��Ʒ�ղ���ʵ��
CREATE TABLE dwd_product_collect (
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`collect_date` DATE COMMENT '�ղ�����',
`collector_count` BIGINT COMMENT '�ղ�����'
) COMMENT='��Ʒ�ղ���ʵ��';

-- ��Ʒ�ӹ���ʵ��
CREATE TABLE dwd_product_cart (
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`cart_date` DATE COMMENT '�ӹ�����',
`cart_quantity` BIGINT COMMENT '�ӹ�����',
`cart_user_count` BIGINT COMMENT '�ӹ�����'
) COMMENT='��Ʒ�ӹ���ʵ��';

-- ��Ʒ������ʵ��
CREATE TABLE dwd_product_order (
`product_id` BIGINT COMMENT '��ƷID',
`user_id` BIGINT COMMENT '�û�ID',
`order_date` DATE COMMENT '�µ�����',
`quantity` BIGINT COMMENT '�µ�����',
`amount` DECIMAL(10,2) COMMENT '�µ����',
`paid_quantity` BIGINT COMMENT '֧������',
`paid_amount` DECIMAL(10,2) COMMENT '֧�����',
`order_user_count` BIGINT COMMENT '�µ������',
`paid_user_count` BIGINT COMMENT '֧�������'
) COMMENT='��Ʒ������ʵ��';



-- ��ƷЧ�ʻ��ܱ�
CREATE TABLE dws_product_efficiency (
`product_id` BIGINT COMMENT '��ƷID',
`stat_date` DATE COMMENT 'ͳ������',
`view_count` BIGINT COMMENT '��Ʒ�����',
`visitor_count` BIGINT COMMENT '��Ʒ�ÿ���',
`collect_count` BIGINT COMMENT '��Ʒ�ղ�����',
`cart_quantity` BIGINT COMMENT '��Ʒ�ӹ�����',
`cart_user_count` BIGINT COMMENT '��Ʒ�ӹ�����',
`order_quantity` BIGINT COMMENT '�µ�����',
`order_amount` DECIMAL(10,2) COMMENT '�µ����',
`order_user_count` BIGINT COMMENT '�µ������',
`paid_quantity` BIGINT COMMENT '֧������',
`paid_amount` DECIMAL(10,2) COMMENT '֧�����',
`paid_user_count` BIGINT COMMENT '֧�������'
) COMMENT='��ƷЧ�ʻ��ܱ�';

-- ��Ʒ���������
CREATE TABLE dws_product_range_analysis (
`category_id` BIGINT COMMENT '��ĿID',
`price_range` VARCHAR(50) COMMENT '�۸�����',
`quantity_range` VARCHAR(50) COMMENT '֧����������',
`amount_range` VARCHAR(50) COMMENT '֧���������',
`product_count` BIGINT COMMENT '��Ʒ��',
`paid_amount` DECIMAL(10,2) COMMENT '֧�����',
`paid_quantity` BIGINT COMMENT '֧������',
`avg_price` DECIMAL(10,2) COMMENT '������'
) COMMENT='��Ʒ���������';



-- ��ƷЧ�ʼ�ر�
CREATE TABLE ads_product_efficiency_monitor (
`stat_date` DATE COMMENT 'ͳ������',
`time_dimension` VARCHAR(20) COMMENT 'ʱ��ά�ȣ���/��/�£�',
`view_count` BIGINT COMMENT '��Ʒ�����',
`visitor_count` BIGINT COMMENT '��Ʒ�ÿ���',
`collect_count` BIGINT COMMENT '��Ʒ�ղ�����',
`cart_quantity` BIGINT COMMENT '��Ʒ�ӹ�����',
`cart_user_count` BIGINT COMMENT '��Ʒ�ӹ�����',
`order_quantity` BIGINT COMMENT '�µ�����',
`order_amount` DECIMAL(10,2) COMMENT '�µ����',
`order_user_count` BIGINT COMMENT '�µ������',
`paid_quantity` BIGINT COMMENT '֧������',
`paid_amount` DECIMAL(10,2) COMMENT '֧�����',
`paid_user_count` BIGINT COMMENT '֧�������',
`conversion_rate` DECIMAL(5,4) COMMENT '֧��ת����'
) COMMENT='��ƷЧ�ʼ�ر�';

-- ��Ʒ������������
CREATE TABLE ads_product_range_analysis (
`category_id` BIGINT COMMENT '��ĿID',
`category_name` VARCHAR(100) COMMENT '��Ŀ����',
`stat_date` DATE COMMENT 'ͳ������',
`time_dimension` VARCHAR(20) COMMENT 'ʱ��ά�ȣ���/��/�£�',
`range_type` VARCHAR(20) COMMENT '�������ͣ��۸�/����/��',
`range_value` VARCHAR(50) COMMENT '����ֵ',
`product_count` BIGINT COMMENT '��Ʒ��',
`paid_amount` DECIMAL(10,2) COMMENT '֧�����',
`paid_quantity` BIGINT COMMENT '֧������',
`avg_price` DECIMAL(10,2) COMMENT '������',
`sort_metric` VARCHAR(50) COMMENT '����ָ��'
) COMMENT='��Ʒ������������';



# ����������Ŀ - ��Ʒ�����ۼ�ؿ�������ĵ�

**�������**: ������-��������-01-��Ʒ�����ۼ�ؿ���  
**��Ŀ����**: ��������  
**����ʱ��**: 2025.1.14  
**������**: ���  
**�ĵ��汾**: v1.0

---

## 1. ��ƷЧ�ʼ�ط���

### 1.1 ��Ʒ�����뽻���������
���Բ�ѯ��������Ʒ�ķ��ʡ��ղء��ӹ�����������������̼��˽���Ʒ�ܻ�ӭ�̶�

- ά�ȣ�ÿ������(store_id)����Ʒ(item_id)
    - ���ʣ��Ե�������Ʒ�ķ������
    - ��������Ʒ�ղء��ӹ����û���Ϊ
    - ���ۣ���Ʒ���µ���֧�����

```sql
-- ʾ��SQL��ͳ�Ƶ�����Ʒ����ָ��
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


### 1.2 ��Ʒ����ת������
���ڷ�����Ʒ���ع⵽����֧����ת��Ч�����Ż���Ʒ��Ӫ����

#### 1.2.1 ��Ʒ������ת���ʷ���
�����Ʒ�ӷÿ͵�����֧���ĸ������ڽ���ת����ͳ�Ʒ������Ż���Ʒչʾ��Ӫ������

- ά�ȣ�����(store_id)����Ʒ(item_id)��ʱ������
    - ÿ����Ʒ��ͳ�������ڵĸ����û���Ϊ����
        - 1. ��Ʒ�ÿ��������ʹ���Ʒ����ҳ��ȥ��������
        - 2. ��Ʒ�ղ���������������ղ���Ʒ��ȥ��������
        - 3. ��Ʒ�ӹ������������ӹ���Ʒ��ȥ��������
        - 4. ��Ʒ�µ�������������Ʒ��ȥ�����������
        - 5. ��Ʒ֧�����������֧����ȥ�����������

- ָ����㣺
    - 1. �����ղ�ת���� = ��Ʒ�ղ����� / ��Ʒ�ÿ��� �� 100%
    - 2. ���ʼӹ�ת���� = ��Ʒ�ӹ����� / ��Ʒ�ÿ��� �� 100%
    - 3. �µ�ת���� = ��Ʒ�µ����� / ��Ʒ�ÿ��� �� 100%
    - 4. ֧��ת���� = ��Ʒ֧������ / ��Ʒ�ÿ��� �� 100%

```sql
-- ʾ��SQL��������Ʒ������ת����
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


## 2. ��Ʒ����ֲ����

### 2.1 ��Ʒ�۸���ֲ����
���԰�����Ʒ�۸��������Ʒ���зֲ㣬������ͬ�۸����Ʒ�����۱���

- ά�ȣ�����(store_id)����Ŀ(category_id)���۸�����(price_range)
    - ÿ���۸������ڵ���Ʒ��������
        - 1. ������Ʒ������֧����Ϊ����Ʒ����
        - 2. ֧�����
        - 3. ֧������
        - 4. �����ۣ�֧�����/֧��������

- �۸����仮�֣�ʾ������
    - �ͼ�λ��0-50Ԫ
    - �еͼ�λ��51-100Ԫ
    - �м�λ��101-200Ԫ
    - �и߼�λ��201-500Ԫ
    - �߼�λ��500Ԫ����

```sql
-- ʾ��SQL�����۸��������Ʒ�������
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
            WHEN p.price <= 50 THEN '0-50Ԫ'
            WHEN p.price <= 100 THEN '51-100Ԫ'
            WHEN p.price <= 200 THEN '101-200Ԫ'
            WHEN p.price <= 500 THEN '201-500Ԫ'
            ELSE '500Ԫ����'
        END AS price_range
    FROM dwd_product_events pe
    JOIN dim_product p ON pe.item_id = p.product_id
    WHERE pe.event_type = 'payment'
) t
GROUP BY store_id, category_id, price_range
ORDER BY store_id, category_id, 
    CASE price_range
        WHEN '0-50Ԫ' THEN 1
        WHEN '51-100Ԫ' THEN 2
        WHEN '101-200Ԫ' THEN 3
        WHEN '201-500Ԫ' THEN 4
        WHEN '500Ԫ����' THEN 5
    END
```


### 2.2 ��Ʒ֧�������ֲ����
������Ʒ֧����������Ʒ���зֲ㣬�˽ⲻͬ�����㼶��Ʒ�ķֲ����

#### 2.2.1 ֧�������������
��Բ�֧ͬ�������������Ʒ����ͳ�Ʒ�����Ϊ��Ʒ��Ӫ�ṩ�ο�

- ά�ȣ�����(store_id)����Ŀ(category_id)��֧����������(sales_volume_range)
    - ÿ��֧�����������ڵ���Ʒ��������
        - 1. ������Ʒ��
        - 2. ֧�����
        - 3. ֧������
        - 4. ������
        - 5. ռ��֧�����ı���

- ֧���������仮�֣�ʾ������
    - ��������0-50��
    - �е�������51-100��
    - ��������101-150��
    - �и�������151-300��
    - ��������300������

```sql
-- ʾ��SQL����֧������������Ʒ�������
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
            WHEN SUM(pe.paid_quantity) <= 50 THEN '0-50��'
            WHEN SUM(pe.paid_quantity) <= 100 THEN '51-100��'
            WHEN SUM(pe.paid_quantity) <= 150 THEN '101-150��'
            WHEN SUM(pe.paid_quantity) <= 300 THEN '151-300��'
            ELSE '300������'
        END AS sales_volume_range
    FROM dwd_product_events pe
    JOIN dim_product p ON pe.item_id = p.product_id
    WHERE pe.event_type = 'payment'
    GROUP BY pe.store_id, pe.item_id, p.category_id
) t
GROUP BY store_id, category_id, sales_volume_range
ORDER BY store_id, category_id, 
    CASE sales_volume_range
        WHEN '0-50��' THEN 1
        WHEN '51-100��' THEN 2
        WHEN '101-150��' THEN 3
        WHEN '151-300��' THEN 4
        WHEN '300������' THEN 5
    END
```


## 3. ����ָ���ֵ�

### 3.1 ����ָ��
| ָ������ | ���� | �����߼� |
|---------|------|----------|
| ��Ʒ�ÿ��� | ͳ�������ڷ�����Ʒ����ҳ��ȥ������ | COUNT(DISTINCT user_id) WHERE event_type = 'view' |
| ��Ʒ����� | ͳ����������Ʒ����ҳ������Ĵ��� | COUNT(*) WHERE event_type = 'view' |
| ��Ʒƽ��ͣ��ʱ�� | ���зÿ��ܵ�ͣ��ʱ��/�ÿ��� | ��ͣ��ʱ��/COUNT(DISTINCT user_id) |
| ��Ʒ����ҳ������ | û�з��������Ϊ������/�ÿ��� | (�ÿ���-����û���)/�ÿ��� |

### 3.2 ����ָ��
| ָ������ | ���� | �����߼� |
|---------|------|----------|
| ��Ʒ�ղ����� | ͳ����������������ղ���Ʒ��ȥ������ | COUNT(DISTINCT user_id) WHERE event_type = 'collect' |
| ��Ʒ�ӹ����� | ͳ���������������빺�ﳵ����Ʒ�����ܺ� | SUM(quantity) WHERE event_type = 'cart' |
| ��Ʒ�ӹ����� | ͳ�������������ӹ���Ʒ��ȥ������ | COUNT(DISTINCT user_id) WHERE event_type = 'cart' |
| �����ղ�ת���� | �ղ�����/�ÿ��� | ��Ʒ�ղ�����/��Ʒ�ÿ��� |
| ���ʼӹ�ת���� | �ӹ�����/�ÿ��� | ��Ʒ�ӹ�����/��Ʒ�ÿ��� |

### 3.3 ����ָ��
| ָ������ | ���� | �����߼� |
|---------|------|----------|
| �µ������ | ������Ʒ��ȥ��������� | COUNT(DISTINCT user_id) WHERE event_type = 'order' |
| �µ����� | ��Ʒ�����µ��ۼƼ��� | SUM(quantity) WHERE event_type = 'order' |
| �µ���� | ��Ʒ�����µ��ۼƽ�� | SUM(amount) WHERE event_type = 'order' |
| �µ�ת���� | �µ������/�ÿ��� | �µ������/��Ʒ�ÿ��� |
| ֧������� | ���֧����ȥ��������� | COUNT(DISTINCT user_id) WHERE event_type = 'payment' |
| ֧������ | ������֧������Ʒ���� | SUM(quantity) WHERE event_type = 'payment' |
| ֧����� | ���֧���Ľ�� | SUM(amount) WHERE event_type = 'payment' |
| ֧��ת���� | ֧�������/�ÿ��� | ֧�������/��Ʒ�ÿ��� |
| �͵��� | ƽ��ÿ��֧����ҵ�֧����� | ֧�����/֧������� |
| �ÿ�ƽ����ֵ | ƽ��ÿ���ÿͿ��ܴ�����֧����� | ֧�����/��Ʒ�ÿ��� |

---

## 4. �ܽ�

���ĵ������ع����ģ�壬��ϸ��������Ʒ�����ۼ�ؿ���ĺ��ķ���ά�Ⱥ�ָ������߼���
ͨ����ƷЧ�ʼ�غ���Ʒ����ֲ��������ģ�飬�ܹ����������̼�ȫ���˽���Ʒ��Ӫ״����
��ʱ�����쳣�����е������ĵ��е�SQLʾ��Ϊ���������ṩ�˾����ʵ��˼·��