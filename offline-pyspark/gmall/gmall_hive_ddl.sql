
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_activity_info (
    `id` BIGINT COMMENT '�id',
    `activity_name` STRING COMMENT '�����',
    `activity_type` STRING COMMENT '����ͣ�1��������2���ۿۣ�',
    `activity_desc` STRING COMMENT '�����',
    `start_time` STRING COMMENT '��ʼʱ��',
    `end_time` STRING COMMENT '����ʱ��',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '���'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_activity_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_activity_rule (
    `id` INT COMMENT '���',
    `activity_id` INT COMMENT '�id',
    `activity_type` STRING COMMENT '�����',
    `condition_amount` DECIMAL(16,2) COMMENT '�������',
    `condition_num` BIGINT COMMENT '��������',
    `benefit_amount` DECIMAL(16,2) COMMENT '�Żݽ��',
    `benefit_discount` DECIMAL(10,2) COMMENT '�Ż��ۿ�',
    `benefit_level` BIGINT COMMENT '�Żݼ���',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_activity_rule/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_activity_sku (
    `id` BIGINT COMMENT '���',
    `activity_id` BIGINT COMMENT '�id ',
    `sku_id` BIGINT COMMENT 'sku_id',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '���Ʒ������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_activity_sku/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_attr_info (
    `id` BIGINT COMMENT '���',
    `attr_name` STRING COMMENT '��������',
    `category_id` BIGINT COMMENT '����id',
    `category_level` INT COMMENT '����㼶',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '���Ա�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_attr_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_attr_value (
    `id` BIGINT COMMENT '���',
    `value_name` STRING COMMENT '����ֵ����',
    `attr_id` BIGINT COMMENT '����id',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '����ֵ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_attr_value/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_category1 (
    `id` BIGINT COMMENT 'һ��Ʒ��id',
    `name` STRING COMMENT 'һ��Ʒ������',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'һ��Ʒ���'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_category1/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_category2 (
    `id` BIGINT COMMENT '����Ʒ��id',
    `name` STRING COMMENT '����Ʒ������',
    `category1_id` BIGINT COMMENT 'һ��Ʒ����',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '����Ʒ���'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_category2/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_category3 (
    `id` BIGINT COMMENT '����Ʒ��id',
    `name` STRING COMMENT '����Ʒ������',
    `category2_id` BIGINT COMMENT '����Ʒ����',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '����Ʒ���'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_category3/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_dic (
    `dic_code` STRING COMMENT '���',
    `dic_name` STRING COMMENT '��������',
    `parent_code` STRING COMMENT '�����',
    `create_time` STRING COMMENT '��������',
    `operate_time` STRING COMMENT '�޸�����'
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_dic/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_frontend_param (
    `id` BIGINT COMMENT '���',
    `code` STRING COMMENT '��������',
    `delete_id` BIGINT COMMENT '����id',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'ǰ�����ݱ�����'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_frontend_param/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_province (
    `id` BIGINT COMMENT 'id',
    `name` STRING COMMENT 'ʡ������',
    `region_id` STRING COMMENT '����id',
    `area_code` STRING COMMENT '��������',
    `iso_code` STRING COMMENT '�ɰ���ʱ�׼�������룬�����ӻ�ʹ��',
    `iso_3166_2` STRING COMMENT '�°���ʱ�׼�������룬�����ӻ�ʹ��',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'ʡ�ݱ�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_province/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_region (
    `id` STRING COMMENT '����id',
    `region_name` STRING COMMENT '��������',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING
) COMMENT '������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_region/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_sale_attr (
    `id` BIGINT COMMENT '���',
    `name` STRING COMMENT '������������',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '�����������Ա�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_sale_attr/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_trademark (
    `id` BIGINT COMMENT '���',
    `tm_name` STRING COMMENT 'Ʒ������',
    `logo_url` STRING COMMENT 'Ʒ��logo��ͼƬ·��',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'Ʒ�Ʊ�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_trademark/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_cart_info (
    `id` BIGINT COMMENT '���',
    `user_id` STRING COMMENT '�û�id',
    `sku_id` BIGINT COMMENT 'skuid',
    `cart_price` DECIMAL(10,2) COMMENT '���빺�ﳵʱ�۸�',
    `sku_num` INT COMMENT '����',
    `img_url` STRING COMMENT '��ƷͼƬ��ַ',
    `sku_name` STRING COMMENT 'sku���� (����)',
    `is_checked` INT COMMENT '�Ƿ�ѡ��',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��',
    `is_ordered` BIGINT COMMENT '�Ƿ��Ѿ��µ�',
    `order_time` STRING COMMENT '�µ�ʱ��'
) COMMENT '���ﳵ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_cart_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_cms_banner (
    `id` BIGINT COMMENT 'ID',
    `title` STRING COMMENT '����',
    `image_url` STRING COMMENT 'ͼƬ��ַ',
    `link_url` STRING COMMENT '���ӵ�ַ',
    `sort` INT COMMENT '����',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '��ҳbanner��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_cms_banner/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_comment_info (
    `id` BIGINT COMMENT '���',
    `user_id` BIGINT COMMENT '�û�id',
    `nick_name` STRING COMMENT '�û��ǳ�',
    `head_img` STRING,
    `sku_id` BIGINT COMMENT 'skuid',
    `spu_id` BIGINT COMMENT 'spuid',
    `order_id` BIGINT COMMENT '�������',
    `appraise` STRING COMMENT '���� 1 ���� 2 ���� 3 ����',
    `comment_txt` STRING COMMENT '��������',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '��Ʒ���۱�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_comment_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_coupon_info (
    `id` BIGINT COMMENT '����ȯ���',
    `coupon_name` STRING COMMENT '����ȯ����',
    `coupon_type` STRING COMMENT '����ȯ���� 1 �ֽ�ȯ 2 �ۿ�ȯ 3 ����ȯ 4 ��������ȯ',
    `condition_amount` DECIMAL(10,2) COMMENT '������',
    `condition_num` BIGINT COMMENT '������',
    `activity_id` BIGINT COMMENT '����',
    `benefit_amount` DECIMAL(16,2) COMMENT '������',
    `benefit_discount` DECIMAL(10,2) COMMENT '�ۿ�',
    `create_time` STRING COMMENT '����ʱ��',
    `range_type` STRING COMMENT '��Χ���� 1����Ʒ(spuid) 2��Ʒ��(��������id) 3��Ʒ��',
    `limit_num` INT COMMENT '������ô���',
    `taken_count` INT COMMENT '�����ô���',
    `start_time` STRING COMMENT '������ȡ�Ŀ�ʼʱ��',
    `end_time` STRING COMMENT '������ȡ�Ľ���ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��',
    `expire_time` STRING COMMENT '����ʱ��',
    `range_desc` STRING COMMENT '��Χ����'
) COMMENT '�Ż�ȯ��Ϣ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_coupon_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_coupon_range (
    `id` BIGINT COMMENT '����ȯ���',
    `coupon_id` BIGINT COMMENT '�Ż�ȯid',
    `range_type` STRING COMMENT '��Χ���� 1����Ʒ(spuid) 2��Ʒ��(��������id) 3��Ʒ��',
    `range_id` BIGINT,
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '�Ż�ȯ��Χ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_coupon_range/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_coupon_use (
    `id` BIGINT COMMENT '���',
    `coupon_id` BIGINT COMMENT '����ȯID',
    `user_id` BIGINT COMMENT '�û�ID',
    `order_id` BIGINT COMMENT '����ID',
    `coupon_status` STRING COMMENT '����ȯ״̬��1��δʹ�� 2����ʹ�ã�',
    `get_time` STRING COMMENT '��ȡʱ��',
    `using_time` STRING COMMENT 'ʹ��ʱ��',
    `used_time` STRING COMMENT '֧��ʱ��',
    `expire_time` STRING COMMENT '����ʱ��',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '�Ż�ȯ���ñ�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_coupon_use/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_favor_info (
    `id` BIGINT COMMENT '���',
    `user_id` BIGINT COMMENT '�û�id',
    `sku_id` BIGINT COMMENT 'skuid',
    `spu_id` BIGINT COMMENT 'spuid',
    `is_cancel` STRING COMMENT '�Ƿ���ȡ�� 0 ���� 1 ��ȡ��',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '��Ʒ�ղر�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_favor_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_financial_sku_cost (
    `id` STRING,
    `sku_id` BIGINT COMMENT 'sku_id',
    `sku_name` STRING COMMENT 'sku����',
    `busi_date` STRING COMMENT 'ҵ������',
    `is_lastest` STRING COMMENT '�Ƿ����',
    `sku_cost` DECIMAL(16,2) COMMENT '��Ʒ����ɱ�',
    `create_time` STRING COMMENT '����ʱ��'
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_financial_sku_cost/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_detail (
    `id` BIGINT COMMENT '���',
    `order_id` BIGINT COMMENT '����id',
    `sku_id` BIGINT COMMENT 'sku_id',
    `sku_name` STRING COMMENT 'sku���ƣ�����)',
    `img_url` STRING COMMENT 'ͼƬ���ӣ�����)',
    `order_price` DECIMAL(10,2) COMMENT '����۸�(�µ�ʱsku�۸�',
    `sku_num` BIGINT COMMENT '�������',
    `create_time` STRING COMMENT '����ʱ��',
    `split_total_amount` DECIMAL(16,2),
    `split_activity_amount` DECIMAL(16,2),
    `split_coupon_amount` DECIMAL(16,2),
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '������ϸ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_detail/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_detail_activity (
    `id` BIGINT COMMENT '���',
    `order_id` BIGINT COMMENT '����id',
    `order_detail_id` BIGINT COMMENT '������ϸid',
    `activity_id` BIGINT COMMENT '�id',
    `activity_rule_id` BIGINT COMMENT '�����id',
    `sku_id` BIGINT COMMENT 'skuid',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '������ϸ�������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_detail_activity/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_detail_coupon (
    `id` BIGINT COMMENT '���',
    `order_id` BIGINT COMMENT '����id',
    `order_detail_id` BIGINT COMMENT '������ϸid',
    `coupon_id` BIGINT COMMENT '����ȯid',
    `coupon_use_id` BIGINT COMMENT '����ȯ����id',
    `sku_id` BIGINT COMMENT 'skuid',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '������ϸ�Ż�ȯ������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_detail_coupon/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_info (
    `id` BIGINT COMMENT '���',
    `consignee` STRING COMMENT '�ջ���',
    `consignee_tel` STRING COMMENT '�ռ��˵绰',
    `total_amount` DECIMAL(10,2) COMMENT '�ܽ��',
    `order_status` STRING COMMENT '����״̬',
    `user_id` BIGINT COMMENT '�û�id',
    `payment_way` STRING COMMENT '���ʽ',
    `delivery_address` STRING COMMENT '�ͻ���ַ',
    `order_comment` STRING COMMENT '������ע',
    `out_trade_no` STRING COMMENT '�������ױ�ţ�������֧����)',
    `trade_body` STRING COMMENT '��������(������֧����)',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '����ʱ��',
    `expire_time` STRING COMMENT 'ʧЧʱ��',
    `process_status` STRING COMMENT '����״̬',
    `tracking_no` STRING COMMENT '���������',
    `parent_order_id` BIGINT COMMENT '���������',
    `img_url` STRING COMMENT 'ͼƬ����',
    `province_id` INT COMMENT 'ʡ��id',
    `activity_reduce_amount` DECIMAL(16,2) COMMENT '�������',
    `coupon_reduce_amount` DECIMAL(16,2) COMMENT '�Ż�ȯ������',
    `original_total_amount` DECIMAL(16,2) COMMENT 'ԭʼ�ܽ��',
    `feight_fee` DECIMAL(16,2) COMMENT '�˷ѽ��',
    `feight_fee_reduce` DECIMAL(16,2) COMMENT '�˷Ѽ�����',
    `refundable_time` STRING COMMENT '���˿�ʱ�䣨ǩ�պ�30�죩'
) COMMENT '������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_refund_info (
    `id` BIGINT COMMENT '���',
    `user_id` BIGINT COMMENT '�û�id',
    `order_id` BIGINT COMMENT '����id',
    `sku_id` BIGINT COMMENT 'skuid',
    `refund_type` STRING COMMENT '�˿�����',
    `refund_num` BIGINT COMMENT '�˻�����',
    `refund_amount` DECIMAL(16,2) COMMENT '�˿���',
    `refund_reason_type` STRING COMMENT 'ԭ������',
    `refund_reason_txt` STRING COMMENT 'ԭ������',
    `refund_status` STRING COMMENT '�˿�״̬��0�������� 1�����˿',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '�˵���'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_refund_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_status_log (
    `id` BIGINT COMMENT '���',
    `order_id` BIGINT COMMENT '����id',
    `order_status` STRING COMMENT '����״̬',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '����״̬��ˮ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_status_log/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_payment_info (
    `id` INT COMMENT '���',
    `out_trade_no` STRING COMMENT '����ҵ����',
    `order_id` BIGINT COMMENT '����id',
    `user_id` BIGINT COMMENT '�û�id',
    `payment_type` STRING COMMENT '֧�����ͣ�΢�� ֧������',
    `trade_no` STRING COMMENT '���ױ��',
    `total_amount` DECIMAL(10,2) COMMENT '֧�����',
    `subject` STRING COMMENT '��������',
    `payment_status` STRING COMMENT '֧��״̬',
    `create_time` STRING COMMENT '����ʱ��',
    `callback_time` STRING COMMENT '�ص�ʱ��',
    `callback_content` STRING COMMENT '�ص���Ϣ',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '֧����Ϣ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_payment_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_promotion_pos (
    `id` BIGINT COMMENT 'Ӫ����λid',
    `pos_location` STRING COMMENT 'Ӫ����λλ��',
    `pos_type` STRING COMMENT 'Ӫ����λ���ͣ�banner,����,�б�, �ٲ�',
    `promotion_type` STRING COMMENT 'Ӫ�����ͣ��㷨���̶�������',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'Ӫ����λ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_promotion_pos/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_promotion_refer (
    `id` BIGINT COMMENT '�ⲿӪ������id',
    `refer_name` STRING COMMENT '�ⲿӪ����������',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'Ӫ��������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_promotion_refer/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_refund_payment (
    `id` INT COMMENT '���',
    `out_trade_no` STRING COMMENT '����ҵ����',
    `order_id` BIGINT COMMENT '����id',
    `sku_id` BIGINT COMMENT 'skuid',
    `payment_type` STRING COMMENT '֧�����ͣ�΢�� ֧������',
    `trade_no` STRING COMMENT '���ױ��',
    `total_amount` DECIMAL(10,2) COMMENT '�˿���',
    `subject` STRING COMMENT '��������',
    `refund_status` STRING COMMENT '�˿�״̬',
    `create_time` STRING COMMENT '����ʱ��',
    `callback_time` STRING COMMENT '�ص�ʱ��',
    `callback_content` STRING COMMENT '�ص���Ϣ',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '�˿�֧����'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_refund_payment/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_seckill_goods (
    `id` BIGINT,
    `spu_id` BIGINT COMMENT 'spu_id',
    `sku_id` BIGINT COMMENT 'sku_id',
    `sku_name` STRING COMMENT '����',
    `sku_default_img` STRING COMMENT '��ƷͼƬ',
    `price` DECIMAL(10,2) COMMENT 'ԭ�۸�',
    `cost_price` DECIMAL(10,2) COMMENT '��ɱ�۸�',
    `create_time` STRING COMMENT '�������',
    `check_time` STRING COMMENT '�������',
    `status` STRING COMMENT '���״̬',
    `start_time` STRING COMMENT '��ʼʱ��',
    `end_time` STRING COMMENT '����ʱ��',
    `num` INT COMMENT '��ɱ��Ʒ��',
    `stock_count` INT COMMENT 'ʣ������',
    `sku_desc` STRING COMMENT '����'
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_seckill_goods/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_sku_attr_value (
    `id` BIGINT COMMENT '���',
    `attr_id` BIGINT COMMENT 'ƽ̨����id������)',
    `value_id` BIGINT COMMENT 'ƽ̨����ֵid',
    `sku_id` BIGINT COMMENT 'skuid',
    `attr_name` STRING COMMENT 'ƽ̨��������',
    `value_name` STRING COMMENT 'ƽ̨����ֵ����',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'skuƽ̨����ֵ������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_sku_attr_value/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_sku_image (
    `id` BIGINT COMMENT '���',
    `sku_id` BIGINT COMMENT 'skuid',
    `img_name` STRING COMMENT 'ͼƬ���ƣ����ࣩ',
    `img_url` STRING COMMENT 'ͼƬ·��(����)',
    `spu_img_id` BIGINT COMMENT 'spuͼƬid',
    `is_default` STRING COMMENT '�Ƿ�Ĭ��',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '��浥ԪͼƬ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_sku_image/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_sku_info (
    `id` BIGINT COMMENT 'skuid',
    `spu_id` BIGINT COMMENT 'spuid',
    `price` DECIMAL(10,0) COMMENT '�۸�',
    `sku_name` STRING COMMENT 'sku����',
    `sku_desc` STRING COMMENT '��Ʒ�������',
    `weight` DECIMAL(10,2) COMMENT '����',
    `tm_id` BIGINT COMMENT 'Ʒ��id(����)',
    `category3_id` BIGINT COMMENT '����Ʒ��id������)',
    `sku_default_img` STRING COMMENT 'Ĭ����ʾͼƬ��ַ(����)',
    `is_sale` INT COMMENT '�Ƿ����ۣ�1���� 0����',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'sku��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_sku_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_sku_sale_attr_value (
    `id` BIGINT COMMENT '���',
    `sku_id` BIGINT COMMENT 'sku_id',
    `spu_id` INT COMMENT 'spu_id(����)',
    `sale_attr_value_id` BIGINT COMMENT '��������ֵid',
    `sale_attr_id` BIGINT COMMENT '��������id',
    `sale_attr_name` STRING COMMENT '������������',
    `sale_attr_value_name` STRING COMMENT '��������ֵ����',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'sku��������ֵ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_sku_sale_attr_value/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_spu_image (
    `id` BIGINT COMMENT '���',
    `spu_id` BIGINT COMMENT '��Ʒid',
    `img_name` STRING COMMENT 'ͼƬ����',
    `img_url` STRING COMMENT 'ͼƬ·��',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'spuͼƬ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_spu_image/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_spu_info (
    `id` BIGINT COMMENT 'spu_id',
    `spu_name` STRING COMMENT 'spu����',
    `description` STRING COMMENT '������Ϣ',
    `category3_id` BIGINT COMMENT '����Ʒ��id',
    `tm_id` BIGINT COMMENT 'Ʒ��id',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'spu��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_spu_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_spu_poster (
    `id` BIGINT COMMENT '���',
    `spu_id` BIGINT COMMENT '��Ʒid',
    `img_name` STRING COMMENT '�ļ�����',
    `img_url` STRING COMMENT '�ļ�·��',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '����ʱ��',
    `is_deleted` INT COMMENT '�߼�ɾ�� 1��true����ɾ���� 0��false��δɾ��'
) COMMENT '��Ʒ������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_spu_poster/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_spu_sale_attr (
    `id` BIGINT COMMENT '���(ҵ�����޹���)',
    `spu_id` BIGINT COMMENT '��Ʒid',
    `base_sale_attr_id` BIGINT COMMENT '��������id',
    `sale_attr_name` STRING COMMENT '������������(����)',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'spu�������Ա�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_spu_sale_attr/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_spu_sale_attr_value (
    `id` BIGINT COMMENT '��������ֵ���',
    `spu_id` BIGINT COMMENT 'spuid',
    `base_sale_attr_id` BIGINT COMMENT '��������id',
    `sale_attr_value_name` STRING COMMENT '��������ֵ����',
    `sale_attr_name` STRING COMMENT '������������(����)',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT 'spu��������ֵ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_spu_sale_attr_value/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_user_address (
    `id` BIGINT COMMENT '���',
    `user_id` BIGINT COMMENT '�û�id',
    `province_id` BIGINT COMMENT 'ʡ��id',
    `user_address` STRING COMMENT '�û���ַ',
    `consignee` STRING COMMENT '�ռ���',
    `phone_num` STRING COMMENT '��ϵ��ʽ',
    `is_default` STRING COMMENT '�Ƿ���Ĭ��',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��'
) COMMENT '�û���ַ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_user_address/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_user_info (
    `id` BIGINT COMMENT '���',
    `login_name` STRING COMMENT '�û�����',
    `nick_name` STRING COMMENT '�û��ǳ�',
    `passwd` STRING COMMENT '�û�����',
    `name` STRING COMMENT '�û�����',
    `phone_num` STRING COMMENT '�ֻ���',
    `email` STRING COMMENT '����',
    `head_img` STRING COMMENT 'ͷ��',
    `user_level` STRING COMMENT '�û�����',
    `birthday` STRING COMMENT '�û�����',
    `gender` STRING COMMENT '�Ա� M��,FŮ',
    `create_time` STRING COMMENT '����ʱ��',
    `operate_time` STRING COMMENT '�޸�ʱ��',
    `status` STRING COMMENT '״̬'
) COMMENT '�û���'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_user_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_ware_info (
    `id` BIGINT,
    `name` STRING,
    `address` STRING,
    `areacode` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_ware_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_ware_order_task (
    `id` BIGINT COMMENT '���',
    `order_id` BIGINT COMMENT '�������',
    `consignee` STRING COMMENT '�ջ���',
    `consignee_tel` STRING COMMENT '�ջ��˵绰',
    `delivery_address` STRING COMMENT '�ͻ���ַ',
    `order_comment` STRING COMMENT '������ע',
    `payment_way` STRING COMMENT '���ʽ 1:���߸��� 2:��������',
    `task_status` STRING COMMENT '������״̬',
    `order_body` STRING COMMENT '��������',
    `tracking_no` STRING COMMENT '��������',
    `create_time` STRING COMMENT '����ʱ��',
    `ware_id` BIGINT COMMENT '�ֿ���',
    `task_comment` STRING COMMENT '��������ע'
) COMMENT '��湤������ ��湤������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_ware_order_task/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_ware_order_task_detail (
    `id` BIGINT COMMENT '���',
    `sku_id` BIGINT COMMENT 'sku_id',
    `sku_name` STRING COMMENT 'sku����',
    `sku_num` INT COMMENT '�������',
    `task_id` BIGINT COMMENT '���������',
    `refund_status` STRING
) COMMENT '��湤������ϸ�� ��湤������ϸ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_ware_order_task_detail/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_ware_sku (
    `id` BIGINT COMMENT '���',
    `sku_id` BIGINT COMMENT 'skuid',
    `warehouse_id` BIGINT COMMENT '�ֿ�id',
    `stock` INT COMMENT '�����',
    `stock_name` STRING COMMENT '�������',
    `stock_locked` INT COMMENT '���������'
) COMMENT 'sku��ֿ������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_ware_sku/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_z_log (
    `id` BIGINT,
    `log` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_z_log/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')
