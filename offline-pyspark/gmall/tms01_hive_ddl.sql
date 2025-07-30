
CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_a_template_city_distance (
    `id` BIGINT,
    `city_no1` BIGINT,
    `city_no2` BIGINT,
    `distance` BIGINT,
    `remark` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_a_template_city_distance/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_base_complex (
    `id` BIGINT,
    `complex_name` STRING,
    `province_id` BIGINT,
    `city_id` BIGINT,
    `district_id` BIGINT,
    `district_name` STRING,
    `create_time` STRING,
    `update_time` STRING,
    `is_deleted` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_base_complex/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_base_dic (
    `id` BIGINT COMMENT 'id',
    `parent_id` BIGINT COMMENT '�ϼ�id',
    `name` STRING COMMENT '����',
    `dict_code` STRING COMMENT '����',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` INT COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '�����ֵ�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_base_dic/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_base_organ (
    `id` BIGINT,
    `org_name` STRING COMMENT '��������',
    `org_level` BIGINT COMMENT '��������',
    `region_id` BIGINT COMMENT '����id��1������Ϊcity ,2������Ϊdistrict',
    `org_parent_id` BIGINT COMMENT '�ϼ�����id',
    `points` STRING COMMENT '����ξ�γ�����꼯��',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '������Χ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_base_organ/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_base_province (
    `id` BIGINT COMMENT 'id',
    `name` STRING COMMENT 'ʡ����',
    `region_id` STRING COMMENT '����id',
    `area_code` STRING COMMENT '������λ��',
    `iso_code` STRING COMMENT '���ʱ���',
    `iso_3166_2` STRING COMMENT 'ISO3166����',
    `create_time` STRING,
    `operate_time` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_base_province/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_base_region_info (
    `id` BIGINT COMMENT 'id',
    `parent_id` BIGINT COMMENT '�ϼ�id',
    `name` STRING COMMENT '����',
    `dict_code` STRING COMMENT '����',
    `short_name` STRING COMMENT '���',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` INT COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '�����ֵ�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_base_region_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_employee_info (
    `id` BIGINT COMMENT 'id',
    `username` STRING COMMENT '����',
    `password` STRING COMMENT '����',
    `real_name` STRING COMMENT '����',
    `id_card` STRING COMMENT '���֤��',
    `phone` STRING COMMENT '�ֻ�',
    `birthday` STRING COMMENT '����',
    `gender` STRING COMMENT '�Ա�',
    `address` STRING COMMENT '��ַ',
    `employment_date` STRING COMMENT '��ְ����',
    `graduation_date` STRING COMMENT '��ҵʱ��',
    `education` STRING COMMENT 'ѧ��',
    `position_type` STRING COMMENT '��λ���',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT 'ְԱ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_employee_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_express_courier (
    `id` BIGINT,
    `emp_id` BIGINT COMMENT 'ְԱid',
    `org_id` BIGINT COMMENT '����id',
    `working_phone` STRING COMMENT '�����绰',
    `express_type` STRING COMMENT '�������',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '���Ա��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_express_courier/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_express_courier_complex (
    `id` BIGINT,
    `courier_emp_id` BIGINT COMMENT '���Աid',
    `complex_id` BIGINT COMMENT 'С��',
    `create_time` STRING,
    `update_time` STRING,
    `is_deleted` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_express_courier_complex/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_express_task_collect (
    `id` BIGINT,
    `order_id` BIGINT COMMENT '����id',
    `status` STRING COMMENT '����״̬��1Ϊ��ִ�У���Ӧ �����ź��뽻�ӣ���2 ������ 3������ �� 4Ϊ����ɡ�5Ϊ��ȡ��  ',
    `org_id` BIGINT COMMENT '����ID',
    `org_name` STRING COMMENT '��������',
    `courier_emp_id` BIGINT COMMENT '���ԱID',
    `courier_name` STRING COMMENT '���Ա����',
    `estimated_collected_time` STRING COMMENT 'Ԥ������ʱ��',
    `estimated_commit_time` STRING COMMENT 'Ԥ������ʱ��',
    `actual_collected_time` STRING COMMENT 'ʵ������ʱ��',
    `actual_commit_time` STRING COMMENT 'ʵ���ύʱ��',
    `cancel_time` STRING COMMENT 'ȡ��ʱ��',
    `remark` STRING COMMENT '��ע',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT 'ȡ�����ɼ�������Ϣ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_express_task_collect/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_express_task_delivery (
    `id` BIGINT,
    `order_id` BIGINT COMMENT '����id',
    `status` STRING COMMENT '����״̬',
    `org_id` BIGINT COMMENT '����ID',
    `org_name` STRING COMMENT '��������',
    `courier_emp_id` BIGINT COMMENT '���ԱID',
    `courier_name` STRING COMMENT '���Ա����',
    `estimated_end_time` STRING COMMENT 'Ԥ�����ʱ��',
    `start_delivery_time` STRING COMMENT 'ʵ�ʿ�ʼʱ��',
    `delivered_time` STRING COMMENT '���ʱ��',
    `is_rejected` STRING COMMENT '�Ƿ�����˻�',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT 'ȡ�����ɼ�������Ϣ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_express_task_delivery/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_line_base_info (
    `id` BIGINT,
    `name` STRING,
    `line_no` STRING COMMENT '��·���',
    `line_level` STRING COMMENT '����',
    `org_id` BIGINT COMMENT '��������',
    `transport_line_type_id` STRING COMMENT '��·����',
    `start_org_id` BIGINT COMMENT '��ʼ�ػ���id',
    `start_org_name` STRING,
    `end_org_id` BIGINT,
    `end_org_name` STRING,
    `pair_line_id` BIGINT COMMENT 'һ��һ�ص�·��',
    `distance` DECIMAL(10,2),
    `cost` DECIMAL(10,2),
    `estimated_time` INT COMMENT 'Ԥ��ʱ�䣨���ӣ�',
    `status` STRING COMMENT '״̬ 0������ 1������',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '��·������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_line_base_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_line_base_shift (
    `id` BIGINT,
    `line_id` BIGINT COMMENT '��·id',
    `start_time` STRING COMMENT '��ο�ʼʱ��',
    `driver1_emp_id` BIGINT COMMENT '��һ˾��',
    `driver2_emp_id` BIGINT COMMENT '�ڶ�˾��',
    `truck_id` BIGINT COMMENT '����',
    `pair_shift_id` BIGINT COMMENT '��԰��(ͬһ����һȥһ�ص���һ���)',
    `is_enabled` STRING COMMENT '״̬ 0������ 1������',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '��·������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_line_base_shift/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_order_cargo (
    `id` BIGINT,
    `order_id` STRING COMMENT '����id',
    `cargo_type` STRING COMMENT '��������id',
    `volume_length` INT COMMENT '��cm',
    `volume_width` INT COMMENT '��cm',
    `volume_height` INT COMMENT '��cm',
    `weight` DECIMAL(16,2) COMMENT '���� kg',
    `remark` STRING COMMENT '��ע',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '������ϸ'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_order_cargo/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_order_info (
    `id` BIGINT COMMENT 'id',
    `order_no` STRING COMMENT '������',
    `status` STRING COMMENT '����״̬',
    `collect_type` STRING COMMENT 'ȡ�����ͣ�1Ϊ�����Լģ�2Ϊ����ȡ��',
    `user_id` BIGINT COMMENT '�ͻ�id',
    `receiver_complex_id` BIGINT COMMENT '�ռ���С��id',
    `receiver_province_id` STRING COMMENT '�ռ���ʡ��id',
    `receiver_city_id` STRING COMMENT '�ռ��˳���id',
    `receiver_district_id` STRING COMMENT '�ռ�������id',
    `receiver_address` STRING COMMENT '�ռ�����ϸ��ַ',
    `receiver_name` STRING COMMENT '�ռ�������',
    `receiver_phone` STRING COMMENT '�ռ��˵绰',
    `receive_location` STRING COMMENT '��ʼ�㾭γ��',
    `sender_complex_id` BIGINT COMMENT '������С��id',
    `sender_province_id` STRING COMMENT '������ʡ��id',
    `sender_city_id` STRING COMMENT '�����˳���id',
    `sender_district_id` STRING COMMENT '����������id',
    `sender_address` STRING COMMENT '��������ϸ��ַ',
    `sender_name` STRING COMMENT '����������',
    `sender_phone` STRING COMMENT '�����˵绰',
    `send_location` STRING COMMENT '����������',
    `payment_type` STRING COMMENT '֧����ʽ',
    `cargo_num` INT COMMENT '�������',
    `amount` DECIMAL(32,2) COMMENT '���',
    `estimate_arrive_time` STRING COMMENT 'Ԥ�Ƶ���ʱ��',
    `distance` DECIMAL(10,2) COMMENT '���룬��λ������',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '��������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_order_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_order_org_bound (
    `id` BIGINT,
    `order_id` BIGINT COMMENT '��������',
    `org_id` BIGINT COMMENT '����id',
    `status` STRING COMMENT '״̬ ���� ���',
    `inbound_time` STRING COMMENT '���ʱ��',
    `inbound_emp_id` BIGINT COMMENT '�����Ա',
    `sort_time` STRING COMMENT '�ּ�ʱ��',
    `sorter_emp_id` BIGINT COMMENT '�ּ���Ա',
    `outbound_time` STRING COMMENT '����ʱ��',
    `outbound_emp_id` BIGINT COMMENT '������Ա',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '�޸�ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����־'
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_order_org_bound/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_order_trace_log (
    `id` BIGINT,
    `order_id` BIGINT COMMENT '�������',
    `trace_desc` STRING COMMENT '��������',
    `create_time` STRING COMMENT '����ʱ��',
    `cur_task_id` BIGINT COMMENT '��Դ�������id',
    `task_type` STRING COMMENT '��������:1  ���� 2 ���� 3 ��ת 4���� ',
    `charge_emp_id` BIGINT COMMENT '������id',
    `remark` STRING COMMENT '��ע',
    `is_rollback` STRING COMMENT '�Ƿ������',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '������׷����Ϣ'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_order_trace_log/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_sorter_info (
    `id` BIGINT,
    `emp_id` BIGINT COMMENT 'ְԱid',
    `org_id` BIGINT COMMENT '����id',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '�ּ�Ա'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_sorter_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_transport_plan_line_detail (
    `id` BIGINT,
    `order_id` BIGINT COMMENT '�������',
    `start_org_id` BIGINT COMMENT '��ʼ����',
    `end_org_id` BIGINT COMMENT '�յ����',
    `line_base_id` BIGINT COMMENT 'ҵ�����·id',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '�˵�·����ϸ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_transport_plan_line_detail/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_transport_task (
    `id` BIGINT,
    `shift_id` BIGINT COMMENT '����id',
    `line_id` BIGINT COMMENT '·��id',
    `start_org_id` BIGINT COMMENT '��ʼ����id',
    `start_org_name` STRING COMMENT '��ʼ��������',
    `end_org_id` BIGINT COMMENT 'Ŀ�Ļ���id',
    `end_org_name` STRING COMMENT 'Ŀ�Ļ�������',
    `status` STRING COMMENT '����״̬',
    `order_num` INT COMMENT '�˵�����',
    `driver1_emp_id` BIGINT COMMENT '˾��1id',
    `driver1_name` STRING COMMENT '˾��1����',
    `driver2_emp_id` BIGINT COMMENT '˾��2id',
    `driver2_name` STRING COMMENT '˾��2����',
    `truck_id` BIGINT COMMENT '����id',
    `truck_no` STRING COMMENT '��������',
    `actual_start_time` STRING COMMENT 'ʵ������ʱ��',
    `actual_end_time` STRING COMMENT 'ʵ�ʵ���ʱ��',
    `actual_distance` DECIMAL(16,2) COMMENT 'ʵ����ʻ����',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '���������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_transport_task/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_transport_task_detail (
    `id` BIGINT,
    `transport_task_id` BIGINT COMMENT '��������id',
    `order_id` BIGINT COMMENT '�˵�id',
    `sorter_emp_id` BIGINT COMMENT '�ּ�Աid',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '�˵����������������'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_transport_task_detail/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_transport_task_process (
    `id` BIGINT,
    `transport_task_id` BIGINT COMMENT '����id',
    `cur_distance` DECIMAL(16,2) COMMENT '��ǰ��ʻ���',
    `line_distance` DECIMAL(16,2) COMMENT '���',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_transport_task_process/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_truck_driver (
    `id` BIGINT,
    `emp_id` BIGINT COMMENT '��Ա���',
    `org_id` BIGINT COMMENT '������',
    `team_id` BIGINT COMMENT '���Ӻ�',
    `license_type` STRING COMMENT '׼�ݳ���',
    `init_license_date` STRING COMMENT '������֤����',
    `expire_date` STRING COMMENT '��Ч��ֹ����',
    `license_no` STRING COMMENT '��ʻ֤��',
    `license_picture_url` STRING COMMENT '����ͼƬ',
    `is_enabled` INT COMMENT '״̬ 0������ 1������',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '˾����ʻ֤��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_truck_driver/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_truck_info (
    `id` BIGINT COMMENT 'id',
    `team_id` BIGINT COMMENT '��������id',
    `truck_no` STRING COMMENT '���ƺ���',
    `truck_model_id` STRING COMMENT '�ͺ�',
    `device_gps_id` STRING COMMENT 'GPS�豸id',
    `engine_no` STRING COMMENT '����������',
    `license_registration_date` STRING COMMENT 'ע��ʱ��',
    `license_last_check_date` STRING COMMENT '����������',
    `license_expire_date` STRING COMMENT 'ʧЧ�¼�����',
    `picture_url` STRING COMMENT 'ͼƬ��Ϣ',
    `is_enabled` INT COMMENT '״̬ 0������ 1������',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '������Ϣ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_truck_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_truck_model (
    `id` BIGINT,
    `model_name` STRING COMMENT '�ͺ�����',
    `model_type` STRING COMMENT '����',
    `model_no` STRING COMMENT '�ͺű���',
    `brand` STRING COMMENT 'Ʒ��',
    `truck_weight` DECIMAL(10,2) COMMENT '�������� ��',
    `load_weight` DECIMAL(10,2) COMMENT '����� ��',
    `total_weight` DECIMAL(10,2) COMMENT '������ ��',
    `eev` STRING COMMENT '�ŷű�׼',
    `boxcar_len` DECIMAL(10,2) COMMENT '���䳤m',
    `boxcar_wd` DECIMAL(10,2) COMMENT '�����m',
    `boxcar_hg` DECIMAL(10,2) COMMENT '�����m',
    `max_speed` BIGINT COMMENT '���ʱ�� ǧ��ÿʱ',
    `oil_vol` BIGINT COMMENT '�����ݻ� ��',
    `create_time` STRING,
    `update_time` STRING,
    `is_deleted` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_truck_model/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_truck_team (
    `id` BIGINT COMMENT 'id',
    `name` STRING COMMENT '��������',
    `team_no` STRING COMMENT '���ӱ��',
    `org_id` BIGINT COMMENT '��������',
    `manager_emp_id` BIGINT COMMENT '������',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '���ӱ�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_truck_team/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_user_address (
    `id` BIGINT COMMENT 'id',
    `user_id` BIGINT COMMENT '�û�id',
    `phone` STRING COMMENT '�绰��',
    `province_id` BIGINT COMMENT '����ʡ��id',
    `city_id` BIGINT COMMENT '��������id',
    `district_id` BIGINT COMMENT '��������id',
    `complex_id` BIGINT COMMENT '����С��id',
    `address` STRING COMMENT '��ϸ��ַ',
    `is_default` INT COMMENT '�Ƿ�Ĭ�� 0:��1:��',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '����ʱ��',
    `is_deleted` STRING COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) COMMENT '��ַ��'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_user_address/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_user_info (
    `id` BIGINT COMMENT '���',
    `login_name` STRING COMMENT '�û�����',
    `nick_name` STRING COMMENT '�û��ǳ�',
    `passwd` STRING COMMENT '�û�����',
    `real_name` STRING COMMENT '�û�����',
    `phone_num` STRING COMMENT '�ֻ���',
    `email` STRING COMMENT '����',
    `head_img` STRING COMMENT 'ͷ��',
    `user_level` STRING COMMENT '�û�����',
    `birthday` STRING COMMENT '�û�����',
    `gender` STRING COMMENT '�Ա� M��,FŮ',
    `create_time` STRING COMMENT '����ʱ��',
    `update_time` STRING COMMENT '�޸�ʱ��',
    `is_deleted` STRING COMMENT '״̬'
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_user_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')
