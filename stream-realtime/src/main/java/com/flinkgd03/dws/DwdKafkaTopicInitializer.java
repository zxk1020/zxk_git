package com.flinkgd03.dws;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;

/**
 * @BelongsProject: zxk_git
 * @BelongsPackage: com.retailersv1.dwd
 * @Author: zhuxiangkuan
 * @CreateTime: 2025-08-21  16:33
 * @Description: TODO DWD层Kafka主题初始化类 用于创建DWD层所需的Kafka主题
 * @Version: 1.0
 */


public class DwdKafkaTopicInitializer {

    public static void createDwdTopics() {
        String bootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");

        // 创建DWD层Kafka主题
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.start.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.page.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.action.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.display.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.err.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.order.detail.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.cart.add.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.order.cancel.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.payment.success.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.refund.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.comment.info.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.user.register.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.toolCoupon.get.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.toolCoupon.use.topic"), 2, (short) 1, false);
        //ods
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.user.visit.log.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.product.interaction.log.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.order.pay.log.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.search.log.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.page.click.log.topic"), 2, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.crowd.attribute.log.topic"), 2, (short) 1, false);
        //dwd
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.user.visit.log.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.product.interaction.log.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.order.pay.log.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.search.log.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.page.click.log.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.crowd.attribute.log.topic"), 2, (short) 1, false);
//        FlinkGd03_ods_all_data

//        FlinkGd03_ods_user_visit_log
//        FlinkGd03_ods_product_interaction_log
//        FlinkGd03_ods_order_pay_log
//        FlinkGd03_ods_search_log
//        FlinkGd03_ods_page_click_log
//        FlinkGd03_ods_crowd_attribute_log
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("FlinkGd03.ods.all.data.topic"), 2, (short) 1, false);
//
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dirty.topic"), 1, (short) 1, false);
//        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.err.log"), 1, (short) 1, false);
        System.out.println("DWD层Kafka主题创建完成");
    }

    public static void main(String[] args) {
        createDwdTopics();
    }
}
