package com.retailersv1.dwd;

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
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.start.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.page.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.action.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.display.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.err.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.order.detail.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.cart.add.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.order.cancel.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.payment.success.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.refund.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.comment.info.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.user.register.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.toolCoupon.get.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dwd.toolCoupon.use.topic"), 2, (short) 1, false);

        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.dirty.topic"), 1, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.err.log"), 1, (short) 1, false);
        System.out.println("DWD层Kafka主题创建完成");
    }

    public static void main(String[] args) {
        createDwdTopics();
    }
}
