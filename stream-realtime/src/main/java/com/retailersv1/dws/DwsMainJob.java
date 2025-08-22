package com.retailersv1.dws;

import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: zxk_git
 * @BelongsPackage: com.retailersv1.dws
 * @Author: zhuxiangkuan
 * @CreateTime: 2025-08-22  14:25
 * @Description: TODO
 * @Version: 1.0
 */

public class DwsMainJob {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 明确设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 如果需要checkpointing，明确配置
//        env.enableCheckpointing(10000); // 10秒间隔

        // 设置检查点存储位置（使用内存或文件系统）
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");
        // 或者使用内存存储（仅用于开发测试）
        // env.getCheckpointConfig().setCheckpointStorage("memory://");

        EnvironmentSettingUtils.defaultParameter(env);

        System.out.println("🚀 启动 DWS 层数据处理作业...");

        // 添加Kafka连接性测试
        String bootstrapServers = com.stream.common.utils.ConfigUtils.getString("kafka.bootstrap.servers");
        System.out.println("Kafka Bootstrap Servers: " + bootstrapServers);

        // 测试各主题是否存在
        String pageTopic = com.stream.common.utils.ConfigUtils.getString("kafka.dwd.page.topic");
        String orderDetailTopic = com.stream.common.utils.ConfigUtils.getString("kafka.dwd.order.detail.topic");

        System.out.println("检查Kafka主题是否存在...");
        System.out.println("Page Topic: " + pageTopic + " exists: " +
                KafkaUtils.kafkaTopicExists(bootstrapServers, pageTopic));
        System.out.println("Order Detail Topic: " + orderDetailTopic + " exists: " +
                KafkaUtils.kafkaTopicExists(bootstrapServers, orderDetailTopic));


        // 处理流量域数据
        System.out.println("=== 开始处理流量域数据 ===");
        new DwsTrafficSourceKeywordJob().process(env);

        // 处理交易域数据
        System.out.println("=== 开始处理交易域数据 ===");
        new DwsTradeSkuOrderJob().process(env);

        // 处理用户域数据
        System.out.println("=== 开始处理用户域数据 ===");
        new DwsUserLoginJob().process(env);

        System.out.println("🏁 准备提交Flink作业...");
        env.execute("DWS-Main-Job");
    }
}
