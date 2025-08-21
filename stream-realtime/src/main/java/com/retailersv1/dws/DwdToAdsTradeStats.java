package com.retailersv1.dws;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.functions.MapFunction;


/**
 * @BelongsProject: zxk_git
 * @BelongsPackage: com.retailersv1.dws
 * @Author: zhuxiangkuan
 * @CreateTime: 2025-08-21  14:39
 * @Description: TODO DWD层数据汇总到ADS层 - 交易统计示例
 * @Version: 1.0
 */

public class DwdToAdsTradeStats {

    private static final String KAFKA_BOOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String DWD_TRADE_ORDER_TOPIC = ConfigUtils.getString("kafka.dwd.trade.order"); // 需要在配置文件中添加
    private static final String ADS_TRADE_STATS_TOPIC = "ads_trade_stats"; // 示例topic名

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 读取DWD层订单明细数据
        DataStreamSource<String> dwdOrderStream = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        KAFKA_BOOTSTRAP_SERVERS,
                        DWD_TRADE_ORDER_TOPIC,
                        "dwd_to_ads_group",
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_dwd_order_detail"
        );

        // 解析并转换为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonStream = dwdOrderStream.map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSONObject.parseObject(value);
                    }
                }).uid("dwd_order_parse_json")
                .name("dwd_order_parse_json");

        // TODO: 添加业务逻辑处理，如按时间窗口聚合、维度补全等
        // 示例：按小时统计订单金额和数量

        // 输出到ADS层Kafka主题（或写入Doris等存储）
        jsonStream.map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject value) throws Exception {
                        // 处理后的数据转换为字符串
                        return value.toJSONString();
                    }
                }).sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOOTSTRAP_SERVERS, ADS_TRADE_STATS_TOPIC))
                .uid("ads_trade_stats_sink")
                .name("ads_trade_stats_sink");

        jsonStream.print("dwd_order_stream -> ");

        env.execute("Job-DwdToAdsTradeStats");
    }
}

