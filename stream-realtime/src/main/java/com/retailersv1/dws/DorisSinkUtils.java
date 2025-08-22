// DorisSinkUtils.java - 增强版本
package com.retailersv1.dws;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author hp
 */
public class DorisSinkUtils {
    public static DorisSink<String> buildDorisSink(String tableName) {
        try {
            System.out.println("🔧 开始配置Doris Sink: " + tableName);

            String fenodes = com.stream.common.utils.ConfigUtils.getString("doris.fenodes");
            String database = com.stream.common.utils.ConfigUtils.getString("doris.database");
            String username = com.stream.common.utils.ConfigUtils.getString("doris.username");
            String password = com.stream.common.utils.ConfigUtils.getString("doris.password");

            System.out.println("📋 Doris配置信息:");
            System.out.println("   FE Nodes: " + fenodes);
            System.out.println("   Database: " + database);
            System.out.println("   Table: " + tableName);
            System.out.println("   Username: " + username);

            Properties props = new Properties();
            props.setProperty("format", "json");
            props.setProperty("read_json_by_line", "true");
            props.setProperty("timeout", "60000"); // 增加超时时间

            DorisSink<String> sink = DorisSink.<String>builder()
                    .setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisOptions(
                            DorisOptions.builder()
                                    .setFenodes(fenodes)
                                    .setTableIdentifier(database + "." + tableName)
                                    .setUsername(username)
                                    .setPassword(password)
                                    .build()
                    )
                    .setDorisExecutionOptions(DorisExecutionOptions.builder()
                            .setLabelPrefix("dws_label_" + System.currentTimeMillis())
                            .disable2PC()
                            .setDeletable(false)
                            .setBufferCount(3) // 减少缓冲区数量
                            .setBufferSize(512*1024) // 减少缓冲区大小
                            .setMaxRetries(1) // 减少重试次数
                            .setStreamLoadProp(props)
                            .build())
                    .setSerializer(new SimpleStringSerializer())
                    .build();

            System.out.println("✅ Doris Sink配置完成: " + tableName);
            return sink;
        } catch (Exception e) {
            System.err.println("❌ Doris Sink配置失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to create Doris sink for table: " + tableName, e);
        }
    }

    // 简化版测试main方法
    public static void main(String[] args) throws Exception {
        System.out.println("开始测试Doris写入功能...");

        // 创建一条测试数据
        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        Date now = new Date();
        Date startTime = new Date(now.getTime() - 5 * 60 * 1000); // 5分钟前

        com.alibaba.fastjson.JSONObject record = new com.alibaba.fastjson.JSONObject();
        record.put("stt", datetimeFormat.format(startTime));
        record.put("edt", datetimeFormat.format(now));
        record.put("cur_date", dateFormat.format(startTime));
        record.put("back_ct", 15);
        record.put("uu_ct", 100);

        String testData = record.toJSONString();
        System.out.println("测试数据: " + testData);

        // 创建Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建数据源
        DataStreamSource<String> source = env.fromElements(testData);

        // 打印测试数据
        source.print("📤 准备写入Doris的数据");

        // 写入Doris
        source.sinkTo(buildDorisSink("dws_user_user_login_window"))
                .name("test_doris_sink")
                .uid("test_doris_sink_uid");

        System.out.println("🏁 启动Flink作业写入Doris...");
        env.execute("Doris-Sink-Test");
    }
}
