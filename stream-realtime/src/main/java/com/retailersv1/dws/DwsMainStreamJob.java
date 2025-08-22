// DwsMainStreamJob.java - 根据数据样例和建表语句修改后的版本
package com.retailersv1.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import com.stream.common.utils.ConfigUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @BelongsProject: zxk_git
 * @BelongsPackage: com.retailersv1.dws
 * @Author: zhuxiangkuan
 * @CreateTime: 2025-08-22  20:23
 * @Description: TODO DWS层主流处理作业 从DWD层读取数据，进行聚合计算，写入Doris
 * @Version: 1.0
 */
public class DwsMainStreamJob {

    public static void main(String[] args) throws Exception {
        System.out.println("🚀 启动 DWS 主流处理作业...");

        // 打印配置信息
        printConfigInfo();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        env.setParallelism(1);

        System.out.println("🔧 Flink环境配置完成");

        // 处理流量域 - 搜索关键词统计
        processTrafficKeywordData(env);

        // 处理交易域 - SKU订单统计
        processTradeSkuOrderData(env);

        // 处理用户域 - 用户登录统计
        processUserLoginData(env);

        System.out.println("🏁 准备提交Flink作业...");
        env.execute("DWS-Main-Stream-Job");
    }

    // 打印配置信息
    private static void printConfigInfo() {
        try {
            System.out.println("=== 配置信息 ===");
            System.out.println("Kafka Bootstrap Servers: " + ConfigUtils.getString("kafka.bootstrap.servers"));
            System.out.println("Kafka DWD Page Topic: " + ConfigUtils.getString("kafka.dwd.page.topic"));
            System.out.println("Kafka DWD Order Detail Topic: " + ConfigUtils.getString("kafka.dwd.order.detail.topic"));
            System.out.println("Doris FE Nodes: " + ConfigUtils.getString("doris.fenodes"));
            System.out.println("Doris Database: " + ConfigUtils.getString("doris.database"));
            System.out.println("Doris Username: " + ConfigUtils.getString("doris.username"));
            System.out.println("===============");
        } catch (Exception e) {
            System.err.println("❌ 获取配置信息失败: " + e.getMessage());
        }
    }

    /**
     * 处理流量域 - 搜索关键词统计
     * 来源：dwd_traffic_page 的搜索行为数据
     * 字段：stt（窗口起始）、edt（窗口结束）、cur_date（日期）、keyword（关键词）、keyword_count（出现次数）
     */
    private static void processTrafficKeywordData(StreamExecutionEnvironment env) {
        try {
            System.out.println("=== 开始处理流量域搜索关键词数据 ===");

            String bootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");
            String pageTopic = ConfigUtils.getString("kafka.dwd.page.topic");
            System.out.println("🔍 Kafka配置 - Servers: " + bootstrapServers + ", Topic: " + pageTopic);

            // 测试Kafka连接性
            testKafkaTopic(bootstrapServers, pageTopic, "流量域页面数据");

            // 从DWD层读取页面数据
            DataStreamSource<String> pageDs = env.fromSource(
                    KafkaUtils.buildKafkaSource(
                            bootstrapServers,
                            pageTopic,
                            "dws_keyword_group_" + System.currentTimeMillis(),
                            OffsetsInitializer.earliest()
                    ),
                    WatermarkStrategy.noWatermarks(),
                    "read_dwd_page_for_keyword"
            );

            System.out.println("📥 已创建Kafka数据源");

            // 解析并过滤包含搜索关键词的数据
            DataStream<JSONObject> keywordDs = pageDs
                    .map(value -> {
                        try {
                            return JSON.parseObject(value);
                        } catch (Exception e) {
                            System.err.println("❌ JSON解析失败: " + value);
                            return null;
                        }
                    })
                    .filter(json -> {
                        if (json != null) {
                            // 检查是否有搜索关键词
                            String searchKeyword = json.getString("search_keyword");
                            if (searchKeyword != null && !searchKeyword.isEmpty()) {
                                System.out.println("🔍 发现搜索关键词数据: keyword=" + searchKeyword + ", data=" + json.toJSONString());
                                return true;
                            }
                        }
                        return false;
                    })
                    .name("filter_search_keyword");

            System.out.println("📊 已创建关键词过滤流");

            // 按关键词分组并窗口聚合 - 使用处理时间窗口 + 计数触发器
            DataStream<String> keywordWindowDs = keywordDs
                    .keyBy(json -> json.getString("search_keyword"))
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 缩短到10秒
                    .trigger(CountTrigger.of(1)) // 添加计数触发器，每条数据都触发
                    .aggregate(
                            new KeywordAggregateFunction(),
                            new KeywordProcessWindowFunction()
                    );

            System.out.println("🧮 已创建关键词聚合流");

            // 直接添加Sink
            System.out.println("开始配置关键词Doris Sink...");
            keywordWindowDs.sinkTo(DorisSinkUtils.buildDorisSink("dws_traffic_source_keyword_page_view_window"))
                    .name("sink_keyword_to_doris")
                    .uid("dws_keyword_doris_sink");
            System.out.println("✅ 关键词Doris Sink配置完成");

            System.out.println("✅ 流量域搜索关键词处理完成");

        } catch (Exception e) {
            System.err.println("❌ 流量域搜索关键词处理异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 处理交易域 - SKU订单统计
     * 来源：dwd_trade_order_detail
     * 字段：stt、edt、cur_date、sku_id、sku_name、original_amount（原始金额）、activity_reduce_amount（活动减免）、coupon_reduce_amount（优惠券减免）、order_amount（下单金额）
     */
    private static void processTradeSkuOrderData(StreamExecutionEnvironment env) {
        try {
            System.out.println("=== 开始处理交易域SKU订单数据 ===");

            String bootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");
            String orderDetailTopic = ConfigUtils.getString("kafka.dwd.order.detail.topic");
            System.out.println("📦 Kafka配置 - Servers: " + bootstrapServers + ", Topic: " + orderDetailTopic);

            // 测试Kafka连接性
            testKafkaTopic(bootstrapServers, orderDetailTopic, "交易域订单明细");

            // 从DWD层读取订单明细数据
            DataStreamSource<String> orderDetailDs = env.fromSource(
                    KafkaUtils.buildKafkaSource(
                            bootstrapServers,
                            orderDetailTopic,
                            "dws_sku_order_group_" + System.currentTimeMillis(),
                            OffsetsInitializer.earliest()
                    ),
                    WatermarkStrategy.noWatermarks(),
                    "read_dwd_order_detail"
            );

            System.out.println("📥 已创建订单明细数据源");

            // 解析订单明细数据
            DataStream<JSONObject> orderDetailJsonDs = orderDetailDs
                    .map(value -> {
                        try {
                            return JSON.parseObject(value);
                        } catch (Exception e) {
                            System.err.println("❌ 订单JSON解析失败: " + value);
                            return null;
                        }
                    })
                    .filter(json -> json != null && json.getString("sku_id") != null)
                    .name("parse_order_detail");

            System.out.println("📊 已创建订单明细解析流");

            // 按SKU ID分组并窗口聚合 - 使用处理时间窗口 + 计数触发器
            DataStream<String> skuOrderWindowDs = orderDetailJsonDs
                    .keyBy(json -> json.getString("sku_id"))
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 缩短到10秒
                    .trigger(CountTrigger.of(1)) // 添加计数触发器，每条数据都触发
                    .aggregate(
                            new SkuOrderAggregateFunction(),
                            new SkuOrderProcessWindowFunction()
                    );

            System.out.println("🧮 已创建SKU订单聚合流");

            // 直接添加Sink
            System.out.println("开始配置SKU订单Doris Sink...");
            skuOrderWindowDs.sinkTo(DorisSinkUtils.buildDorisSink("dws_trade_sku_order_window"))
                    .name("sink_sku_order_to_doris")
                    .uid("dws_sku_order_doris_sink");
            System.out.println("✅ SKU订单Doris Sink配置完成");

            System.out.println("✅ 交易域SKU订单处理完成");

        } catch (Exception e) {
            System.err.println("❌ 交易域SKU订单处理异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 处理用户域 - 用户登录统计
     * 来源：dwd_traffic_page 的登录行为数据
     * 字段：stt、edt、cur_date、back_ct（回流用户数）、uu_ct（独立用户数）
     */
    private static void processUserLoginData(StreamExecutionEnvironment env) {
        try {
            System.out.println("=== 开始处理用户域登录数据 ===");

            String bootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");
            String pageTopic = ConfigUtils.getString("kafka.dwd.page.topic");
            System.out.println("👤 Kafka配置 - Servers: " + bootstrapServers + ", Topic: " + pageTopic);

            // 测试Kafka连接性
            testKafkaTopic(bootstrapServers, pageTopic, "用户域页面数据");

            // 从DWD层读取页面数据
            DataStreamSource<String> pageDs = env.fromSource(
                    KafkaUtils.buildKafkaSource(
                            bootstrapServers,
                            pageTopic,
                            "dws_user_login_group_" + System.currentTimeMillis(),
                            OffsetsInitializer.earliest()
                    ),
                    WatermarkStrategy.noWatermarks(),
                    "read_dwd_page_for_login"
            );

            System.out.println("📥 已创建登录页面数据源");

            // 解析并过滤登录行为数据
            DataStream<JSONObject> loginDs = pageDs
                    .map(value -> {
                        try {
                            return JSON.parseObject(value);
                        } catch (Exception e) {
                            System.err.println("❌ 登录页面JSON解析失败: " + value);
                            return null;
                        }
                    })
                    .filter(json -> {
                        if (json != null) {
                            // 判断是否为登录行为
                            boolean isLogin = isLoginBehavior(json);
                            if (isLogin) {
                                JSONObject common = json.getJSONObject("common");
                                String userId = common != null ? common.getString("user_id") : "unknown";
                                System.out.println("👤 发现登录行为数据: user_id=" + userId + ", data=" + json.toJSONString());
                            }
                            return isLogin;
                        }
                        return false;
                    })
                    .name("filter_login_behavior");

            System.out.println("📊 已创建登录行为过滤流");

            // 全局窗口聚合用户登录统计 - 使用处理时间窗口 + 计数触发器
            DataStream<String> userLoginWindowDs = loginDs
                    .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 缩短到10秒
                    .trigger(CountTrigger.of(1)) // 添加计数触发器，每条数据都触发
                    .aggregate(
                            new UserLoginAggregateFunction(),
                            new UserLoginProcessWindowFunction()
                    );

            System.out.println("🧮 已创建用户登录聚合流");

            // 直接添加Sink
            System.out.println("开始配置用户登录Doris Sink...");
            userLoginWindowDs.sinkTo(DorisSinkUtils.buildDorisSink("dws_user_user_login_window"))
                    .name("sink_user_login_to_doris")
                    .uid("dws_user_login_doris_sink");
            System.out.println("✅ 用户登录Doris Sink配置完成");

            System.out.println("✅ 用户域登录处理完成");

        } catch (Exception e) {
            System.err.println("❌ 用户域登录处理异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 测试Kafka主题连接性
    private static void testKafkaTopic(String bootstrapServers, String topic, String description) {
        try {
            boolean exists = KafkaUtils.kafkaTopicExists(bootstrapServers, topic);
            System.out.println("📡 " + description + " - Topic [" + topic + "] 存在性: " + exists);
        } catch (Exception e) {
            System.err.println("❌ 测试Kafka主题连接性失败: " + e.getMessage());
        }
    }

    // 判断是否为登录行为
    private static boolean isLoginBehavior(JSONObject json) {
        try {
            JSONObject page = json.getJSONObject("page");
            return page != null && "login".equals(page.getString("page_id"));
        } catch (Exception e) {
            return false;
        }
    }

    // 关键词聚合函数
    private static class KeywordAggregateFunction implements org.apache.flink.api.common.functions.AggregateFunction<JSONObject, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(JSONObject value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 关键词窗口处理函数
    private static class KeywordProcessWindowFunction extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) {
            long count = elements.iterator().next();
            TimeWindow window = context.window();

            JSONObject result = new JSONObject();
            result.put("stt", datetimeFormat.format(new Date(window.getStart())));
            result.put("edt", datetimeFormat.format(new Date(window.getEnd())));
            result.put("cur_date", dateFormat.format(new Date(window.getStart())));
            result.put("keyword", key);
            result.put("keyword_count", count);

            String resultStr = result.toJSONString();
            System.out.println("📤 关键词窗口结果: " + resultStr);
            out.collect(resultStr);
        }
    }

    // SKU订单聚合函数
    private static class SkuOrderAggregateFunction implements org.apache.flink.api.common.functions.AggregateFunction<JSONObject, JSONObject, JSONObject> {

        private BigDecimal safeBigDecimal(String value) {
            if (value == null || value.isEmpty()) {
                return BigDecimal.ZERO;
            }
            try {
                return new BigDecimal(value);
            } catch (NumberFormatException e) {
                System.err.println("⚠️ BigDecimal转换失败，使用默认值0: " + value);
                return BigDecimal.ZERO;
            }
        }

        @Override
        public JSONObject createAccumulator() {
            JSONObject acc = new JSONObject();
            acc.put("original_amount", BigDecimal.ZERO);
            acc.put("activity_reduce_amount", BigDecimal.ZERO);
            acc.put("coupon_reduce_amount", BigDecimal.ZERO);
            acc.put("order_amount", BigDecimal.ZERO);
            acc.put("sku_name", "");
            return acc;
        }

        @Override
        public JSONObject add(JSONObject value, JSONObject accumulator) {
            // 累积SKU名称（取第一个）
            if (accumulator.getString("sku_name") == null || accumulator.getString("sku_name").isEmpty()) {
                accumulator.put("sku_name", value.getString("sku_name"));
            }

            // 累积金额字段
            accumulator.put("original_amount",
                    safeBigDecimal(accumulator.getString("original_amount")).add(
                            safeBigDecimal(value.getString("split_original_amount"))));
            accumulator.put("activity_reduce_amount",
                    safeBigDecimal(accumulator.getString("activity_reduce_amount")).add(
                            safeBigDecimal(value.getString("split_activity_amount"))));
            accumulator.put("coupon_reduce_amount",
                    safeBigDecimal(accumulator.getString("coupon_reduce_amount")).add(
                            safeBigDecimal(value.getString("split_coupon_amount"))));
            accumulator.put("order_amount",
                    safeBigDecimal(accumulator.getString("order_amount")).add(
                            safeBigDecimal(value.getString("split_total_amount"))));

            return accumulator;
        }

        @Override
        public JSONObject getResult(JSONObject accumulator) {
            return accumulator;
        }

        @Override
        public JSONObject merge(JSONObject a, JSONObject b) {
            JSONObject merged = new JSONObject();
            merged.put("sku_name", a.getString("sku_name")); // 取第一个SKU名称

            merged.put("original_amount",
                    safeBigDecimal(a.getString("original_amount")).add(
                            safeBigDecimal(b.getString("original_amount"))));
            merged.put("activity_reduce_amount",
                    safeBigDecimal(a.getString("activity_reduce_amount")).add(
                            safeBigDecimal(b.getString("activity_reduce_amount"))));
            merged.put("coupon_reduce_amount",
                    safeBigDecimal(a.getString("coupon_reduce_amount")).add(
                            safeBigDecimal(b.getString("coupon_reduce_amount"))));
            merged.put("order_amount",
                    safeBigDecimal(a.getString("order_amount")).add(
                            safeBigDecimal(b.getString("order_amount"))));

            return merged;
        }
    }

    // SKU订单窗口处理函数
    private static class SkuOrderProcessWindowFunction extends ProcessWindowFunction<JSONObject, String, String, TimeWindow> {
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void process(String key, Context context, Iterable<JSONObject> elements, Collector<String> out) {
            JSONObject aggData = elements.iterator().next();
            TimeWindow window = context.window();

            JSONObject result = new JSONObject();
            result.put("stt", datetimeFormat.format(new Date(window.getStart())));
            result.put("edt", datetimeFormat.format(new Date(window.getEnd())));
            result.put("cur_date", dateFormat.format(new Date(window.getStart())));
            result.put("sku_id", Integer.parseInt(key)); // 转换为INT类型
            result.put("sku_name", aggData.getString("sku_name"));
            result.put("original_amount", aggData.getString("original_amount"));
            result.put("activity_reduce_amount", aggData.getString("activity_reduce_amount"));
            result.put("coupon_reduce_amount", aggData.getString("coupon_reduce_amount"));
            result.put("order_amount", aggData.getString("order_amount"));

            String resultStr = result.toJSONString();
            System.out.println("📤 SKU订单窗口结果: " + resultStr);
            out.collect(resultStr);
        }
    }

    // 用户登录聚合函数
    private static class UserLoginAggregateFunction implements org.apache.flink.api.common.functions.AggregateFunction<JSONObject, Tuple2<Set<String>, Set<String>>, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Set<String>, Set<String>> createAccumulator() {
            return new Tuple2<>(new HashSet<>(), new HashSet<>());
        }

        @Override
        public Tuple2<Set<String>, Set<String>> add(JSONObject value, Tuple2<Set<String>, Set<String>> accumulator) {
            JSONObject common = value.getJSONObject("common");
            if (common != null) {
                String userId = common.getString("user_id");
                String isNew = common.getString("is_new");

                // 统计所有用户
                accumulator.f0.add(userId);

                // 统计回流用户（非新用户）
                if ("0".equals(isNew)) {
                    accumulator.f1.add(userId);
                }
            }
            return accumulator;
        }

        @Override
        public Tuple2<Integer, Integer> getResult(Tuple2<Set<String>, Set<String>> accumulator) {
            // 返回回流用户数和总用户数
            return new Tuple2<>(accumulator.f1.size(), accumulator.f0.size());
        }

        @Override
        public Tuple2<Set<String>, Set<String>> merge(Tuple2<Set<String>, Set<String>> a, Tuple2<Set<String>, Set<String>> b) {
            Set<String> allUsers = new HashSet<>(a.f0);
            allUsers.addAll(b.f0);

            Set<String> backUsers = new HashSet<>(a.f1);
            backUsers.addAll(b.f1);

            return new Tuple2<>(allUsers, backUsers);
        }
    }

    // 用户登录窗口处理函数
    private static class UserLoginProcessWindowFunction extends ProcessAllWindowFunction<Tuple2<Integer, Integer>, String, TimeWindow> {
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void process(Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) {
            Tuple2<Integer, Integer> stats = elements.iterator().next();
            TimeWindow window = context.window();

            JSONObject result = new JSONObject();
            result.put("stt", datetimeFormat.format(new Date(window.getStart())));
            result.put("edt", datetimeFormat.format(new Date(window.getEnd())));
            result.put("cur_date", dateFormat.format(new Date(window.getStart())));
            result.put("back_ct", stats.f0); // 回流用户数
            result.put("uu_ct", stats.f1);   // 独立用户数

            String resultStr = result.toJSONString();
            System.out.println("📤 用户登录窗口结果: " + resultStr);
            out.collect(resultStr);
        }
    }
}
