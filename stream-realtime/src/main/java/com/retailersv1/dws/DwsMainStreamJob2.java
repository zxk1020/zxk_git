// DwsMainStreamJob.java - 修改为写入MySQL版本，数据库名为FlinkDws2Mysql
package com.retailersv1.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import com.stream.common.utils.ConfigUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.Date;

/**
 * @author hp
 */
public class DwsMainStreamJob2 {
    // MySQL配置信息 - 修改数据库名为FlinkDws2Mysql
    private static final String MYSQL_URL = "jdbc:mysql://192.168.142.130:3306/FlinkDws2Mysql?useSSL=false&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";

    public static void main(String[] args) throws Exception {
        System.out.println("🚀 启动 DWS 主流处理作业...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        env.setParallelism(1);

        System.out.println("🔧 Flink环境配置完成");

        // 处理流量域 - 搜索关键词统计
//        processTrafficKeywordData(env);

        // 处理交易域 - SKU订单统计
//        processTradeSkuOrderData(env);

        // 处理用户域 - 用户登录统计
        processUserLoginData(env);

        System.out.println("🏁 准备提交Flink作业...");
        env.execute("DWS-Main-Stream-Job");
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

            System.out.println(" Kafka Bootstrap Servers: " + bootstrapServers);
            System.out.println(" Kafka Topic: " + pageTopic);

            // 从DWD层读取页面数据 - 添加事件时间水位线
            DataStreamSource<String> pageDs = env.fromSource(
                    KafkaUtils.buildKafkaSource(
                            bootstrapServers,
                            pageTopic,
                            "dws_keyword_group_" + System.currentTimeMillis(),
                            OffsetsInitializer.earliest()
                    ),
                    WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                            .withTimestampAssigner((event, timestamp) -> {
                                try {
                                    JSONObject json = JSON.parseObject(event);
                                    System.out.println("📥 接收到原始数据: " + event);
                                    Long ts = json.getLong("ts");
                                    // 处理毫秒级时间戳
                                    long eventTime = ts != null ? ts : System.currentTimeMillis();
                                    System.out.println("⏱️ 事件时间: " + new Date(eventTime) + ", 时间戳: " + eventTime);
                                    return eventTime;
                                } catch (Exception e) {
                                    System.err.println("❌ 解析时间戳失败: " + event);
                                    return System.currentTimeMillis();
                                }
                            }),
                    "read_dwd_page_for_keyword"
            );

            // 打印原始数据
            pageDs.print("📄 原始页面数据");

            // 解析并过滤包含搜索关键词的数据
            DataStream<JSONObject> keywordDs = pageDs
                    .map(value -> {
                        try {
                            JSONObject json = JSON.parseObject(value);
                            System.out.println("🔄 解析JSON: " + value);
                            return json;
                        } catch (Exception e) {
                            System.err.println("❌ JSON解析失败: " + value);
                            return null;
                        }
                    })
                    .filter(json -> {
                        if (json != null) {
                            // 根据实际数据样例调整过滤条件
                            // 数据样例：{"page_id":"search","user_id":"3","is_new":"0","during_time":14248,"channel":"oppo","mid":"mid_291","ts":1755535163561}
                            String pageId = json.getString("page_id");
                            String userId = json.getString("user_id");
                            System.out.println("🔍 过滤检查 - page_id: " + pageId + ", user_id: " + userId);
                            boolean result = "search".equals(pageId) && userId != null && !userId.isEmpty();
                            if (result) {
                                System.out.println("✅ 通过过滤的数据: " + json.toJSONString());
                            }
                            return result;
                        }
                        System.out.println("❌ JSON为空，过滤掉");
                        return false;
                    });

            // 打印过滤后的数据
            keywordDs.print("🔍 过滤后的关键词数据");

            // 按用户ID分组并窗口聚合（因为原始数据中没有item字段，使用userId作为示例）
            DataStream<String> keywordWindowDs = keywordDs
                    .keyBy(json -> {
                        String key = json.getString("user_id"); // 使用user_id作为分组键
                        System.out.println("🔑 分组键: " + key);
                        return key != null ? key : "unknown";
                    })
                    .window(TumblingEventTimeWindows.of(Time.minutes(1))) // 1分钟事件时间窗口
                    .allowedLateness(Time.seconds(10)) // 允许10秒延迟数据
                    .aggregate(
                            new KeywordAggregateFunction(),
                            new KeywordProcessWindowFunction()
                    );

            // 添加打印语句显示中间结果
            keywordWindowDs.map(result -> {
                System.out.println("🔑 搜索关键词结果: " + result);
                return result;
            });

            // 直接写入MySQL
            keywordWindowDs.addSink(new MysqlKeywordSink())
                    .name("mysql_keyword_sink")
                    .uid("mysql_keyword_sink_uid");

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

            System.out.println(" Kafka Bootstrap Servers: " + bootstrapServers);
            System.out.println(" Kafka Order Detail Topic: " + orderDetailTopic);

            // 从DWD层读取订单明细数据 - 添加事件时间水位线
            DataStreamSource<String> orderDetailDs = env.fromSource(
                    KafkaUtils.buildKafkaSource(
                            bootstrapServers,
                            orderDetailTopic,
                            "dws_sku_order_group_" + System.currentTimeMillis(),
                            OffsetsInitializer.earliest()
                    ),
                    WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                            .withTimestampAssigner((event, timestamp) -> {
                                try {
                                    JSONObject json = JSON.parseObject(event);
                                    System.out.println("📥 接收到订单明细数据: " + event);
                                    Long ts = json.getLong("ts");
                                    // 处理毫秒级时间戳
                                    long eventTime = ts != null ? ts : System.currentTimeMillis();
                                    System.out.println("⏱️ 订单事件时间: " + new Date(eventTime) + ", 时间戳: " + eventTime);
                                    return eventTime;
                                } catch (Exception e) {
                                    System.err.println("❌ 解析订单时间戳失败: " + event);
                                    return System.currentTimeMillis();
                                }
                            }),
                    "read_dwd_order_detail"
            );

            // 打印原始订单数据
            orderDetailDs.print("📄 原始订单数据");

            // 解析订单明细数据
            DataStream<JSONObject> orderDetailJsonDs = orderDetailDs
                    .map(value -> {
                        try {
                            JSONObject json = JSON.parseObject(value);
                            System.out.println("🔄 解析订单JSON: " + value);
                            return json;
                        } catch (Exception e) {
                            System.err.println("❌ 订单JSON解析失败: " + value);
                            return null;
                        }
                    })
                    .filter(json -> {
                        if (json != null) {
                            String skuId = json.getString("sku_id");
                            String skuName = json.getString("sku_name");
                            System.out.println("🔍 订单过滤检查 - sku_id: " + skuId + ", sku_name: " + skuName);
                            boolean result = skuId != null && !skuId.isEmpty() && skuName != null && !skuName.isEmpty();
                            if (result) {
                                System.out.println("✅ 通过订单过滤的数据: " + json.toJSONString());
                            }
                            return result;
                        }
                        System.out.println("❌ 订单JSON为空，过滤掉");
                        return false;
                    });

            // 打印过滤后的订单数据
            orderDetailJsonDs.print("🔍 过滤后的订单数据");

            // 按SKU ID分组并窗口聚合
            DataStream<String> skuOrderWindowDs = orderDetailJsonDs
                    .keyBy(json -> {
                        String skuId = json.getString("sku_id");
                        System.out.println("📦 SKU订单分组键: " + skuId);
                        return skuId != null ? skuId : "unknown";
                    })
                    .window(TumblingEventTimeWindows.of(Time.minutes(1))) // 1分钟事件时间窗口
                    .allowedLateness(Time.seconds(10)) // 允许10秒延迟数据
                    .aggregate(
                            new SkuOrderAggregateFunction(),
                            new SkuOrderProcessWindowFunction()
                    );

            // 添加打印语句显示中间结果
            skuOrderWindowDs.map(result -> {
                System.out.println("📦 SKU订单结果: " + result);
                return result;
            });

            // 直接写入MySQL
            skuOrderWindowDs.addSink(new MysqlSkuOrderSink())
                    .name("mysql_sku_order_sink")
                    .uid("mysql_sku_order_sink_uid");

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

            // 从DWD层读取页面数据 - 添加事件时间水位线
            DataStreamSource<String> pageDs = env.fromSource(
                    KafkaUtils.buildKafkaSource(
                            bootstrapServers,
                            pageTopic,
                            "dws_user_login_group_" + System.currentTimeMillis(),
                            OffsetsInitializer.earliest()
                    ),
                    WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                            .withTimestampAssigner((event, timestamp) -> {
                                try {
                                    JSONObject json = JSON.parseObject(event);
                                    Long ts = json.getLong("ts");
                                    long eventTime = ts != null ? ts : System.currentTimeMillis();
                                    return eventTime;
                                } catch (Exception e) {
                                    return System.currentTimeMillis();
                                }
                            }),
                    "read_dwd_page_for_login"
            );

            // 打印原始数据以便调试
            pageDs.print("📄 原始用户页面数据");

            // 解析并识别用户访问行为数据（任何有user_id的页面访问都算作用户活动）
            DataStream<JSONObject> userActivityDs = pageDs
                    .map(value -> {
                        try {
                            return JSON.parseObject(value);
                        } catch (Exception e) {
                            return null;
                        }
                    })
                    .filter(json -> {
                        if (json != null) {
                            String userId = json.getString("user_id");
                            String pageId = json.getString("page_id");
                            System.out.println("🔍 用户活动检查 - user_id: " + userId + ", page_id: " + pageId);

                            // 任何有有效user_id的页面访问都算作用户活动
                            boolean result = userId != null && !userId.isEmpty();
                            if (result) {
                                System.out.println("✅ 通过用户活动过滤的数据: " + json.toJSONString());
                            }
                            return result;
                        }
                        return false;
                    });

            // 打印过滤后的用户活动数据
            userActivityDs.print("🔍 过滤后的用户活动数据");

            // 全局窗口聚合用户登录统计
            DataStream<String> userLoginWindowDs = userActivityDs
                    .windowAll(TumblingEventTimeWindows.of(Time.minutes(1))) // 1分钟事件时间窗口
                    .allowedLateness(Time.seconds(10)) // 允许10秒延迟数据
                    .aggregate(
                            new UserLoginAggregateFunction(),
                            new UserLoginProcessWindowFunction()
                    );

            // 添加打印语句显示中间结果
            userLoginWindowDs.map(result -> {
                System.out.println("👤 用户活动结果: " + result);
                return result;
            });

            // 直接写入MySQL
            userLoginWindowDs.addSink(new MysqlUserLoginSink())
                    .name("mysql_user_login_sink")
                    .uid("mysql_user_login_sink_uid");

            System.out.println("✅ 用户域处理完成");

        } catch (Exception e) {
            System.err.println("❌ 用户域处理异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 关键词聚合函数
    private static class KeywordAggregateFunction implements org.apache.flink.api.common.functions.AggregateFunction<JSONObject, Long, Long> {
        @Override
        public Long createAccumulator() {
            System.out.println("🆕 创建关键词累加器");
            return 0L;
        }

        @Override
        public Long add(JSONObject value, Long accumulator) {
            System.out.println("➕ 累加关键词计数: " + value.toJSONString());
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            System.out.println("📈 获取关键词结果: " + accumulator);
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            System.out.println("🔄 合并关键词计数: " + a + " + " + b);
            return a + b;
        }
    }

    // 关键词窗口处理函数 - 严格按照MySQL表字段顺序生成JSON
    private static class KeywordProcessWindowFunction extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long count = elements.iterator().next();
            TimeWindow window = context.window();

            System.out.println("🧮 关键词窗口处理 - 用户ID: " + key + ", 计数: " + count);
            System.out.println("🕐 窗口开始时间: " + new Date(window.getStart()));
            System.out.println("🕐 窗口结束时间: " + new Date(window.getEnd()));

            // 严格按照MySQL表字段顺序构建JSON: stt, edt, cur_date, keyword, keyword_count
            JSONObject result = new JSONObject();
            result.put("stt", datetimeFormat.format(new Date(window.getStart())));
            result.put("edt", datetimeFormat.format(new Date(window.getEnd())));
            result.put("cur_date", dateFormat.format(new Date(window.getStart())));
            result.put("keyword", key); // 使用用户ID作为关键词示例
            result.put("keyword_count", count);

            String output = result.toJSONString();
            System.out.println("📤 关键词窗口结果输出: " + output);
            out.collect(output);
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
            System.out.println("🆕 创建SKU订单累加器");
            return acc;
        }

        @Override
        public JSONObject add(JSONObject value, JSONObject accumulator) {
            System.out.println("➕ 累加SKU订单数据: " + value.toJSONString());
            String skuName = value.getString("sku_name");

            // 累积SKU名称（取第一个）
            if (accumulator.getString("sku_name") == null || accumulator.getString("sku_name").isEmpty()) {
                accumulator.put("sku_name", skuName);
            }

            // 累积金额字段 - 根据实际数据样例调整字段名
            BigDecimal originalAmount = safeBigDecimal(accumulator.getString("original_amount"))
                    .add(safeBigDecimal(value.getString("split_original_amount")));
            BigDecimal activityAmount = safeBigDecimal(accumulator.getString("activity_reduce_amount"))
                    .add(safeBigDecimal(value.getString("split_activity_amount")));
            BigDecimal couponAmount = safeBigDecimal(accumulator.getString("coupon_reduce_amount"))
                    .add(safeBigDecimal(value.getString("split_coupon_amount")));
            BigDecimal orderAmount = safeBigDecimal(accumulator.getString("order_amount"))
                    .add(safeBigDecimal(value.getString("split_original_amount"))); // 使用原始金额作为订单金额

            accumulator.put("original_amount", originalAmount);
            accumulator.put("activity_reduce_amount", activityAmount);
            accumulator.put("coupon_reduce_amount", couponAmount);
            accumulator.put("order_amount", orderAmount);

            return accumulator;
        }

        @Override
        public JSONObject getResult(JSONObject accumulator) {
            System.out.println("📈 获取SKU订单结果: " + accumulator.toJSONString());
            return accumulator;
        }

        @Override
        public JSONObject merge(JSONObject a, JSONObject b) {
            System.out.println("🔄 合并SKU订单数据");
            JSONObject merged = new JSONObject();
            merged.put("sku_name", a.getString("sku_name") != null && !a.getString("sku_name").isEmpty() ?
                    a.getString("sku_name") : b.getString("sku_name"));

            BigDecimal originalAmount = safeBigDecimal(a.getString("original_amount"))
                    .add(safeBigDecimal(b.getString("original_amount")));
            BigDecimal activityAmount = safeBigDecimal(a.getString("activity_reduce_amount"))
                    .add(safeBigDecimal(b.getString("activity_reduce_amount")));
            BigDecimal couponAmount = safeBigDecimal(a.getString("coupon_reduce_amount"))
                    .add(safeBigDecimal(b.getString("coupon_reduce_amount")));
            BigDecimal orderAmount = safeBigDecimal(a.getString("order_amount"))
                    .add(safeBigDecimal(b.getString("order_amount")));

            merged.put("original_amount", originalAmount);
            merged.put("activity_reduce_amount", activityAmount);
            merged.put("coupon_reduce_amount", couponAmount);
            merged.put("order_amount", orderAmount);

            return merged;
        }
    }

    // SKU订单窗口处理函数 - 严格按照MySQL表字段顺序生成JSON
    private static class SkuOrderProcessWindowFunction extends ProcessWindowFunction<JSONObject, String, String, TimeWindow> {
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void process(String key, Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {
            JSONObject aggData = elements.iterator().next();
            TimeWindow window = context.window();

            System.out.println("🧮 SKU订单窗口处理 - SKU ID: " + key);
            System.out.println("🕐 窗口开始时间: " + new Date(window.getStart()));
            System.out.println("🕐 窗口结束时间: " + new Date(window.getEnd()));

            // 严格按照MySQL表字段顺序构建JSON: stt, edt, cur_date, sku_id, sku_name, original_amount, activity_reduce_amount, coupon_reduce_amount, order_amount
            JSONObject result = new JSONObject();
            result.put("stt", datetimeFormat.format(new Date(window.getStart())));
            result.put("edt", datetimeFormat.format(new Date(window.getEnd())));
            result.put("cur_date", dateFormat.format(new Date(window.getStart())));

            // SKU ID字段确保为整数类型
            try {
                result.put("sku_id", Integer.parseInt(key));
            } catch (NumberFormatException e) {
                System.err.println("⚠️ SKU ID转换失败，使用原值: " + key);
                result.put("sku_id", key);
            }

            // SKU名称字段
            String skuName = aggData.getString("sku_name");
            if (skuName != null && skuName.length() > 255) {
                // 截断超过255字符的SKU名称
                skuName = skuName.substring(0, 255);
                System.out.println("✂️ SKU名称过长，已截断: " + skuName);
            }
            result.put("sku_name", skuName);

            // 金额字段处理 - 确保使用正确的精度和舍入模式
            try {
                BigDecimal originalAmount = new BigDecimal(aggData.getString("original_amount")).setScale(2, BigDecimal.ROUND_HALF_UP);
                BigDecimal activityReduceAmount = new BigDecimal(aggData.getString("activity_reduce_amount")).setScale(2, BigDecimal.ROUND_HALF_UP);
                BigDecimal couponReduceAmount = new BigDecimal(aggData.getString("coupon_reduce_amount")).setScale(2, BigDecimal.ROUND_HALF_UP);
                BigDecimal orderAmount = new BigDecimal(aggData.getString("order_amount")).setScale(2, BigDecimal.ROUND_HALF_UP);

                // 确保金额不为负数
                result.put("original_amount", originalAmount.max(BigDecimal.ZERO));
                result.put("activity_reduce_amount", activityReduceAmount.max(BigDecimal.ZERO));
                result.put("coupon_reduce_amount", couponReduceAmount.max(BigDecimal.ZERO));
                result.put("order_amount", orderAmount.max(BigDecimal.ZERO));
            } catch (Exception e) {
                System.err.println("⚠️ 金额字段处理失败: " + e.getMessage());
                result.put("original_amount", BigDecimal.ZERO);
                result.put("activity_reduce_amount", BigDecimal.ZERO);
                result.put("coupon_reduce_amount", BigDecimal.ZERO);
                result.put("order_amount", BigDecimal.ZERO);
            }

            String output = result.toJSONString();
            System.out.println("📤 SKU订单窗口结果: " + output);
            out.collect(output);
        }
    }

    // 用户登录聚合函数
    private static class UserLoginAggregateFunction implements org.apache.flink.api.common.functions.AggregateFunction<JSONObject, Tuple2<Set<String>, Set<String>>, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Set<String>, Set<String>> createAccumulator() {
            System.out.println("🆕 创建用户活动累加器");
            return new Tuple2<>(new HashSet<>(), new HashSet<>());
        }

        @Override
        public Tuple2<Set<String>, Set<String>> add(JSONObject value, Tuple2<Set<String>, Set<String>> accumulator) {
            System.out.println("➕ 累加用户活动数据: " + value.toJSONString());

            // 获取用户信息
            String userId = value.getString("user_id");
            String isNew = value.getString("is_new");

            if (userId != null) {
                // 累积所有用户
                accumulator.f0.add(userId);
                // 累积回流用户（老用户）
                if ("0".equals(isNew)) {
                    accumulator.f1.add(userId);
                }
            }
            return accumulator;
        }

        @Override
        public Tuple2<Integer, Integer> getResult(Tuple2<Set<String>, Set<String>> accumulator) {
            System.out.println("📈 获取用户活动结果 - 回流用户数: " + accumulator.f1.size() + ", 独立用户数: " + accumulator.f0.size());
            // f1 = 回流用户数, f0 = 独立用户数
            return new Tuple2<>(accumulator.f1.size(), accumulator.f0.size());
        }

        @Override
        public Tuple2<Set<String>, Set<String>> merge(Tuple2<Set<String>, Set<String>> a, Tuple2<Set<String>, Set<String>> b) {
            System.out.println("🔄 合并用户活动数据");
            Set<String> allUsers = new HashSet<>(a.f0);
            allUsers.addAll(b.f0);

            Set<String> backUsers = new HashSet<>(a.f1);
            backUsers.addAll(b.f1);

            return new Tuple2<>(allUsers, backUsers);
        }
    }

    // 用户登录窗口处理函数 - 严格按照MySQL表字段顺序生成JSON
    private static class UserLoginProcessWindowFunction extends ProcessAllWindowFunction<Tuple2<Integer, Integer>, String, TimeWindow> {
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void process(Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) throws Exception {
            Tuple2<Integer, Integer> stats = elements.iterator().next();
            TimeWindow window = context.window();

            System.out.println("🧮 用户登录窗口处理");
            System.out.println("🕐 窗口开始时间: " + new Date(window.getStart()));
            System.out.println("🕐 窗口结束时间: " + new Date(window.getEnd()));

            // 严格按照MySQL表字段顺序构建JSON: stt, edt, cur_date, back_ct, uu_ct
            JSONObject result = new JSONObject();
            result.put("stt", datetimeFormat.format(new Date(window.getStart())));
            result.put("edt", datetimeFormat.format(new Date(window.getEnd())));
            result.put("cur_date", dateFormat.format(new Date(window.getStart())));
            result.put("back_ct", Math.max(0, stats.f0)); // 回流用户数，确保非负
            result.put("uu_ct", Math.max(0, stats.f1));   // 独立用户数，确保非负

            String output = result.toJSONString();
            System.out.println("📤 用户登录窗口结果: " + output);
            out.collect(output);
        }
    }

    // MySQL Sink for Keyword Data
    private static class MysqlKeywordSink extends RichSinkFunction<String> {
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                System.out.println("✅ MySQL连接成功: " + MYSQL_URL);
                String sql = "INSERT INTO dws_traffic_source_keyword_page_view_window (stt, edt, cur_date, keyword, keyword_count) VALUES (?, ?, ?, ?, ?)";
                preparedStatement = connection.prepareStatement(sql);
                System.out.println("✅ MySQL PreparedStatement创建成功");
            } catch (Exception e) {
                System.err.println("❌ MySQL连接失败: " + e.getMessage());
                throw e;
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            try {
                System.out.println("💾 尝试写入MySQL关键词数据: " + value);
                JSONObject jsonObject = JSON.parseObject(value);
                preparedStatement.setString(1, jsonObject.getString("stt"));
                preparedStatement.setString(2, jsonObject.getString("edt"));
                preparedStatement.setString(3, jsonObject.getString("cur_date"));
                preparedStatement.setString(4, jsonObject.getString("keyword"));
                preparedStatement.setLong(5, jsonObject.getLongValue("keyword_count"));
                int result = preparedStatement.executeUpdate();
                System.out.println("✅ MySQL写入成功，影响行数: " + result);
            } catch (Exception e) {
                System.err.println("❌ MySQL写入失败: " + e.getMessage());
                System.err.println("📝 失败数据: " + value);
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            if (preparedStatement != null) {
                preparedStatement.close();
                System.out.println("✅ PreparedStatement关闭成功");
            }
            if (connection != null) {
                connection.close();
                System.out.println("✅ MySQL连接关闭成功");
            }
        }
    }

    // MySQL Sink for SKU Order Data
    private static class MysqlSkuOrderSink extends RichSinkFunction<String> {
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                System.out.println("✅ MySQL订单连接成功: " + MYSQL_URL);
                String sql = "INSERT INTO dws_trade_sku_order_window (stt, edt, cur_date, sku_id, sku_name, original_amount, activity_reduce_amount, coupon_reduce_amount, order_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                preparedStatement = connection.prepareStatement(sql);
                System.out.println("✅ MySQL订单PreparedStatement创建成功");
            } catch (Exception e) {
                System.err.println("❌ MySQL订单连接失败: " + e.getMessage());
                throw e;
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            try {
                System.out.println("💾 尝试写入MySQL SKU订单数据: " + value);
                JSONObject jsonObject = JSON.parseObject(value);
                preparedStatement.setString(1, jsonObject.getString("stt"));
                preparedStatement.setString(2, jsonObject.getString("edt"));
                preparedStatement.setString(3, jsonObject.getString("cur_date"));
                preparedStatement.setObject(4, jsonObject.get("sku_id")); // 处理可能的字符串类型
                preparedStatement.setString(5, jsonObject.getString("sku_name"));
                preparedStatement.setBigDecimal(6, jsonObject.getBigDecimal("original_amount"));
                preparedStatement.setBigDecimal(7, jsonObject.getBigDecimal("activity_reduce_amount"));
                preparedStatement.setBigDecimal(8, jsonObject.getBigDecimal("coupon_reduce_amount"));
                preparedStatement.setBigDecimal(9, jsonObject.getBigDecimal("order_amount"));
                int result = preparedStatement.executeUpdate();
                System.out.println("✅ MySQL订单写入成功，影响行数: " + result);
            } catch (Exception e) {
                System.err.println("❌ MySQL订单写入失败: " + e.getMessage());
                System.err.println("📝 失败数据: " + value);
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            if (preparedStatement != null) {
                preparedStatement.close();
                System.out.println("✅ 订单PreparedStatement关闭成功");
            }
            if (connection != null) {
                connection.close();
                System.out.println("✅ MySQL订单连接关闭成功");
            }
        }
    }

    // MySQL Sink for User Login Data
    private static class MysqlUserLoginSink extends RichSinkFunction<String> {
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                System.out.println("✅ MySQL用户登录连接成功: " + MYSQL_URL);
                String sql = "INSERT INTO dws_user_user_login_window (stt, edt, cur_date, back_ct, uu_ct) VALUES (?, ?, ?, ?, ?)";
                preparedStatement = connection.prepareStatement(sql);
                System.out.println("✅ MySQL用户登录PreparedStatement创建成功");
            } catch (Exception e) {
                System.err.println("❌ MySQL用户登录连接失败: " + e.getMessage());
                throw e;
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            try {
                System.out.println("💾 尝试写入MySQL用户登录数据: " + value);
                JSONObject jsonObject = JSON.parseObject(value);
                preparedStatement.setString(1, jsonObject.getString("stt"));
                preparedStatement.setString(2, jsonObject.getString("edt"));
                preparedStatement.setString(3, jsonObject.getString("cur_date"));
                preparedStatement.setLong(4, jsonObject.getLongValue("back_ct"));
                preparedStatement.setLong(5, jsonObject.getLongValue("uu_ct"));
                int result = preparedStatement.executeUpdate();
                System.out.println("✅ MySQL用户登录写入成功，影响行数: " + result);
            } catch (Exception e) {
                System.err.println("❌ MySQL用户登录写入失败: " + e.getMessage());
                System.err.println("📝 失败数据: " + value);
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            if (preparedStatement != null) {
                preparedStatement.close();
                System.out.println("✅ 用户登录PreparedStatement关闭成功");
            }
            if (connection != null) {
                connection.close();
                System.out.println("✅ MySQL用户登录连接关闭成功");
            }
        }
    }
}
