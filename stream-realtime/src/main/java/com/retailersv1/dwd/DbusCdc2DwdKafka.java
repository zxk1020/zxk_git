package com.retailersv1.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.*;
import com.stream.common.utils.*;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * DWD层数据处理主类
 * 负责从Kafka读取ODS层数据，清洗并关联维度信息，输出到DWD层Kafka主题
 */
public class DbusCdc2DwdKafka {

    // 日志相关主题
    private static final String kafka_topic_base_log_data = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");
    // 数据库CDC主题
    private static final String kafka_topic_db_data = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");

    // 输出标签 - 日志类型
    private static final OutputTag<String> startTag = new OutputTag<String>("start") {
    };
    private static final OutputTag<String> pageTag = new OutputTag<String>("page") {
    };
    private static final OutputTag<String> actionTag = new OutputTag<String>("action") {
    };
    private static final OutputTag<String> displayTag = new OutputTag<String>("display") {
    };
    private static final OutputTag<String> errTag = new OutputTag<String>("err") {
    };
    private static final OutputTag<String> orderDetailTag = new OutputTag<String>("order_detail") {
    };
    private static final OutputTag<String> cartAddTag = new OutputTag<String>("cart_add") {
    };
    private static final OutputTag<String> orderCancelTag = new OutputTag<String>("order_cancel") {
    };
    private static final OutputTag<String> paymentSuccessTag = new OutputTag<String>("payment_success") {
    };
    private static final OutputTag<String> refundTag = new OutputTag<String>("refund") {
    };
    private static final OutputTag<String> commentInfoTag = new OutputTag<String>("comment_info") {
    };
    private static final OutputTag<String> userRegisterTag = new OutputTag<String>("user_register") {
    };
    private static final OutputTag<String> couponGetTag = new OutputTag<String>("coupon_get") {
    };
    private static final OutputTag<String> couponUseTag = new OutputTag<String>("coupon_use") {
    };

    // 动态主题名（从配置文件加载）
    private static final String kafka_dwd_start_topic = ConfigUtils.getString("kafka.dwd.start.topic");
    private static final String kafka_dwd_page_topic = ConfigUtils.getString("kafka.dwd.page.topic");
    private static final String kafka_dwd_action_topic = ConfigUtils.getString("kafka.dwd.action.topic");
    private static final String kafka_dwd_display_topic = ConfigUtils.getString("kafka.dwd.display.topic");
    private static final String kafka_dwd_err_topic = ConfigUtils.getString("kafka.dwd.err.topic");
    private static final String kafka_dwd_order_detail_topic = ConfigUtils.getString("kafka.dwd.order.detail.topic");
    private static final String kafka_dwd_cart_add_topic = ConfigUtils.getString("kafka.dwd.cart.add.topic");
    private static final String kafka_dwd_order_cancel_topic = ConfigUtils.getString("kafka.dwd.order.cancel.topic");
    private static final String kafka_dwd_payment_success_topic = ConfigUtils.getString("kafka.dwd.payment.success.topic");
    private static final String kafka_dwd_refund_topic = ConfigUtils.getString("kafka.dwd.refund.topic");
    private static final String kafka_dwd_comment_info_topic = ConfigUtils.getString("kafka.dwd.comment.info.topic");
    private static final String kafka_dwd_user_register_topic = ConfigUtils.getString("kafka.dwd.user.register.topic");
    private static final String kafka_dwd_toolCoupon_get_topic = ConfigUtils.getString("kafka.dwd.toolCoupon.get.topic");
    private static final String kafka_dwd_toolCoupon_use_topic = ConfigUtils.getString("kafka.dwd.toolCoupon.use.topic");

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        System.out.println("🚀 启动 DbusCdc2DwdKafka 程序...");
        System.out.println("🕐 启动时间: " + new Date());

        CommonUtils.printCheckPropEnv(
                false,
                kafka_topic_base_log_data,
                kafka_topic_db_data,
                kafka_botstrap_servers,
                kafka_dwd_start_topic,
                kafka_dwd_page_topic,
                kafka_dwd_action_topic,
                kafka_dwd_display_topic,
                kafka_dwd_err_topic,
                kafka_dwd_order_detail_topic,
                kafka_dwd_cart_add_topic,
                kafka_dwd_order_cancel_topic,
                kafka_dwd_payment_success_topic,
                kafka_dwd_refund_topic,
                kafka_dwd_comment_info_topic,
                kafka_dwd_user_register_topic,
                kafka_dwd_toolCoupon_get_topic,
                kafka_dwd_toolCoupon_use_topic
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        System.out.println("🔧 Flink环境配置完成");

//        processLogData(env);
               processDbCdcData(env);

        System.out.println("🏁 准备提交Flink作业...");
        env.execute("Job-DbusCdc2DwdKafka");
    }

    /**
     * 处理日志数据（流量域）
     */
    /**
     * 处理日志数据（流量域）
     */
    private static void processLogData(StreamExecutionEnvironment env) {
        System.out.println("=== 开始处理日志数据 ===");

        DataStreamSource<String> logKafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        kafka_topic_base_log_data,
                        new Date().toString(),
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_kafka_realtime_log"
        );

        // 添加打印监控
        logKafkaSourceDs.print("📥 原始日志数据");

        SingleOutputStreamOperator<JSONObject> logJsonDs = logKafkaSourceDs.process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            if (JSON.isValid(value)) {
                                JSONObject jsonObject = JSON.parseObject(value);
                                if (jsonObject.getJSONObject("common") != null) {
                                    out.collect(jsonObject);
                                } else {
                                    System.out.println("❌ 缺少 common 字段的数据: " + value);
                                    ctx.output(errTag, value);
                                }
                            } else {
                                System.out.println("❌ 无效JSON数据: " + value);
                                ctx.output(errTag, value);
                            }
                        } catch (Exception e) {
                            System.out.println("❌ JSON解析异常: " + value + ", 错误: " + e.getMessage());
                            ctx.output(errTag, value);
                        }
                    }
                }).uid("log_json_parse")
                .name("log_json_parse");

        // 添加打印监控
        logJsonDs.print("📄 解析后的JSON数据");

        SingleOutputStreamOperator<String> logProcessedDs = logJsonDs.process(new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            System.out.println("🏷️ 处理日志数据: ");

                            // 直接检查JSON对象中是否包含特定字段进行分流
                            if (value.containsKey("start")) {
                                System.out.println("🚀 启动日志: " + value.toJSONString());
                                ctx.output(startTag, value.toJSONString());
                            }
                            if (value.containsKey("page")) {
                                System.out.println("📄 页面日志: " + value.toJSONString());
                                ctx.output(pageTag, value.toJSONString());
                            }
                            if (value.containsKey("action")) {
                                System.out.println("👆 动作日志: " + value.toJSONString());
                                ctx.output(actionTag, value.toJSONString());
                            }
                            if (value.containsKey("displays")) {
                                System.out.println("👀 曝光日志: " + value.toJSONString());
                                ctx.output(displayTag, value.toJSONString());
                            }
                            if (value.containsKey("err")) {
                                System.out.println("⚠️ 错误日志: " + value.toJSONString());
                                ctx.output(errTag, value.toJSONString());
                            }

                            // 如果没有任何已知字段，发送到错误流
                            if (!value.containsKey("start") &&
                                    !value.containsKey("page") &&
                                    !value.containsKey("action") &&
                                    !value.containsKey("displays") &&
                                    !value.containsKey("err")) {
                                System.out.println("❓ 未知类型日志: " + value.toJSONString());
                                ctx.output(errTag, value.toJSONString());
                            }

                        } catch (Exception e) {
                            System.out.println("❌ 日志分流异常: " + value.toJSONString() + ", 错误: " + e.getMessage());
                            ctx.output(errTag, value.toJSONString());
                        }
                    }
                }).uid("log_split_stream")
                .name("log_split_stream");

        // 添加打印监控
        logProcessedDs.print("🔀 分流后的主数据流");

        // 启动日志
        DataStream<String> startDs = logProcessedDs.getSideOutput(startTag);
        startDs.print("🚀 启动日志侧流");
        startDs.process(new DwdTrafficStartProcessFunc())
                .uid("dwd_traffic_start_process")
                .name("dwd_traffic_start_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_start_topic))
                .uid("dwd_traffic_start_sink")
                .name("dwd_traffic_start_sink");

        // 页面浏览
        DataStream<String> pageDs = logProcessedDs.getSideOutput(pageTag);
        pageDs.print("📄 页面日志侧流");
        pageDs.process(new DwdTrafficPageProcessFunc())
                .uid("dwd_traffic_page_process")
                .name("dwd_traffic_page_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_page_topic))
                .uid("dwd_traffic_page_sink")
                .name("dwd_traffic_page_sink");

        // 用户动作
        DataStream<String> actionDs = logProcessedDs.getSideOutput(actionTag);
        actionDs.print("👆 动作日志侧流");
        actionDs.process(new DwdTrafficActionProcessFunc())
                .uid("dwd_traffic_action_process")
                .name("dwd_traffic_action_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_action_topic))
                .uid("dwd_traffic_action_sink")
                .name("dwd_traffic_action_sink");

        // 曝光日志
        DataStream<String> displayDs = logProcessedDs.getSideOutput(displayTag);
        displayDs.print("👀 曝光日志侧流");
        displayDs.process(new DwdTrafficDisplayProcessFunc())
                .uid("dwd_traffic_display_process")
                .name("dwd_traffic_display_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_display_topic))
                .uid("dwd_traffic_display_sink")
                .name("dwd_traffic_display_sink");

        // 错误日志
        DataStream<String> errDs = logProcessedDs.getSideOutput(errTag);
        errDs.print("⚠️ 错误日志侧流");
        errDs.sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_err_topic))
                .uid("dwd_traffic_err_sink")
                .name("dwd_traffic_err_sink");

        System.out.println("=== 日志数据处理完成 ===");
    }



    /**
     * 处理数据库CDC数据
     */

    private static void processDbCdcData(StreamExecutionEnvironment env) {
        System.out.println("=== 开始处理数据库CDC数据 ===");

        // 定义维度表集合（这些表已经存到HBase，不需要在DWD层处理）
        final Set<String> dimensionTables = new HashSet<>(Arrays.asList(
                "activity_info",
                "activity_rule",
                "activity_sku",
                "base_category1",
                "base_category2",
                "base_category3",
                "base_dic",
                "base_province",
                "base_region",
                "base_trademark",
                "coupon_info",
                "coupon_range",
                "financial_sku_cost",
                "sku_info",
                "spu_info",
                "user_info"
        ));

        DataStreamSource<String> dbKafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        kafka_topic_db_data,
                        new Date().toString(),
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_kafka_cdc_db"
        );

        // 添加打印监控
        dbKafkaSourceDs.print("📥 原始CDC数据");

        SingleOutputStreamOperator<JSONObject> dbJsonDs = dbKafkaSourceDs.process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            if (JSON.isValid(value)) {
                                JSONObject jsonObject = JSON.parseObject(value);
                                JSONObject data = jsonObject.getJSONObject("after");
                                if (data != null) {
                                    System.out.println("📊 CDC有效数据: " + value);
                                    out.collect(jsonObject);
                                } else {
                                    System.out.println("❌ CDC数据缺少after字段: " + value);
                                    ctx.output(errTag, value);
                                }
                            } else {
                                System.out.println("❌ CDC无效JSON数据: " + value);
                                ctx.output(errTag, value);
                            }
                        } catch (Exception e) {
                            System.out.println("❌ CDC JSON解析异常: " + value + ", 错误: " + e.getMessage());
                            ctx.output(errTag, value);
                        }
                    }
                }).uid("db_json_parse")
                .name("db_json_parse");

        // 添加打印监控
        dbJsonDs.print("📄 解析后的CDC JSON数据");

        SingleOutputStreamOperator<String> dbProcessedDs = dbJsonDs.process(new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            String table = value.getJSONObject("source").getString("table");
                            String op = value.getString("op");
                            System.out.println("📋 CDC表名: " + table + ", 操作类型: " + op);

                            // 如果是维度表，直接忽略（因为已经存到HBase了）
                            if (dimensionTables.contains(table)) {
                                System.out.println("📐 维度表数据(已存HBase): " + table + ", 数据: " + value.toJSONString());
                                return; // 直接忽略，不发送到任何流
                            }

                            // 处理事实表（非维度表）
                            switch (table) {
                                case "order_detail":
                                    System.out.println("📦 订单明细数据: " + value.toJSONString());
                                    ctx.output(orderDetailTag, value.toJSONString());
                                    break;
                                case "cart_info":
                                    System.out.println("🛒 购物车数据: " + value.toJSONString());
                                    ctx.output(cartAddTag, value.toJSONString());
                                    break;
                                case "order_info":
                                    System.out.println("📝 订单信息数据: " + value.toJSONString());
                                    // 处理所有操作类型，不仅仅是update
//                                    if ("update".equals(op)) {
                                        System.out.println("❌ 订单取消数据: " + value.toJSONString());
                                        ctx.output(orderCancelTag, value.toJSONString());
//                                    }
                                    // 可以在这里添加其他订单操作的处理
                                    break;
                                case "payment_info":
                                    System.out.println("💳 支付信息数据: " + value.toJSONString());
                                    // 处理所有操作类型，不仅仅是update
//                                    if ("update".equals(op)) {
                                    System.out.println("💰 支付成功数据: " + value.toJSONString());
                                    ctx.output(paymentSuccessTag, value.toJSONString());
//                                    }
                                    // 可以在这里添加其他支付操作的处理
                                    break;
                                case "order_refund_info":
                                    System.out.println("💸 退单数据: " + value.toJSONString());
                                    ctx.output(refundTag, value.toJSONString());
                                    break;
                                case "comment_info":
                                    System.out.println("💬 评论数据: " + value.toJSONString());
                                    ctx.output(commentInfoTag, value.toJSONString());
                                    break;
                                case "coupon_use":
                                    System.out.println("🎟️ 优惠券使用数据: " + value.toJSONString());
//                                    if ("insert".equals(op)) {
//                                    System.out.println("📥 领券数据: " + value.toJSONString());
                                    ctx.output(couponGetTag, value.toJSONString());
//                                    } else if ("update".equals(op)) {
//                                        System.out.println("📤 用券数据: " + value.toJSONString());
                                    ctx.output(couponUseTag, value.toJSONString());
//                                    }
                                    break;
                                // 添加其他可能的事实表
                                case "base_attr_value":
                                    System.out.println("🏷️ 属性值数据: " + value.toJSONString());
                                    // 根据业务需求决定如何处理这个表
                                    ctx.output(errTag,value.toJSONString());
                                    break;
                                default:
                                    System.out.println("❓ 未处理的事实表数据 - 表名: " + table + ", 数据: " + value.toJSONString());
                                    ctx.output(errTag,value.toJSONString());
                                    break;
                            }
                        } catch (Exception e) {
                            System.out.println("❌ CDC分流异常: " + value.toJSONString() + ", 错误: " + e.getMessage());
                            ctx.output(errTag,value.toJSONString());
                        }
                    }
                }).uid("db_split_stream")
                .name("db_split_stream");

        // 添加打印监控
        dbProcessedDs.print("🔀 CDC分流后的主数据流");

        // 下单明细
        DataStream<String> orderDetailDs = dbProcessedDs.getSideOutput(orderDetailTag);
        orderDetailDs.print("📦 订单明细侧流");
        orderDetailDs.process(new DwdTradeOrderDetailProcessFunc())
                .uid("dwd_trade_order_detail_process")
                .name("dwd_trade_order_detail_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_order_detail_topic))
                .uid("dwd_trade_order_detail_sink")
                .name("dwd_trade_order_detail_sink");

        // 加购
        DataStream<String> cartAddDs = dbProcessedDs.getSideOutput(cartAddTag);
        cartAddDs.print("🛒 加购侧流");
        cartAddDs.process(new DwdTradeCartAddProcessFunc())
                .uid("dwd_trade_cart_add_process")
                .name("dwd_trade_cart_add_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_cart_add_topic))
                .uid("dwd_trade_cart_add_sink")
                .name("dwd_trade_cart_add_sink");

        // 取消订单
        DataStream<String> orderCancelDs = dbProcessedDs.getSideOutput(orderCancelTag);
        orderCancelDs.print("❌ 取消订单侧流");
        orderCancelDs.process(new DwdTradeOrderCancelProcessFunc())
                .uid("dwd_trade_order_cancel_process")
                .name("dwd_trade_order_cancel_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_order_cancel_topic))
                .uid("dwd_trade_order_cancel_sink")
                .name("dwd_trade_order_cancel_sink");

        // 支付成功
        DataStream<String> paymentSuccessDs = dbProcessedDs.getSideOutput(paymentSuccessTag);
        paymentSuccessDs.print("💰 支付成功侧流");
        paymentSuccessDs.process(new DwdTradeOrderPaymentSuccessProcessFunc())
                .uid("dwd_trade_order_payment_success_process")
                .name("dwd_trade_order_payment_success_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_payment_success_topic))
                .uid("dwd_trade_order_payment_success_sink")
                .name("dwd_trade_order_payment_success_sink");

        // 退单
        DataStream<String> refundDs = dbProcessedDs.getSideOutput(refundTag);
        refundDs.print("💸 退单侧流");
        refundDs.process(new DwdTradeOrderRefundProcessFunc())
                .uid("dwd_trade_order_refund_process")
                .name("dwd_trade_order_refund_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_refund_topic))
                .uid("dwd_trade_order_refund_sink")
                .name("dwd_trade_order_refund_sink");

        // 评论
        DataStream<String> commentInfoDs = dbProcessedDs.getSideOutput(commentInfoTag);
        commentInfoDs.print("💬 评论侧流");
        commentInfoDs.process(new DwdInteractionCommentInfoProcessFunc())
                .uid("dwd_interaction_comment_info_process")
                .name("dwd_interaction_comment_info_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_comment_info_topic))
                .uid("dwd_interaction_comment_info_sink")
                .name("dwd_interaction_comment_info_sink");

        // 用户注册
        DataStream<String> userRegisterDs = dbProcessedDs.getSideOutput(userRegisterTag);
        userRegisterDs.print("👤 用户注册侧流");
        userRegisterDs.process(new DwdTradeOrderRefundProcessFunc())
                .uid("dwd_user_register_process")
                .name("dwd_user_register_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_user_register_topic))
                .uid("dwd_user_register_sink")
                .name("dwd_user_register_sink");

        // 领券
        DataStream<String> couponGetDs = dbProcessedDs.getSideOutput(couponGetTag);
        couponGetDs.print("🎟️ 领券侧流");
        couponGetDs.process(new DwdToolCouponGetProcessFunc())
                .uid("dwd_toolCoupon_get_process")
                .name("dwd_toolCoupon_get_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_toolCoupon_get_topic))
                .uid("dwd_toolCoupon_get_sink")
                .name("dwd_toolCoupon_get_sink");

        // 用券
        DataStream<String> couponUseDs = dbProcessedDs.getSideOutput(couponUseTag);
        couponUseDs.print("✅ 用券侧流");
        couponUseDs.process(new DwdToolCouponUseProcessFunc())
                .uid("dwd_toolCoupon_use_process")
                .name("dwd_toolCoupon_use_process")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dwd_toolCoupon_use_topic))
                .uid("dwd_toolCoupon_use_sink")
                .name("dwd_toolCoupon_use_sink");

        // 脏数据（包括未处理的表和错误数据）
        DataStream<String> dbDirtyDs = dbProcessedDs.getSideOutput(errTag);
        dbDirtyDs.print("⚠️ CDC脏数据侧流（包含未处理的表）");
        dbDirtyDs.sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, ConfigUtils.getString("kafka.dirty.topic")))
                .uid("db_dirty_data_sink")
                .name("db_dirty_data_sink");

        System.out.println("=== 数据库CDC数据处理完成 ===");
    }

}
