// DwsTradeSkuOrderJob.java
package com.retailersv1.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author hp
 */
public class DwsTradeSkuOrderJob {

    public void process(StreamExecutionEnvironment env) {
        try {
            // 从DWD层读取订单明细数据
            DataStreamSource<String> orderDetailDs = env.fromSource(
                    KafkaUtils.buildKafkaSource(
                            com.stream.common.utils.ConfigUtils.getString("kafka.bootstrap.servers"),
                            com.stream.common.utils.ConfigUtils.getString("kafka.dwd.order.detail.topic"),
                            "dws_sku_order_group_" + System.currentTimeMillis(),
                            OffsetsInitializer.earliest()
                    ),
                    WatermarkStrategy.forMonotonousTimestamps(),
                    "read_dwd_order_detail"
            );

            // 解析订单明细数据
            DataStream<JSONObject> orderDetailJsonDs = orderDetailDs
                    .map(value -> {
                        try {
                            return JSON.parseObject(value);
                        } catch (Exception e) {
                            return null;
                        }
                    })
                    .filter(json -> json != null)
                    .name("parse_order_detail");

            // 按SKU ID分组并窗口聚合
            DataStream<String> skuOrderWindowDs = orderDetailJsonDs
                    .keyBy(json -> json.getString("sku_id"))
                    .window(TumblingEventTimeWindows.of(Time.minutes(5))) // 5分钟窗口
                    .aggregate(
                            new SkuOrderAggregateFunction(),
                            new SkuOrderProcessWindowFunction()
                    );


            // 异步关联SKU维度信息（简化实现，实际需要使用异步IO）
            DataStream<String> skuOrderWithDimDs = skuOrderWindowDs
                    .map(jsonStr -> {
                        JSONObject json = JSON.parseObject(jsonStr);
                        // 模拟从HBase获取SKU名称
                        String skuId = json.getString("sku_id");
                        String skuName = getSkuNameFromDim(skuId);
                        json.put("sku_name", skuName);
                        return json.toJSONString();
                    })
                    .name("join_sku_dimension");

            // 添加打印语句
            skuOrderWithDimDs.print("💰 交易域-SKU订单统计").setParallelism(1);

            // 写入Doris - 修改后
            skuOrderWithDimDs.sinkTo(DorisSinkUtils.buildDorisSink("dws_trade_sku_order_window"))
                    .name("sink_sku_order_to_doris")
                    .uid("dws_sku_order_doris_sink");


            System.out.println("✅ 交易域处理完成");

        } catch (Exception e) {
            System.err.println("❌ 交易域处理异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 聚合函数：统计SKU订单金额
// 聚合函数：统计SKU订单金额
    private static class SkuOrderAggregateFunction implements org.apache.flink.api.common.functions.AggregateFunction<JSONObject, JSONObject, JSONObject> {
        @Override
        public JSONObject createAccumulator() {
            JSONObject acc = new JSONObject();
            acc.put("original_amount", BigDecimal.ZERO);
            acc.put("activity_reduce_amount", BigDecimal.ZERO);
            acc.put("coupon_reduce_amount", BigDecimal.ZERO);
            acc.put("order_amount", BigDecimal.ZERO);
            return acc;
        }

        @Override
        public JSONObject add(JSONObject value, JSONObject accumulator) {
            // 添加空值检查，如果字段为null则使用"0"作为默认值
            String splitOriginalAmount = value.getString("split_original_amount");
            if (splitOriginalAmount == null) {
                splitOriginalAmount = "0";
            }

            String splitActivityAmount = value.getString("split_activity_amount");
            if (splitActivityAmount == null) {
                splitActivityAmount = "0";
            }

            String splitCouponAmount = value.getString("split_coupon_amount");
            if (splitCouponAmount == null) {
                splitCouponAmount = "0";
            }

            String splitTotalAmount = value.getString("split_total_amount");
            if (splitTotalAmount == null) {
                splitTotalAmount = "0";
            }

            accumulator.put("original_amount",
                    new BigDecimal(accumulator.getString("original_amount")).add(
                            new BigDecimal(splitOriginalAmount)));
            accumulator.put("activity_reduce_amount",
                    new BigDecimal(accumulator.getString("activity_reduce_amount")).add(
                            new BigDecimal(splitActivityAmount)));
            accumulator.put("coupon_reduce_amount",
                    new BigDecimal(accumulator.getString("coupon_reduce_amount")).add(
                            new BigDecimal(splitCouponAmount)));
            accumulator.put("order_amount",
                    new BigDecimal(accumulator.getString("order_amount")).add(
                            new BigDecimal(splitTotalAmount)));
            return accumulator;
        }

        @Override
        public JSONObject getResult(JSONObject accumulator) {
            return accumulator;
        }

        @Override
        public JSONObject merge(JSONObject a, JSONObject b) {
            // 同样添加空值检查
            String aOriginalAmount = a.getString("original_amount");
            if (aOriginalAmount == null) {
                aOriginalAmount = "0";
            }

            String bOriginalAmount = b.getString("original_amount");
            if (bOriginalAmount == null) {
                bOriginalAmount = "0";
            }

            String aActivityReduceAmount = a.getString("activity_reduce_amount");
            if (aActivityReduceAmount == null) {
                aActivityReduceAmount = "0";
            }

            String bActivityReduceAmount = b.getString("activity_reduce_amount");
            if (bActivityReduceAmount == null) {
                bActivityReduceAmount = "0";
            }

            String aCouponReduceAmount = a.getString("coupon_reduce_amount");
            if (aCouponReduceAmount == null) {
                aCouponReduceAmount = "0";
            }

            String bCouponReduceAmount = b.getString("coupon_reduce_amount");
            if (bCouponReduceAmount == null) {
                bCouponReduceAmount = "0";
            }

            String aOrderAmount = a.getString("order_amount");
            if (aOrderAmount == null) {
                aOrderAmount = "0";
            }

            String bOrderAmount = b.getString("order_amount");
            if (bOrderAmount == null) {
                bOrderAmount = "0";
            }

            JSONObject merged = new JSONObject();
            merged.put("original_amount",
                    new BigDecimal(aOriginalAmount).add(
                            new BigDecimal(bOriginalAmount)));
            merged.put("activity_reduce_amount",
                    new BigDecimal(aActivityReduceAmount).add(
                            new BigDecimal(bActivityReduceAmount)));
            merged.put("coupon_reduce_amount",
                    new BigDecimal(aCouponReduceAmount).add(
                            new BigDecimal(bCouponReduceAmount)));
            merged.put("order_amount",
                    new BigDecimal(aOrderAmount).add(
                            new BigDecimal(bOrderAmount)));
            return merged;
        }
    }


    // 窗口处理函数：生成最终结果
    private static class SkuOrderProcessWindowFunction extends ProcessWindowFunction<JSONObject, String, String, TimeWindow> {
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public void process(String key, Context context, Iterable<JSONObject> elements, Collector<String> out) {
            JSONObject aggData = elements.iterator().next();
            TimeWindow window = context.window();

            JSONObject result = new JSONObject();
            result.put("stt", new Timestamp(window.getStart()).toString());
            result.put("edt", new Timestamp(window.getEnd()).toString());
            result.put("cur_date", dateFormat.format(new Date(window.getStart())));
            result.put("sku_id", key);
            result.put("original_amount", aggData.getString("original_amount"));
            result.put("activity_reduce_amount", aggData.getString("activity_reduce_amount"));
            result.put("coupon_reduce_amount", aggData.getString("coupon_reduce_amount"));
            result.put("order_amount", aggData.getString("order_amount"));

            out.collect(result.toJSONString());
        }
    }

    // 模拟从维度获取SKU名称
    private static String getSkuNameFromDim(String skuId) {
        // 实际实现中应该从HBase或其他维度存储中查询SKU名称
        return "SKU_NAME_" + skuId;
    }
}
