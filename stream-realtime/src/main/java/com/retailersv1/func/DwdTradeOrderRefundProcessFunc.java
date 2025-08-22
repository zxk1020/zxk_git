package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTradeOrderRefundProcessFunc extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject data = jsonObject.getJSONObject("after");

            JSONObject result = new JSONObject();
            result.put("id", data.getString("id"));
            result.put("order_id", data.getString("order_id"));
            result.put("user_id", data.getString("user_id"));
            result.put("sku_id", data.getString("sku_id"));
            result.put("refund_num", data.getInteger("refund_num")); // 修改为Integer类型
            result.put("refund_amount", data.getString("refund_amount"));
            result.put("create_time", data.getLong("create_time")); // 修改为Long类型
            result.put("ts", jsonObject.getLong("ts_ms")); // 字段名修正

            out.collect(result.toJSONString());
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}

