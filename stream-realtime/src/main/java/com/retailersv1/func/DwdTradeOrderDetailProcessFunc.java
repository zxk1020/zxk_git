package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTradeOrderDetailProcessFunc extends ProcessFunction<String, String> {
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
            result.put("sku_name", data.getString("sku_name"));
            result.put("sku_num", data.getString("sku_num"));
            result.put("split_original_amount", data.getString("split_original_amount"));
            result.put("split_activity_amount", data.getString("split_activity_amount"));
            result.put("split_coupon_amount", data.getString("split_coupon_amount"));
            result.put("create_time", data.getString("create_time"));
            result.put("ts", jsonObject.getLong("ts"));

            out.collect(result.toJSONString());
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}
