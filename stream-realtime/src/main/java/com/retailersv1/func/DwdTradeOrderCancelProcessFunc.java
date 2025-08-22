package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTradeOrderCancelProcessFunc extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject data = jsonObject.getJSONObject("after");

            // 只处理取消状态的订单(根据order_status判断)
            String orderStatus = data.getString("order_status");
            if ("1003".equals(orderStatus)) { // 假设1003代表取消状态
                JSONObject result = new JSONObject();
                result.put("id", data.getString("id"));
                result.put("user_id", data.getString("user_id"));
                result.put("cancel_time", data.getLong("operate_time")); // 使用操作时间作为取消时间
                result.put("sku_id", data.getString("sku_id")); // 这个字段在order_info中没有，需要从order_detail获取
                result.put("split_total_amount", data.getString("total_amount"));
                result.put("ts", jsonObject.getLong("ts_ms")); // 字段名修正

                out.collect(result.toJSONString());
            }
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}

