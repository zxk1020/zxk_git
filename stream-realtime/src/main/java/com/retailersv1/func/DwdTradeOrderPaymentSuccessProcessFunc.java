package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTradeOrderPaymentSuccessProcessFunc extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject data = jsonObject.getJSONObject("after");

            // 只处理支付成功的订单
            String paymentStatus = data.getString("payment_status");
            if ("1603".equals(paymentStatus)) { // 根据样例数据，1603代表支付成功

                JSONObject result = new JSONObject();
                result.put("id", data.getString("id"));
                result.put("order_id", data.getString("order_id"));
                result.put("user_id", data.getString("user_id"));
                result.put("payment_type", data.getString("payment_type"));
                result.put("callback_time", data.getLong("callback_time")); // 修改为Long类型
                result.put("split_payment_amount", data.getString("total_amount")); // 字段名修正
                result.put("ts", jsonObject.getLong("ts_ms")); // 字段名修正

                out.collect(result.toJSONString());
            }
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}

