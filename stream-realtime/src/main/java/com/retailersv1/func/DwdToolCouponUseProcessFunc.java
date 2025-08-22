package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdToolCouponUseProcessFunc extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject data = jsonObject.getJSONObject("after");

            // 只处理已使用的优惠券
            String couponStatus = data.getString("coupon_status");
            if ("1402".equals(couponStatus)) { // 假设1402代表已使用状态

                JSONObject result = new JSONObject();
                result.put("id", data.getString("id"));
                result.put("user_id", data.getString("user_id"));
                result.put("coupon_id", data.getString("coupon_id"));
                result.put("order_id", data.getString("order_id"));
//                result.put("use_time", data.getLong("use_time")); // 需要添加use_time字段到数据表中
//                result.put("benefit_amount", data.getString("benefit_amount")); // 需要添加benefit_amount字段到数据表中
                result.put("ts", jsonObject.getLong("ts_ms")); // 字段名修正

                out.collect(result.toJSONString());
            }
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}

