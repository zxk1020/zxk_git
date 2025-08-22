package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdToolCouponGetProcessFunc extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject data = jsonObject.getJSONObject("after");

            // 只处理已使用的优惠券
            String couponStatus = data.getString("coupon_status");
            if ("1401".equals(couponStatus)) { // 假设1402代表已使用状态

                JSONObject result = new JSONObject();
                result.put("id", data.getString("id"));
                result.put("user_id", data.getString("user_id"));
                result.put("coupon_id", data.getString("coupon_id"));
                result.put("get_time", data.getLong("get_time"));
                result.put("ts", jsonObject.getLong("ts_ms")); // 字段名修正

                out.collect(result.toJSONString());
            }
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}

