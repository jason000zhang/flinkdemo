package com.calabar.flinkDemo.wordCount;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.HashMap;
import java.util.Map;

public class MySource1 extends RichSourceFunction<String> {

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        Map<String,Object> map = new HashMap<>();


        map.put("userid","1");
        map.put("orderid",1627291884000L);
        map.put("behave","pay");
        map.put("pay",2);
        ctx.collect(JSON.toJSONString(map));

        map.put("userid","1");
        map.put("orderid",1627291884001L);
        map.put("behave","order");
        map.put("pay",3);
        ctx.collect(JSON.toJSONString(map));


//        Thread.sleep(500);




////        Thread.sleep(500);
//
        map.put("userid","1");
        map.put("orderid",1627291885001L);
        map.put("behave","order");
        map.put("pay",4);
        ctx.collect(JSON.toJSONString(map));
//        Thread.sleep(500);

        map.put("userid","1");
        map.put("orderid",1627291886001L);
        map.put("behave","pay");
        map.put("pay",5);
        ctx.collect(JSON.toJSONString(map));
//        Thread.sleep(1000);



    }
    @Override
    public void cancel() {

    }

}
