import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

public class MySource extends RichSourceFunction<String> {

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        Map<String,String> map = new HashMap<>();


        map.put("userid","2");
        map.put("orderid","1627291885001");
        map.put("behave","pay");
        ctx.collect(JSON.toJSONString(map));

        map.put("userid","1");
        map.put("orderid","1627291880000");
        map.put("behave","order");
        ctx.collect(JSON.toJSONString(map));


//        Thread.sleep(500);




////        Thread.sleep(500);
//
//        map.put("userid","1");
//        map.put("orderid","1627291884004");
//        map.put("behave","order");
//        ctx.collect(JSON.toJSONString(map));
////        Thread.sleep(500);
//
//        map.put("userid","2");
//        map.put("orderid","1627291884003");
//        map.put("behave","pay");
//        ctx.collect(JSON.toJSONString(map));
//        Thread.sleep(1000);



    }
    @Override
    public void cancel() {

    }

}
