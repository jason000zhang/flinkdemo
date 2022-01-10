package com.calabar.flinkDemo.wordCount;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @Author lifusheng
 * @Date 2022/1/4 10:09
 */
public class TestAssociative {
    public static void main(String[] args) throws Exception {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.12.24.102:9092")
                .setTopics("test")
                .setGroupId("test333")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

//        DataStreamSource<String> kafka =env.fromSource(kafkaSource, WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new TimestampAssignerSupplier<String>() {
//            @Override
//            public SerializableTimestampAssigner<String> createTimestampAssigner(Context context) {
//                return new SerializableTimestampAssigner<String>() {
//                    @Override
//                    public long extractTimestamp(String element, long recordTimestamp) {
//                        JSONObject jsonObject = JSONObject.parseObject(element);
//                        try {
//                            return df.parse(jsonObject.getString("timestamp")).getTime();
//                        } catch (ParseException e) {
//                            e.printStackTrace();
//                        }
//                        return System.currentTimeMillis();
//                    }
//                };
//            }
//        }), "Kafka Source");
        //forBoundedOutOfOrderness(Duration.ofSeconds(2)
        DataStreamSource<String> kafka =env.fromSource(kafkaSource, WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new TimestampAssignerSupplier<String>() {
            @Override
            public SerializableTimestampAssigner<String> createTimestampAssigner(Context context) {
                return new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        JSONObject jsonObject = JSONObject.parseObject(element);
                        try {
                            return df.parse(jsonObject.getString("timestamp")).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return System.currentTimeMillis();
                    }
                };
            }
        }), "Kafka Source");
        kafka.print();
        Pattern<String,String> myPattern = Pattern.<String>begin("A", AfterMatchSkipStrategy.noSkip()).where(new IterativeCondition<String>() {
            @Override
            public boolean filter(String value, Context<String> ctx) throws Exception {
                JSONObject event = JSON.parseObject(value);
                String logType = event.get("sys_log_type").toString();
                System.out.println("进入A事件filter:"+logType);
                if(logType.equals("sys_linux")){
                    System.out.println("命中A事件=========》"+logType);
                    return true;
                }
                return false;
            }
        }).followedBy("B").where(new IterativeCondition<String>() {
            @Override
            public boolean filter(String value, Context<String> ctx) throws Exception {
                JSONObject currentEvent = JSON.parseObject(value);
                String logType = currentEvent.get("sys_log_type").toString();
                System.out.println("进入B事件filter:"+logType);
                if(logType.equals("sys_windows")){
                    Iterator<String> iterator = ctx.getEventsForPattern("A").iterator();
                   while (iterator.hasNext()){
                       String before = iterator.next();
                       JSONObject beforeEvent = JSON.parseObject(before);
                       if(currentEvent.getString("src_ip").equals(beforeEvent.getString("src_ip"))){
                           System.out.println("命中B事件=========》"+currentEvent.getString("src_ip")+"=="+beforeEvent.getString("src_ip"));
                           return true;
                       }

                   }
                }
                return false;
            }
        }).followedBy("C").where(new IterativeCondition<String>() {
            @Override
            public boolean filter(String value, Context<String> ctx) throws Exception {
                JSONObject currentEvent = JSON.parseObject(value);
                String logType = currentEvent.get("sys_log_type").toString();
                System.out.println("进入C事件filter:"+logType);
                if(logType.equals("bro_conn")){
                    Iterator<String> iterator = ctx.getEventsForPattern("B").iterator();
                    while (iterator.hasNext()){
                        String before = iterator.next();
                        JSONObject beforeEvent = JSON.parseObject(before);
                        if(currentEvent.getString("dst_ip").equals(beforeEvent.getString("dst_ip"))){
                            System.out.println("命中C事件=========》"+currentEvent.getString("dst_ip")+"=="+beforeEvent.getString("dst_ip"));
                            return true;
                        }

                    }
                }
                return false;
            }
        }).within(Time.minutes(5));
//        /.within(Time.minutes(5))
//       .times(1).within(Time.minutes(5)).optional().greedy()
        //.times(1).within(Time.minutes(5))
        PatternStream<String> pattern = CEP.pattern( kafka, myPattern);
        pattern.process(new PatternProcessFunction<String, Object>() {

            @Override
            public void processMatch(Map<String, List<String>> match, Context context, Collector<Object> collector) throws Exception {
                System.out.println("触发一次-----------------");
                for (String key : match.keySet()) {
                    System.out.println(key);
                    List<String> datas = match.get(key);
                    for (String data : datas) {
                        System.out.println(data);
                    }
                }
                System.out.println("-------------------------");
            }
        }).print();

  env.execute();
    }
}
