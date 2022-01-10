package com.calabar.flinkDemo.wordCount;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaStream {
    public static void main(String[] args) throws Exception {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "10.12.24.102:9092");
//        properties.setProperty("group.id", "test");
//
//        DataStreamSource<String> kafka = env.addSource(new FlinkKafkaConsumer<String>("test",new SimpleStringSchema(),properties));
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.12.24.102:9092")
                .setTopics("test")
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> kafka =env.fromSource(kafkaSource, WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new TimestampAssignerSupplier<String>() {

            @Override
            public TimestampAssigner<String> createTimestampAssigner(Context context) {
                return new TimestampAssigner<String>() {
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

//        DataStreamSource<String> kafka =env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        OutputTag<String> outputTag = new OutputTag<String>("myOutput"){};
        SingleOutputStreamOperator<Object> process = kafka.process(new ProcessFunction<String, Object>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Object> out) throws Exception {


                if("bro_conn".equals(JSONObject.parseObject(value).getString("sys_log_type"))){
//                    System.out.println("333");
                    ctx.output(outputTag, value);
                }

            }
        });
        DataStream<String> sideOutput = process.getSideOutput(outputTag);

//        sideOutput.print();

//        sideOutput =sideOutput.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new TimestampAssignerSupplier<String>() {
//            @Override
//            public TimestampAssigner<String> createTimestampAssigner(Context context) {
//                return new TimestampAssigner<String>() {
//                    @Override
//                    public long extractTimestamp(String element, long recordTimestamp) {
//                        JSONObject jsonObject = JSONObject.parseObject(element);
//                        System.out.println("2");
//                        try {
//                            return df.parse(jsonObject.getString("timestamp")).getTime();
//                        } catch (ParseException e) {
//                            e.printStackTrace();
//                        }
//                        return System.currentTimeMillis();
//                    }
//                };
//            }
//        }));
        //
//        SingleOutputStreamOperator<Tuple2<String, Long>> map = source.map(new MapFunction<String, Tuple2<String, Long>>() {
//            @Override
//            public Tuple2<String, Long> map(String value) throws Exception {
//                JSONObject jsonObject = JSONObject.parseObject(value);
//                return new Tuple2<String, Long>(jsonObject.getString("timestamp"), 1L);
//            }
//        });
//        map.keyBy(x->x.f0).window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS))).process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
//            @Override
//            public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
//                long sum = 0L;
//                Iterator<Tuple2<String, Long>> iterator = iterable.iterator();
//                while (iterator.hasNext()) {
//                    Tuple2<String, Long> next = iterator.next();
//                    System.out.println(next.f0);
//                    sum += next.f1;
//                }
//                collector.collect(s+","+sum);
//            }
//        }).print();

//        sideOutput.print();
        Pattern<String,String> myPattern = Pattern.<String>begin("start").where(new IterativeCondition<String>() {
            @Override
            public boolean filter(String value, Context<String> ctx) throws Exception {
                System.out.println("11111");
                return true;
            }
        }).followedBy("next").where(new IterativeCondition<String>() {
            @Override
            public boolean filter(String value, Context<String> ctx) throws Exception {
                return true;
            }
        });
        OutputTag<String> lateTag = new OutputTag<String>("lateTag"){};
//        sideOutput=sideOutput.keyBy(new KeySelector<String, Object>() {
//
//            @Override
//            public Object getKey(String value) throws Exception {
//                System.out.println(value+"==========");
//                return JSONObject.parseObject(value).getString("type");
//            }
//        });
        PatternStream<String> pattern = CEP.pattern( sideOutput, myPattern).sideOutputLateData(lateTag);



        SingleOutputStreamOperator<String> resultStream = pattern.select(outputTag,
                /**
                 * 超时的
                 */
                new PatternTimeoutFunction<String, String>() {
                    @Override
                    public String timeout(Map<String, List<String>> pattern, long timeoutTimestamp) throws Exception {
                        List<String> startList = pattern.get("start");
                        String tuple3 = startList.get(0);
                        System.out.println(tuple3+"============");
                        return null;
                    }
                }, new PatternSelectFunction<String, String>() {
                    @Override
                    public String select(Map<String, List<String>> pattern) throws Exception {
                        System.out.println(pattern);
                        System.out.println(pattern);
                        //匹配上第一个条件的
                        return "endList.get(0)";
                    }
                }
        );
        resultStream.getSideOutput(lateTag).print();
        resultStream.print();
//        PatternStream<String> pattern = CEP.pattern( sideOutput, myPattern).inEventTime().select(new PatternTimeoutFunction<String, R>() {
//            @Override
//            public Object timeout(Map<String, List<String>> map, long l) throws Exception {
//                System.out.println(map);
//                return null;
//            }
//        }).print();
//        pattern.process(new PatternProcessFunction<String, Object>() {
//
//            @Override
//            public void processMatch(Map<String, List<String>> map, Context context, Collector<Object> collector) throws Exception {
//                System.out.println("55");
//                collector.collect(map);
//            }
//        }).print();

        env.execute();
    }
}
