
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

public class TestPattern {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1);
        /**
         *  接收source并将数据转换成一个tuple
         */
        DataStream<Map<String,Object>> myDataStream  = env.addSource(new MySource()).map(new MapFunction<String, Map<String,Object>>() {
            @Override
            public Map<String,Object> map(String value) throws Exception {

                JSONObject json = JSON.parseObject(value);

                return (Map) json;
            }
        }).assignTimestampsAndWatermarks(new TimestampExtractor());
        myDataStream.keyBy(new KeySelector<Map<String, Object>, Object>() {
            @Override
            public Object getKey(Map<String, Object> stringObjectMap) throws Exception {
                return stringObjectMap.get("userid");
            }
        }).timeWindow(Time.seconds(2)).trigger(new Trigger<Map<String, Object>, TimeWindow>() {
            @Override
            public TriggerResult onElement(Map<String, Object> stringObjectMap, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                System.out.println("on element"+System.currentTimeMillis());
                return TriggerResult.FIRE;
            }

            @Override
            public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                return null;
            }

            @Override
            public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                System.out.println("=====event time"+System.currentTimeMillis());
                return TriggerResult.CONTINUE;
            }

            @Override
            public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

            }
        }).aggregate(new AggregateFunction<Map<String, Object>, Tuple2<Long,Long>,Double>(){


            @Override
            public Tuple2<Long, Long> createAccumulator() {
                return new Tuple2<>(0L, 0L);
            }

            @Override
            public Tuple2<Long, Long> add(Map<String, Object> stringObjectMap, Tuple2<Long, Long> longDoubleTuple2) {
                return new Tuple2<>(longDoubleTuple2.f0 + (Integer)stringObjectMap.get("pay"), longDoubleTuple2.f1 + 1L);
            }

            @Override
            public Double getResult(Tuple2<Long, Long> longDoubleTuple2) {
                return ((double) longDoubleTuple2.f0) / longDoubleTuple2.f1;
            }

            @Override
            public Tuple2<Long, Long> merge(Tuple2<Long, Long> longDoubleTuple2, Tuple2<Long, Long> acc1) {
                return null;
            }
        }).print();



//        Expression [] expressions = new Expression[3];
//
//        expressions[0]=$("userid");
//        expressions[1]=$("orderid").rowtime();
//        expressions[2]=$("behave");
//        Table table =tableEnv.fromDataStream(myDataStream);
//        table.printSchema();
//
//        GroupWindowedTable window = table.window(Tumble.over("1.seconds").on($("orderid")).as("w"));
//
////        window.groupBy("w,*").select("w.start as aa,w.end as bb").execute().print();
//
//        window.groupBy("w").select("w.start as aa,count(1)").execute().print();
//        /**
//         * 定义一个规则
//         * 接受到behave是order以后，下一个动作必须是pay才算符合这个需求
//         */
//

        Pattern<Map<String,Object>, Map<String,Object>> myPattern = Pattern.<Map<String,Object>>begin("start", AfterMatchSkipStrategy.skipPastLastEvent()).where(new IterativeCondition<Map<String,Object>>() {
            @Override
            public boolean filter(Map<String,Object> value, Context<Map<String,Object>> ctx) throws Exception {
                System.out.println("11111");
                return true;
            }
        }).followedBy("next").where(new IterativeCondition<Map<String,Object>>() {
            @Override
            public boolean filter(Map<String,Object> value, Context<Map<String,Object>> ctx) throws Exception {
                System.out.println(222);
                return true;
            }
        });


        PatternStream<Map<String,Object>> pattern = CEP.pattern( myDataStream, myPattern);
        pattern.process(new PatternProcessFunction<Map<String, Object>, Object>() {
            @Override
            public void processMatch(Map<String, List<Map<String, Object>>> map, Context context, Collector<Object> collector) throws Exception {
                System.out.println(333);
            }
        }).print();



//        //记录超时的订单
//        OutputTag<Map<String,Object>> outputTag = new OutputTag<Map<String,Object>>("myOutput"){};
//
//        SingleOutputStreamOperator<Map<String,Object>> resultStream = pattern.select(outputTag,
//                /**
//                 * 超时的
//                 */
//                new PatternTimeoutFunction<Map<String,Object>, Map<String,Object>>() {
//                    @Override
//                    public Map<String,Object> timeout(Map<String, List<Map<String,Object>>> pattern, long timeoutTimestamp) throws Exception {
//                        List<Map<String,Object>> startList = pattern.get("start");
//                        Map<String,Object> tuple3 = startList.get(0);
//                        return null;
//                    }
//                }, new PatternSelectFunction<Map<String,Object>, Map<String,Object>>() {
//                    @Override
//                    public Map<String,Object> select(Map<String, List<Map<String,Object>>> pattern) throws Exception {
//                        //匹配上第一个条件的
//                        List<Map<String,Object>> startList = pattern.get("start");
//                        //匹配上第二个条件的
//                        List<Map<String,Object>> endList = pattern.get("next");
//
//                        Map<String,Object> tuple3 = endList.get(0);
//                        return endList.get(0);
//                    }
//                }
//        );
//        //输出匹配上规则的数据
//        resultStream.print();

//        //输出超时数据的流
//        DataStream<Tuple6<String, String, String,String, String, String>> sideOutput = resultStream.getSideOutput(outputTag);
//        sideOutput.print();

        env.execute("Test CEP");
    }
}

