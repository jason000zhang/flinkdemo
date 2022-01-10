package com.calabar.flinkDemo.wordCount;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.util.Collector;
import org.codehaus.janino.SimpleCompiler;

import java.util.concurrent.TimeUnit;

/**
 * <p/>
 * <li>@author: jyj019 </li>
 * <li>Date: 2018/9/3 11:07</li>
 * <li>@version: 2.0.0 </li>
 * <li>@since JDK 1.8 </li>
 */
public class WordCountExample {
    public static void main(String[] args) throws Exception {
        System.out.println(System.currentTimeMillis());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        System.out.println(System.currentTimeMillis());

// or TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);
        DataStream<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?",
                "I think I hear them. Stand, ho! Who's there?");

        DataStream<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new LineSplitter());
//                .groupBy(0)
//                .sum(1);
        Table table =tableEnv.fromDataStream(text);
        System.out.println(System.currentTimeMillis());
        tableEnv.createTemporaryView("test",wordCounts);
        tableEnv.sqlQuery("select f0 from test").execute().print();
        System.out.println(System.currentTimeMillis());
//        tableEnv.sqlQuery("select count(f0) from test").execute();
//
//        System.out.println(System.currentTimeMillis());
//        table.printSchema();
//        table.as("a,b").printSchema();
//        tableEnv.toDataSet(table, Row.class).print();
//        table.printSchema();
//        table = table.groupBy("name").select("*");
//        table.printSchema();
//        tableEnv.toDataSet(table, Row.class).print();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                    out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}