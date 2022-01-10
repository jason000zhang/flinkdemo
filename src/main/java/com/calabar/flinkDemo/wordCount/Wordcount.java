package com.calabar.flinkDemo.wordCount;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class Wordcount {
    public static void main(String[] args) throws Exception {
        System.out.println(System.currentTimeMillis());

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?",
                "I think I hear them. Stand, ho! Who's there?");

        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);
        wordCounts.print();
        JobExecutionResult result = env.execute("My Flink Job");
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " to execute");
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                //  for (String word2 : line.split(",")) {
                out.collect(new Tuple2<>(word, 1));
                //     }
            }
        }
    }
}
