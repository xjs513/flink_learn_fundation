package com.evente.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Test1 {
    public static void main(String[] args) {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String path = "E:\\flink_projects\\flink_learn_fundation\\data\\abc.txt";
        DataSet<String> textFile = environment.readTextFile(path);

        DataSet<Tuple2<String, Integer>> tuples = textFile.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String line, Collector<String> collector) {
                for (String s : line.split(" ")) {
                    collector.collect(s);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) {
                return new Tuple2<>(s, 1);
            }
        });

        DataSet<Tuple2<String, Integer>> sum = tuples.groupBy(0).sum(1);

        try {
            sum.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
