package com.evente.job;

import com.evente.source.SimpleSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestCustomSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        SimpleSource source = new SimpleSource(10000);
        DataStreamSource<Long> longDataStreamSource = environment.addSource(source);

        longDataStreamSource.print();

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
