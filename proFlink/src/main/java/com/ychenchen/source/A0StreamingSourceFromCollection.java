package com.ychenchen.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author alexis.yang
 * @since 2020/12/25 11:50 AM
 */
public class A0StreamingSourceFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> data = new ArrayList<>();
        data.add("hello");
        data.add("alexis");
        data.add("flink");
        DataStreamSource<String> stringDataStreamSource = env.fromCollection(data);
        SingleOutputStreamOperator<String> doubled = stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s + s;
            }
        });
        doubled.print().setParallelism(1);
        env.execute("StreamingSourceFromCollection");
    }

}
