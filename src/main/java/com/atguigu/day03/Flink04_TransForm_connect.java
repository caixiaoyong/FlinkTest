package com.atguigu.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * connect / union
 * @author CZY
 * @date 2022/1/10 23:43
 * @description Flink03_TransForm_KeyBy
 */
public class Flink04_TransForm_connect {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从元素读取数据
        DataStreamSource<String> streamSource = env.fromElements("a", "b", "c", "d");
        DataStreamSource<Integer> streamSource1 = env.fromElements(1, 2, 3, 4);

        DataStreamSource<Integer> streamSource2 = env.fromElements(10, 20, 30, 40);
        DataStreamSource<Integer> streamSource3 = env.fromElements(100, 200, 300, 400);

        //TODO 3.使用connect
        ConnectedStreams<String, Integer> connect = streamSource.connect(streamSource1);

        connect.process(new CoProcessFunction<String, Integer, String>() {
            @Override
            public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
            }

            @Override
            public void processElement2(Integer value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value.toString());
            }
        }).print();
        env.execute();
        System.out.println("-----------------------");
        //TODO 使用Union
        streamSource1
                .union(streamSource2)
                .union(streamSource3)
                .print();
        env.execute();
    }
}
