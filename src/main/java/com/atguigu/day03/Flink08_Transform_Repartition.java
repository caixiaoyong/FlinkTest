package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CZY
 * @date 2022/1/11 16:55
 * @description Flink8_Transform_Repartition
 */
public class Flink08_Transform_Repartition {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //2.使用端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 5555);

        //使用lambda表达式
        SingleOutputStreamOperator<String> map = streamSource.map(r -> r).setParallelism(2);

        KeyedStream<String, String> keyDStream = map.keyBy(value -> value);

        DataStream<String> shuffleDStream = map.shuffle();

        DataStream<String> rebalance = map.rebalance();

        DataStream<String> rescale = map.rescale();

        map.print("Origin...").setParallelism(2);
        keyDStream.print("keyDStream...");
        shuffleDStream.print("shuffleDStream...");
        rebalance.print("rebalance...");
        rescale.print("rescale...");
        System.out.println("--------------------");
        env.execute();
    }
}
