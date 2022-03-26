package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 无界流
 * @author CZY
 * @date 2021/12/27 19:58
 * @description Flink03_Stream_unBounded_WordCount
 */
public class Flink03_Stream_unBounded_WordCount {
    public static void main(String[] args) throws Exception{
        //1 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将并行度设置为1
        env.setParallelism(1);

        //2 读取无界数据--kafka，从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3 将数据按照空格分割
        SingleOutputStreamOperator<String> WordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            /**
             *
             * @param value Type of the input elements.
             * @param out Type of the returned elements.
             * @throws Exception
             */
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //4 将单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> word2Tuple = WordDStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //5 将相同的单词聚合到一块 --使用keyBy的另一种写法
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = word2Tuple.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //6 对单词个数进行累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = tuple2StringKeyedStream.sum(1);

        //7. 打印到控制台
        result.print();

        //8 执行代码
        env.execute();

    }
}
