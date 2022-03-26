package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * 流处理 WordCount 有界流
 * @author CZY
 * @date 2021/12/27 14:59
 * @description Flink02_Stream_Bounded_WordCount
 */
public class Flink02_Stream_Bounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1. 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将并行度设置为1
        env.setParallelism(1);

        //2. 读取文件 --有界数据
        DataStreamSource<String> lineDss = env.readTextFile("input/words.txt");

        //3. 将数据按照空格打散
        SingleOutputStreamOperator<String> wordDStream = lineDss.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //按空格切分
                String[] words = value.split(" ");
                // 遍历出数组中每一个单词
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        wordDStream.print();
        env.execute();
        System.out.println("=================1");
        //4.将打散出的单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> word2OneDStream = wordDStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2 map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });
        //Lambda表达式
//        SingleOutputStreamOperator<Tuple2<String, Integer>> word2OneDStream = wordDStream.map(value -> Tuple2.of(value, 1)).returns(Types.TUPLE(Types.STRING,Types.LONG));

        word2OneDStream.print();
        env.execute();
        System.out.println("=================2");
        //5. 将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = word2OneDStream.keyBy(0);

        //6. 做累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //7. 打印到控制台
        result.print();

        //8. 执行代码
        env.execute();
    }
}
