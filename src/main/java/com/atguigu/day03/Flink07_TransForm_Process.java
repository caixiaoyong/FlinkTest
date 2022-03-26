package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author CZY
 * @date 2022/1/11 11:32
 * @description Flink05_TransForm_Max_MaxBy
 */
public class Flink07_TransForm_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> word2OneDStream = env.socketTextStream("localhost", 5555)
                //3.使用process可以实现flatMap效果，将数据按空格切分组成Tuple2元组
                .process(new ProcessFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                });
        //4.按照key分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = word2OneDStream.keyBy(0);

        //5.利用process实现sum效果
        keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String,Integer>>() {
               private HashMap<String, Integer> words = new HashMap<>();
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (words.containsKey(value.f0)){
                    //如果不是第一次出现，则累加到上一次的结果中
                    words.put(value.f0,value.f1+words.get(value.f0));
                }else {
                    //如果是第一次来，则存到map中
                    words.put(value.f0,value.f1);
                }
                out.collect(Tuple2.of(value.f0,words.get(value.f0)));
            }
        }).print();

        env.execute();

    }
}
