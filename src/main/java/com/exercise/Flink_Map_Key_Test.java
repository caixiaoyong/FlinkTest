package com.exercise;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author CZY
 * @date 2022/3/26 11:10
 * @description Flink_Map_Key_Test
 */
public class Flink_Map_Key_Test {
    public static void main(String args[]){
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //TODO 2.1 TransForm转换算子process
        SingleOutputStreamOperator<Tuple2<String, Integer>> processStream = streamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
//                    out.collect(new Tuple2<>(word,1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream = processStream.keyBy(0);

        keyByStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String,Integer>>() {
                HashMap<String, Integer> hashMap = new HashMap<>();
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (hashMap.containsKey(value.f0)) {
                    //如果不是第一次出现，则累加上一次结果
                    hashMap.put(value.f0,value.f1+hashMap.get(value.f0));
                }else {
                    //如果是第一次出现，则存到map中
                    hashMap.put(value.f0,value.f1);
                }
                out.collect(Tuple2.of(value.f0,hashMap.get(value.f0)));
            }
        }).print("ProcessTest");

        //TODO 3.将数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> mapStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] words = value.split(" ");
                return new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
            }
        });

        //TODO 4.按照id分组
        mapStream.keyBy(r->r.getId());
        mapStream.keyBy(WaterSensor::getId);
        mapStream.keyBy("id");
        KeyedStream<WaterSensor, String> keyedStream = mapStream.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });



    }

}
