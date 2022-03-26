package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;


/**
 * 计算每个传感器的水位和
 * ReducingState
 *
 * @author CZY
 * @date 2022/1/15 0:42
 * @description Flink04_State_Keyed_Value
 */
public class Flink06_State_Keyed_ReducingState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 5555);

        //3.将数据转换成JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2])
                );
            }
        });

        //4.按照id进行分组
        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        //TODO 5.计算每个传感器的水位和
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //TODO 1.获取状态
            private ReducingState<Integer> reducingState;

            //TODO 2.初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("reducing-state", new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1+value2;
                    }
                },Types.INT));
            }

            //注意:先存入数据，再取出。如果先取出数据再存入，会重复计算。
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.先将当前数据存入状态中
                reducingState.add(value.getVc());

                //2.再取出状态中的数据
                Integer integer = reducingState.get();

                out.collect(integer.toString());

            }
        }).print();

                env.execute();
    }
}