package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * 去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
 * MapState
 *
 * @author CZY
 * @date 2022/1/15 0:42
 * @description Flink04_State_Keyed_Value
 */
public class Flink08_State_Keyed_MapState {
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

        //TODO 5.计算每个传感器的平均水位
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //TODO 1.获取状态
            private MapState<Integer,WaterSensor> mapState;

            //TODO 2.初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>("mapState",Integer.class,WaterSensor.class));
            }

            //注意:先存入数据，再取出。如果先取出数据再存入，会重复计算。
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.判断当前这个vc是否在状态中
                if (!mapState.contains(value.getVc())){
                    //不存在 则将数据存入状态中
                    mapState.put(value.getVc(),value);
                }
                for (Integer key : mapState.keys()) {
                    out.collect(mapState.get(key).toString());
                }
            }
        }).print();

        env.execute();
    }
}