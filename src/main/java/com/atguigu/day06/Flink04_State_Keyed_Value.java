package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警
 * ValueState
 *
 * @author CZY
 * @date 2022/1/15 0:42
 * @description Flink04_State_Keyed_Value
 */
public class Flink04_State_Keyed_Value {
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

        //TODO 5.检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //TODO 1.定义一个状态来保存上一次水位
            private ValueState<Integer> valueState;

            //TODO 2.初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                //因为后面可能维护多个状态，另外可能快照存入到磁盘里面，待下次恢复可以区别
                // 所以这里设置状态名且唯一；类型为往状态存入数据的类型
                valueState=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state",Integer.class));
                //valueState=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state",Types.INT));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.拿当前的水位和上一次水位对比，如果大于10就报警
                int lastVc = valueState.value() == null ? value.getVc():valueState.value();
                if (Math.abs(value.getVc()-lastVc )>10){
                    out.collect(value.getVc()+"水位线差值大于10！！！");
                }
                //2.将当前水位保存到状态中
                valueState.update(value.getVc());
            }
        })
                .print();

        env.execute();

    }
}
