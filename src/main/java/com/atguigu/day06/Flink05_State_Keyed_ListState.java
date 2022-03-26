package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
 * 针对每个传感器输出最高的3个水位值
 * ListState
 *
 * @author CZY
 * @date 2022/1/15 0:42
 * @description Flink04_State_Keyed_Value
 */
public class Flink05_State_Keyed_ListState {
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

        //TODO 5.针对每个传感器输出最高的3个水位值
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //TODO 1.获取状态中所有水位高度
            private ListState<Integer> listState;

            //TODO 2.初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("ListState", Types.INT));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.将水位值存入列表状态
                listState.add(value.getVc());

                //2.定义一个集合存放元素
                ArrayList<Integer> arrayList = new ArrayList<>();

                //3.获取列表里的值存入集合
                for (Integer integer : listState.get()) {
                    arrayList.add(integer);
                }

                //4.降序排序
                arrayList.sort((o1, o2) -> o2 - o1);

                //5.超过3的移除
                if (arrayList.size() > 3){
                    arrayList.remove(3);
                }
                listState.update(arrayList);
                out.collect(arrayList.toString());
            }
        })
                .print();

        env.execute();

    }
}
