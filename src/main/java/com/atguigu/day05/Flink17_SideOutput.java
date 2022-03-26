package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 侧输出流
 * @author CZY
 * @date 2022/1/12 20:03
 * @description Flink01_TimeWindow_Tumbling
 */
public class Flink17_SideOutput {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);
        
        //2.读取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 5555);
        
        //3.将数据按照逗号切分，转为JavaBean
        KeyedStream<WaterSensor, Tuple> keyedStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2])
                );
            }
        })
                .keyBy("id");

//        OutputTag<WaterSensor> outputTag = new OutputTag< WaterSensor>("out-put") {};
        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                if (value.getVc() > 5) {
                    //TODO 将水位值高于5cm的数据放到侧输出流中，注意！必须传一个匿名内部类
                    ctx.output(new OutputTag<WaterSensor>("out-put") {
                    }, value);
                } else {
                    out.collect(value);
                }
            }
        });

        process.print("主流");
        process.getSideOutput(new OutputTag<WaterSensor>("out-put"){}).print();

        env.execute();

    }
}
