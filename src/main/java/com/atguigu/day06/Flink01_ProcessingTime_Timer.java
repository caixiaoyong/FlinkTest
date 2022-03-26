package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 侧输出流
 * @author CZY
 * @date 2022/1/12 20:03
 * @description Flink01_TimeWindow_Tumbling
 */
public class Flink01_ProcessingTime_Timer {
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
                //对相同的id分组
                .keyBy("id");

        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //TODO 注册一个基于处理时间的定时器
                System.out.println("注册定时器"+ctx.timerService().currentProcessingTime()/1000);
//                System.out.println("注册定时器"+ctx.timestamp()+5000);
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+5000);
//                ctx.timerService().registerProcessingTimeTimer(ctx.timestamp()+5000);
                out.collect(value);
            }

            /**
             * 达到定时器时间后 触发这个方法
             * @param timestamp
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println("5s已过，定时器触发"+ctx.timerService().currentProcessingTime()/1000);
//                System.out.println("5s已过，定时器触发"+ctx.timestamp());
                out.collect(new WaterSensor("s1",5L,1));
            }
        });

        process.print("主流");


        env.execute();

    }
}
