package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 基于事件时间的滚动窗口 窗口大小5s 乱序程度3s 允许迟到2s 侧输出流
 * s1,1,1 开启一个[0,5)的窗口
 * s1,8,1 触发计算
 * s1,2,1 迟到数据，在关窗范围内，写入主流
 * s1,10,1 窗口关闭
 * s1,2,1 迟到数据，在关窗范围外，写入侧输出流
 *
 * @author CZY
 * @date 2022/1/14 20:12
 * @description Flink16_EventTime_WaterMark_ForBounded_TimeWindow_Tumbling_AllowedLateness_SideOutPut
 */
public class Flink16_EventTime_WaterMark_ForBounded_TimeWindow_Tumbling_AllowedLateness_SideOutPut {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 5555);

        //3.将数据转换为JavaBean
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

        //TODO 4.设置WaterMark 乱序程度（固定延迟）3s
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = map.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }));

        //5.将相同id聚合到一块
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(r -> r.getId());

        //TODO 6.开启一个基于事件时间的滚动窗口 窗口大小5s
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //允许迟到的数据（2）延长窗口关闭时间
                .allowedLateness(Time.seconds(2))
                //将关窗迟到的数据放入侧输出流汇总 注意！必须传一个匿名实现类
                .sideOutputLateData(new OutputTag<WaterSensor>("output") {
                });

        SingleOutputStreamOperator<String> result = windowedStream.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前key: " + s
                        + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                        + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        });

        result.print("主流");
        DataStream<WaterSensor> sideOutput = result.getSideOutput(new OutputTag<WaterSensor>("output") {
        });
        sideOutput.print("迟到的数据");

        env.execute();


    }
}
