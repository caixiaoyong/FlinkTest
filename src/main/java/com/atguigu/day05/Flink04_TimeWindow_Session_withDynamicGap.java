package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author CZY
 * @date 2022/1/12 20:03
 * @description Flink01_TimeWindow_Tumbling
 */
public class Flink04_TimeWindow_Session_withDynamicGap {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);
        
        //2.读取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 5555);
        
        //3.将数据按照逗号切分，转为JavaBean
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]) * 1000,
                        Integer.parseInt(split[2])
                );
            }
        })
                .keyBy("id")
                //TODO 开启一个基于时间的会话窗口，使用动态会话
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
                    /**
                     * 提取会话时间的间隔
                     * @param element
                     * @return
                     */
                    @Override
                    public long extract(WaterSensor element) {
                        return element.getTs();
                    }
                }));

        window.process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
                           @Override
                           public void process(Tuple tuple, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                               String msg =
                                       "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                               + elements.spliterator().estimateSize() + "条数据 ";
                               out.collect(msg);
                           }
                       }
        ).print();
        env.execute();

    }
}
