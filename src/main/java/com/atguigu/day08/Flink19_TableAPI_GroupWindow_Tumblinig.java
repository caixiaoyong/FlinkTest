package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author CZY
 * @date 2022/1/20 22:12
 * @description Flink19_TableAPI_GroupWindow_Tumblinig
 */
public class Flink19_TableAPI_GroupWindow_Tumblinig {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取流
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为表并指定事件时间
        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));

/*        //4.开启一个滚动窗口
        Table tableResult = table
                //定义滚动窗口，窗口大小3s，指定时间字段ts，必须要有窗口别名设为w
                .window(Tumble.over(lit(3).second()).on($("ts")).as("w"))
                //窗口必须出现在分组字段中
                .groupBy($("id"), $("w"))
                //开窗、关窗时间
                .select($("id"), $("vc").sum(), $("w").start(), $("w").end());*/

/*        //4.开启一个滑动窗口
        Table tableResult = table
                .window(Slide.over(lit(3).second()).every(lit(2).second()).on($("ts")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("vc").sum(), $("w").start(), $("w").end());*/

        //4.开启一个会话窗口
        Table tableResult = table
                .window(Session.withGap(lit(2).second()).on($("ts")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("vc").sum(), $("w").start(), $("w").end());

        tableResult.execute().print();
    }
}
