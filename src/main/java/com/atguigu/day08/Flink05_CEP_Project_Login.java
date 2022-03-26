package com.atguigu.day08;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 10.1	恶意登录监控
 * 用户2秒内连续两次及以上登录失败则判定为恶意登录。
 * @author CZY
 * @date 2022/1/19 15:03
 * @description Flink05_CEP_Project_Login
 */
public class Flink05_CEP_Project_Login {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取文件 ,并转为JavaBean，同时指定WaterMark
        SingleOutputStreamOperator<LoginEvent> loginEventSingleOutputStreamOperator = env.readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new LoginEvent(
                                Long.parseLong(split[0]),
                                split[1],
                                split[2],
                                Long.parseLong(split[3])*1000//按照毫秒进行处理的，10位数所以*1000
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                        return element.getEventTime();
                                    }
                                })
                );

        //3.将相同用户的数据聚和到一块
        KeyedStream<LoginEvent, Long> keyedStream = loginEventSingleOutputStreamOperator.keyBy(new KeySelector<LoginEvent, Long>() {
            @Override
            public Long getKey(LoginEvent value) throws Exception {
                return value.getUserId();
            }
        });
        //也可以使用
//        loginEventSingleOutputStreamOperator.keyBy("userId");


        //TODO 用户2秒内连续两次及以上登录失败则判定为恶意登录。
        //1.定义模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("start")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        return "fail".equals(loginEvent.getEventType());
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .within(Time.seconds(2));

        //2.将模式作用于流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventSingleOutputStreamOperator, pattern);

        //3.获取匹配的数据
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                return map.toString();
            }
        }).print();

        env.execute();

    }
}
