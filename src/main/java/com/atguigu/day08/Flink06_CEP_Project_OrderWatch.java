package com.atguigu.day08;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 10.2	订单支付实时监控
 * @author CZY
 * @date 2022/1/19 15:35
 * @description Flink06_CEP_Project_OrderWatch
 */
public class Flink06_CEP_Project_OrderWatch {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据,并将其转为JavaBean，同时指定WaterMark
        SingleOutputStreamOperator<OrderEvent> orderEventSingleOutputStreamOperator = env.readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new OrderEvent(
                                Long.parseLong(split[0]),
                                split[1],
                                split[2],
                                Long.parseLong(split[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.getEventTime() * 1000;
                                    }
                                })
                );

        //3.将相同订单聚合到一块
        KeyedStream<OrderEvent, Long> keyedStream = orderEventSingleOutputStreamOperator.keyBy(r -> r.getOrderId());

        //TODO 统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
        //TODO 1.定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                        return "create".equals(orderEvent.getEventType());
                    }
                })
                .next("pay")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                        return "pay".equals(orderEvent.getEventType());
                    }
                })
                .within(Time.minutes(15));

        //TODO 2.将模式作用于流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 3.获取超时数据以及正常数据
        SingleOutputStreamOperator<String> result = patternStream.select(
                new OutputTag<String>("timeout") {},
                //超时的数据
                new PatternTimeoutFunction<OrderEvent, String>() {
                    /**
                     *
                     * @param map Map里对应的key 是前面的start和next字符串关键字
                     * @param l 为List集合，因为在CEP里有循环模式，一个key对应多个事件,因为有循环。作者在封装时 考虑到通用性
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                        return map.toString();
                    }
                },
                //匹配上的数据
                new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> map) throws Exception {
                        return map.toString();
                    }
                }
        );

        result.print("正常数据");
        result.getSideOutput(new OutputTag<String>("timeout"){}).print("超时数据");

        env.execute();

    }
}
