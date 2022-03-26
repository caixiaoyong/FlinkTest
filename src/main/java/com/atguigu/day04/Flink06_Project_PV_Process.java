package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 6.1.1	网站总浏览量（PV）的统计
 * @author CZY
 * @date 2022/1/12 11:35
 * @description Flink06_Project_PV_Process
 */
public class Flink06_Project_PV_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input\\UserBehavior.csv");

        streamSource.process(new ProcessFunction<String, Tuple2<String,Integer>>() {
            //定义一个累加器
            private Integer count=0;
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String,Integer>> out) throws Exception {
                //1.将数据转换为java Bean
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
                //2.过滤出pv数据
                if("pv".equals(userBehavior.getBehavior())){
                    count++;
                    out.collect(Tuple2.of(userBehavior.getBehavior(),count));
                }
            }
        }).print();

        env.execute();
    }
}
