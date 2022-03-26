package com.atguigu.day04;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 6.3	各省份页面广告点击量实时统计
 * @author CZY
 * @date 2022/1/12 18:30
 * @description Flink10_Project_Ads_Click
 */
public class Flink10_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input\\AdClickLog.csv");

        //3.将数据转化为javaBean -> tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = streamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                AdsClickLog adsClickLog = new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );
                return Tuple2.of(adsClickLog.getProvince() + "-" + adsClickLog.getAdId(), 1);
            }
        });

        //4.按照key分组，再求和
        tuple2SingleOutputStreamOperator.keyBy(0).sum(1).print();
        env.execute();
    }
}
