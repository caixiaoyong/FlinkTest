package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author CZY
 * @date 2022/1/21 19:40
 * @description Flink01_SQL_GroupWindow_Tumble
 */
public class Flink02_SQL_GroupWindow_Tumble {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")");

/*        //TODO 3.开启一个滚动窗口
        tableEnv.executeSql("select id," +
                "sum(vc) vcSum," +
                "TUMBLE_START(t,INTERVAL '3' second) as wStart," +
                "TUMBLE_END(t,INTERVAL '3' second) as wEnd" +
                " from sensor" +
                " group by id,TUMBLE(t,INTERVAL '3' second)").print();*/

/*        //TODO 3.开启一个滑动窗口,窗口大小为3s，滑动步长为2s
        tableEnv.executeSql("select id," +
                "sum(vc) vcSum," +
                //第一个INTERVAL为滑动步长，第二个INTERVAL为窗口大小
                "HOP_START(t,INTERVAL '2' second,INTERVAL '3' second) as wStart," +
                "HOP_END(t,INTERVAL '2' second,INTERVAL '3' second) as wEnd" +
                " from sensor" +
                " group by id,HOP(t,INTERVAL '2' second,INTERVAL '3' second)").print();*/

        //TODO 3.开启一个会话窗口,会话间隔2s
        tableEnv.executeSql("select id," +
                "sum(vc) vcSum," +
                //第一个INTERVAL为滑动步长，第二个INTERVAL为窗口大小
                "session_START(t,INTERVAL '2' second) as wStart," +
                "session_END(t,INTERVAL '2' second) as wEnd" +
                " from sensor" +
                " group by id,session(t,INTERVAL '2' second)").print();

    }
}
