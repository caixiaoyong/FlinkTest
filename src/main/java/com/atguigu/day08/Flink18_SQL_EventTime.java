package com.atguigu.day08;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author CZY
 * @date 2022/1/20 16:45
 * @description Flink18_SQL_EventTime
 */
public class Flink18_SQL_EventTime {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 设置时区
        Configuration configuration = tableEnv.getConfig().getConfiguration();//拿tableEnv获取到Configuration
        configuration.setString("table.local-time-zone", "GMT");//然后在configuration里设置时区

        //3.创建连接到文件系统的表，并指定事件时间字段
        tableEnv.executeSql(
                "create table sensor(" +
                        "id string," +
                        "ts bigint," +
                        "vc int, " +
                        "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +//将字段t转换为timestamp(3)类型
                        "watermark for t as t - interval '5' second)" +//乱序程度5s
                        "with("
                        + "'connector' = 'filesystem',"
                        + "'path' = 'input/sensor-sql.txt',"
                        + "'format' = 'csv'"
                        + ")"

        );

        tableEnv.sqlQuery("select * from sensor").execute().print();
    }
}
