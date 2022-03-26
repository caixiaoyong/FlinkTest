package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 4.1.2 在创建表的 DDL 中定义
 * @author CZY
 * @date 2022/1/20 16:35
 * @description Flink17_SQL_ProcessingTime
 */
public class Flink17_SQL_ProcessingTime {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.创建连接到文件系统的表，并指定处理时间字段
        tableEnv.executeSql("create table sensor(id string,ts bigint,vc int,pt_time as PROCTIME()) with("//as不是指别名 而是将其指定为处理时间字段
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")"
        );

        tableEnv.executeSql("select * from sensor").print();


    }
}
