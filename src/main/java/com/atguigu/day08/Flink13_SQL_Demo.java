package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author CZY
 * @date 2022/1/20 13:54
 * @description Flink13_SQL_Demo
 */
public class Flink13_SQL_Demo {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //获取流
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为动态表（未注册的表）
        Table table = tableEnv.fromDataStream(waterSensorStream);

//        TableResult tableResult = tableEnv.executeSql("select * from " + table + " where id='sensor_1'");

        //TODO 方式一： 把表注册为一个临时视图,并查询
        tableEnv.createTemporaryView("sensor",table);

        //TODO 方式二： 直接从流转为临时视图,并查询
//        tableEnv.createTemporaryView("sensor",waterSensorStream);
        TableResult tableResult = tableEnv.executeSql("select * from sensor where id='sensor_1'");


        tableResult.print();

    }
}
