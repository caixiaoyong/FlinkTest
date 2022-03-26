package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author CZY
 * @date 2022/1/21 16:27
 * @description Flink08_HiveCatalog
 */
public class Flink08_HiveCatalog {
    public static void main(String[] args) {
        //设置用户权限
        System.setProperty("HADOOP_USER_NAME", "cy");

        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.创建HiveCatalog
        String name            = "myhive";  // Catalog 名字
        String defaultDatabase = "flink_test"; // 默认数据库
        String hiveConfDir     = "c:/conf"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录

        //1.创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        //2.注册HiveCatalog
        tableEnv.registerCatalog(name,hiveCatalog);

        //3.指定使用哪个HiveCatalog、数据库
        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);

        tableEnv.executeSql("select * from stu").print();


    }
}
