package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;


/**
 * @author CZY
 * @date 2022/1/19 22:30
 * @description Flink09_TableAPI_Connect_File
 */
public class Flink09_TableAPI_Connect_File {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.连接外部文件系统获取数据源
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                //.field("id", "STRING")
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(new FileSystem().path("input/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //将临时表转为Table对象
        Table table = tableEnv.from("sensor");

        //打印1.直接对TableResult类型的表进行打印
        TableResult tableResult = tableEnv.executeSql("select * from sensor");
        tableResult.print();

        //打印2.对Table类型的表先转为（.execute）TableResult类型，再对其进行打印
        Table query = tableEnv.sqlQuery("select * from sensor");
        query.execute().print();

        //打印3.1将表转为流打印
        Table table1 = table
                .groupBy($("id"))
                .select($("id"), $("vc").sum());
        //3.2将表转为流
        tableEnv.toRetractStream(table1, Row.class).print();
        env.execute();
    }
}
