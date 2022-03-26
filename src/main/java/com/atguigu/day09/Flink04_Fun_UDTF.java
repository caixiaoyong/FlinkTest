package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author CZY
 * @date 2022/1/21 22:13
 * @description Flink04_Fun_UDTF
 */
public class Flink04_Fun_UDTF {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从端口获取数据，并将数据转为WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为表
        Table table = tableEnv.fromDataStream(waterSensorDStream);

     /*   //TODO 不注册直接使用
        table
                .joinLateral(call(MyUDTF.class,$("id")))
                .select($("id"),$("word"))//word为侧写表的字段名
                .execute().print();*/

        //先注册再使用
        tableEnv.createTemporarySystemFunction("MyUDTF",MyUDTF.class);
        /*table
                .joinLateral(call("MyUDTF",$("id")))
                .select($("id"),$("word"))//word为侧写表的字段名
                .execute().print();*/

        //在sql中使用
        tableEnv.executeSql("select id,word from "+table+",lateral table(MyUDTF(id))").print();
    }
    //自定义一个类用来实现UDTF（一进多出）,根据id按照下划线切分，获取到字母以及数字
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))//使用暗示，输出侧写表字段名word
    public static class MyUDTF extends TableFunction<Row>{//按行row输出
        public void eval(String id){
            String[] split = id.split("_");
            for (String s : split) {
                collect(Row.of(s));
            }
        }
    }
}
