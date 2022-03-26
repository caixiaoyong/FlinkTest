package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author CZY
 * @date 2022/1/21 23:36
 * @description Flink05_Fun_UDAF
 */
public class Flink05_Fun_UDAF {
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

        //TODO 不注册直接使用
/*        table
                .groupBy($("id"))
                .select($("id"),call(MyUDAF.class,$("vc")))
                .execute().print();*/

        //先注册再使用
        tableEnv.createTemporarySystemFunction("MyUDAF",MyUDAF.class);
/*        table
                .groupBy($("id"))
                .select($("id"),call("MyUDAF",$("vc")))
                .execute().print();*/

        //在sql中使用
        tableEnv.executeSql("select id,MyUDAF(vc) from " + table+" group by id").print();

        //在sql中使用
        tableEnv.executeSql("select id,word from "+table+",lateral table(MyUDTF(id))").print();

    }
    //自定一个类用来实现UDAF（多进一出），根据vc求平均值
    //定义一个累加器
    public static class MyAcc{
        public Integer sum=0;
        public Integer count=0;
    }
    public static class MyUDAF extends AggregateFunction<Double,MyAcc>{

        /**
         * 初始化累加器
         * @return
         */
        @Override
        public MyAcc createAccumulator() {
            return new MyAcc();
        }

        /**
         * 自定义方法来更新累加器
         * 累加操作
         * @param acc
         * @param value
         */
        public void accumulate(MyAcc acc,Integer value){
            acc.sum+=value;
            acc.count+=1;
        }

        /**
         * 获取最终的结果
         * @param myAcc
         * @return
         */
        @Override
        public Double getValue(MyAcc myAcc) {
            return myAcc.sum*1D/myAcc.count;
        }



    }
}
