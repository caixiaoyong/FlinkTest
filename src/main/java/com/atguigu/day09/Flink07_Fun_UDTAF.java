package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
/**
 * @author CZY
 * @date 2022/1/22 0:12
 * @description Flink07_Fun_UDTAF
 */
public class Flink07_Fun_UDTAF {
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

        //不注册直接使用
       /* table
                .groupBy($("id"))
                .flatAggregate(call(MyUDTAF.class, $("vc")).as("value", "rank"))
                .select($("id"),$("value"),$("rank"))
                .execute().print();
*/
        //先注册再使用
        tableEnv.createTemporarySystemFunction("MyUDTAF", MyUDTAF.class);

        table
                .groupBy($("id"))
                .flatAggregate(call("MyUDTAF", $("vc")).as("value", "rank"))
                .select($("id"),$("value"),$("rank"))
                .execute().print();
    }
    //自定义一个类用来实现UDTAF（多进多出）,根据vc求最大的两个值
    //定义一个累加器
    public static class MyTopAcc{
        public Integer first;
        public Integer second;
    }
    public static class MyUDTAF extends TableAggregateFunction<Tuple2<Integer,Integer>,MyTopAcc>{

        /**
         * 初始化累加器
         * @return
         */
        @Override
        public MyTopAcc createAccumulator() {
            MyTopAcc myTopAcc = new MyTopAcc();
            myTopAcc.first = Integer.MIN_VALUE;
            myTopAcc.second = Integer.MIN_VALUE;
            return myTopAcc;
        }

        /**
         * 对每个输入行 调用函数的accumulate（）方法来 更新累加器
         * @param accumulate
         * @param value
         */
        public void accumulate(MyTopAcc accumulate,Integer value){
            //判断当前水位是否排第一
            if (value>accumulate.first){
                accumulate.second = accumulate.first;
                accumulate.first = value;
            }else if (value>accumulate.second){
                accumulate.second = value;
            }
        }

        /**
         * 计算并返回最终结果
         * @param acc
         * @param out
         */
        public void emitValue(MyTopAcc acc, Collector<Tuple2<Integer,Integer>> out){
            if (acc.first!=Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.first,1));
            }

            if (acc.second!=Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.second,2));
            }
        }
    }
}
