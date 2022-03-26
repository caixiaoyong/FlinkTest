package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CZY
 * @date 2022/1/11 11:32
 * @description Flink05_TransForm_Max_MaxBy
 */
public class Flink06_TransForm_reduce {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("localhost", 5555)
                //3.将数据转为JavaBean
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //4.对相同id聚合
        SingleOutputStreamOperator<WaterSensor> reduce = waterSensorDStream.keyBy(r -> r.getId())   //5.求水位和
                .reduce(new ReduceFunction<WaterSensor>() {
                    /**
                     * 每个key的第一条数据不进reduce
                     * @param value1 上一次聚合后的结果
                     * @param value2 当前数据
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("reduce.....");
                        return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
                    }
                });

        reduce.print();


        env.execute();

    }
}
