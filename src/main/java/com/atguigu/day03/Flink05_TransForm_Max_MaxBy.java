package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CZY
 * @date 2022/1/11 11:32
 * @description Flink05_TransForm_Max_MaxBy
 */
public class Flink05_TransForm_Max_MaxBy {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("localhost", 5555)
                //将数据转为JavaBean
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

//        waterSensorDStream.keyBy(r -> r.getId());
//        waterSensorDStream.keyBy(WaterSensor::getId);
//        waterSensorDStream.keyBy("id");
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        //TODO 3. 使用简单滚动聚合算子 对水位求最大值
        SingleOutputStreamOperator<WaterSensor> max = keyedStream.max("vc");

        SingleOutputStreamOperator<WaterSensor> maxBy = keyedStream.maxBy("vc", true);

        SingleOutputStreamOperator<WaterSensor> maxByFalse = keyedStream.maxBy("vc", false);

        max.print("max...");
        maxBy.print("maxBy...");
        maxByFalse.print("maxByFalse...");

        env.execute();

    }
}
