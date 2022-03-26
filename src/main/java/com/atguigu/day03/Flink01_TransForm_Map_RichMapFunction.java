package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CZY
 * @date 2022/1/10 22:32
 * @description Flink01_TransForm_Map_RichMapFunction
 */
public class Flink01_TransForm_Map_RichMapFunction {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        /*SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {

            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });*/
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MyRichMap());
        map.print();
        env.execute();
    }
    public static class MyRichMap extends RichMapFunction<String,WaterSensor>{

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open ...");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close ...");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
            System.out.println("-------------------------");
            System.out.println(getRuntimeContext().getTaskName());
            String[] split = value.split(",");
            System.out.println("-------------------------");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }
    }
}
