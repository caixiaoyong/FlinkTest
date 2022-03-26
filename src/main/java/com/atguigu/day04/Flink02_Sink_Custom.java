package com.atguigu.day04;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @author CZY
 * @date 2022/1/11 19:45
 * @description Flink10_Sink_Redis
 */
public class Flink02_Sink_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 5555);

        //3.将数据转换成JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

                return waterSensor;
            }
        });

        //TODO 自定义Sink
        map.addSink(new MySink());
        env.execute();
    }

    //public static class MySink implements SinkFunction<WaterSensor>{
    //使用富函数优化连接
    public static class MySink extends RichSinkFunction<WaterSensor>{

        private Connection connect;
        private PreparedStatement pstm;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("初始化方法。。。");
            //获取连接
            connect = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");

            //获取语句 预执行者
            pstm = connect.prepareStatement("insert into sensor values(?,?,?)");
        }

        /**
         * 每来一条数据都调用一次
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            //给占位符赋值
            pstm.setString(1,value.getId());
            pstm.setLong(2,value.getTs());
            pstm.setInt(3,value.getVc());

            pstm.execute();
        }

        @Override
        public void close() throws Exception {
            System.out.println("关闭方法。。。");
            pstm.close();
            connect.close();
        }
    }
}
