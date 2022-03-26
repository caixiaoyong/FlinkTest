package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author CZY
 * @date 2022/1/10 19:09
 * @description Flink01_Source_Collection
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //TODO 2.从集合中获取数据（不能设置多并行度）
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
//        DataStreamSource<Integer> streamSource = env.fromCollection(list);

        //TODO 从文件读取数据
//        DataStreamSource<String> streamSource = env.readTextFile("input\\words.txt");

        //TODO 从端口读取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 从元素中读取数据
//        DataStreamSource<String> streamSource = env.fromElements("a", "b", "c", "d", "e");

        //TODO 从hdfs上读取数据
        DataStreamSource<String> streamSource = env.readTextFile("hdfs://hadoop102:8020/input/1.txt");

        streamSource.print();
        env.execute();
    }
}
