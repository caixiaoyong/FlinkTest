package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * 批处理 wordcount
 * @author CZY
 * @date 2021/12/27 10:41
 * @description WordCount
 */
public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        //1. 创建批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2. 从文件读取数据 按行读取
        DataSource<String> lineDS = env.readTextFile("D:\\BigData\\FlinkTest\\input\\words.txt");

        //3. 转换数据格式
        AggregateOperator<Tuple2<String, Integer>> resultSet = lineDS.flatMap(new MyflatMapper())
                .groupBy(0) //按照第一个位置的word分组
                .sum(1);//按照第二个位置的数据求和 底层调用的是reduce
        resultSet.print();

/*        lineDS.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word,1));
                }
            }
        })*/

    }

    private static class MyflatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
        /**
         *
         * @param value 输入的数据
         * @param out 采集器，将数据采集起来发送至下游
         * @throws Exception
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按空格切分
            String[] words = value.split(" ");

            // 遍历所有word 转换成二元组输出
            for (String word : words) {
//                out.collect(new Tuple2<>(word,1));
                out.collect(Tuple2.of(word,1));//底层调用就是上面
            }
        }
    }
}
