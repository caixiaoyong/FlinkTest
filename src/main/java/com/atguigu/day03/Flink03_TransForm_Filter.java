package com.atguigu.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CZY
 * @date 2022/1/10 23:43
 * @description Flink03_TransForm_KeyBy
 */
public class Flink03_TransForm_Filter {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 保留偶数，舍弃奇数
        env
                .fromElements(10,3,4,5,6,7,8,9)
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value % 2==0;
                    }
                })
                .print();
        env.execute();
    }
}
