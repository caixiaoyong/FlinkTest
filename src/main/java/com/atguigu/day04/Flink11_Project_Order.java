package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author CZY
 * @date 2022/1/12 18:41
 * @description Flink11_Project_Order
 */
public class Flink11_Project_Order {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> OrderSource = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> receiptSource = env.readTextFile("input/ReceiptLog.csv");

        //3.分别将两条流转换成javaBean
        SingleOutputStreamOperator<OrderEvent> OrdermapDStream = OrderSource.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        SingleOutputStreamOperator<TxEvent> TxEventmapDStream = receiptSource.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2])
                );
            }
        });

        //4.使用connect连接两条流
        ConnectedStreams<OrderEvent, TxEvent> connect = OrdermapDStream.connect(TxEventmapDStream);

        //5.将相同交易码的数据聚合到一块
        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = connect.keyBy("txId", "txId");

        //6.实时对账两条流的数据
        connectedStreams.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
                HashMap<String, OrderEvent> orderHashMap = new HashMap<>();
                HashMap<String, TxEvent> TxEventHashMap = new HashMap<>();
            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                //先查看交易表的Map集合中能否关联上 交易码
                if (TxEventHashMap.containsKey(value.getTxId())){
                    //关联上则输出 并销毁 避免后续再次关联
                    out.collect("订单:"+value.getOrderId()+" 对账成功。");
                    TxEventHashMap.remove(value.getTxId());
                }else {
                    //没有关联上存入订单表的Map集合
                    orderHashMap.put(value.getTxId(),value);
                }
            }
            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                //先查看订单表的Map集合中能否关联上 交易码
                if (orderHashMap.containsKey(value.getTxId())){
                    //关联上则输出 并销毁 避免后续再次关联
                    out.collect("订单:"+orderHashMap.get(value.getTxId()).getOrderId()+" 对账成功。");
                    orderHashMap.remove(value.getTxId());
                }else {
                    //没有关联上存入订单表的Map集合
                    TxEventHashMap.put(value.getTxId(),value);
                }
            }
        }).print();

        env.execute();


    }
}
