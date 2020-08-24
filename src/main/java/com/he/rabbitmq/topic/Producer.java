package com.he.rabbitmq.topic;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 主题模式-生产者，exchange type 必须为topic
 */
public class Producer {

    /**
     * 交换机名称
     */
    public static final String EXCHANGE_NAME = "test_topic_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {

        //获取连接
        Connection connection = ConnectionUtils.getConnection();

        //创建通道
        Channel channel = connection.createChannel();

        //声明一个类型为direct的交换机
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        //每次只给消费者分发一条消息
        channel.basicQos(1);

        //生产消息
        for (int i = 1; i <= 10; i ++) {
            String routingKey = i%2 == 0 ? "goods.delete" : "goods.add";
            String msg = "hello topic!" + routingKey + i;
            channel.basicPublish(EXCHANGE_NAME, routingKey, null, msg.getBytes());
            System.out.println("消息【" + msg + "】发送成功");
        }

        //关闭通道
        channel.close();

        //关闭连接
        connection.close();
    }
}
