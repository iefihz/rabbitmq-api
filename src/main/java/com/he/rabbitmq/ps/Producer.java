package com.he.rabbitmq.ps;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 发布订阅模式的生产者
 * 一个生产者，一个交换机，多个队列，多个消费者，队列绑定到交换机上，往交换机发送消息
 */
public class Producer {

    /**
     * 交换机名称
     */
    private static final String EXCHANGE_NAME = "test_exchange_fanout";

    public static void main(String[] args) throws IOException, TimeoutException {

        //获取连接
        Connection connection = ConnectionUtils.getConnection();

        //使用连接创建一个通道
        Channel channel = connection.createChannel();

        //使用通道进行交换机声明
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout", false, false, null);

        //使用通道进行消息发送，由于队列还没绑定交换机，所以此时消息会丢失
        String msg = "hello exchange";
        channel.basicPublish(EXCHANGE_NAME, "", null, msg.getBytes());
        System.out.println("消息【" + msg + "】发送成功");

        //关闭通道
        channel.close();

        //关闭连接
        connection.close();

    }
}
