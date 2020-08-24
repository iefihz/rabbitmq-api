package com.he.rabbitmq.simple;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 简单队列消费者，消费者不需要关闭资源
 */
public class Consumer {

    /**
     * 队列名称
     */
    private static final String QUEUE_NAME = "test_simple_queue";

    public static void main(String[] args) throws IOException, TimeoutException {

        //获取连接
        Connection connection = ConnectionUtils.getConnection();

        //使用连接创建一个通道
        Channel channel = connection.createChannel();

        //使用通道声明队列
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        //创建消费者，并重写handleDelivery方法
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("消息【" + new String(body) + "】已被消费");
            }
        };

        //监听队列
        channel.basicConsume(QUEUE_NAME, true, consumer);

        System.out.println("===监听队列已就位===");
    }
}
