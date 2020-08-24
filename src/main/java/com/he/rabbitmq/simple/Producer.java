package com.he.rabbitmq.simple;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 简单队列生产者
 */
public class Producer {

    /**
     * 队列名称
     */
    private static final String QUEUE_NAME = "test_simple_queue";

    public static void main(String[] args) throws IOException, TimeoutException {

        //获取连接
        Connection connection = ConnectionUtils.getConnection();

        //使用连接创建一个通道
        Channel channel = connection.createChannel();

        //使用通道进行队列声明
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        //使用通道进行消息发送
        String msg = "hello simple queue";
        channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());
        System.out.println("消息【" + msg + "】发送成功");

        //关闭通道
        channel.close();

        //关闭连接
        connection.close();

    }
}
