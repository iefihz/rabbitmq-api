package com.he.rabbitmq.work.fairdispatch;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 工作队列生产者-公平分发
 */
public class Producer {

    /**
     * 队列名称
     */
    private static final String QUEUE_NAME = "test_work_queue";

    public static void main(String[] args) throws IOException, TimeoutException {

        //获取连接
        Connection connection = ConnectionUtils.getConnection();

        //使用连接创建一个通道
        Channel channel = connection.createChannel();

        //使用通道进行队列声明
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        //每次只发一条消息给消费者，需要消费者手动确认
        channel.basicQos(1);

        //使用通道进行消息发送
        for (int i = 1; i <= 50; i ++) {
            String msg = "hello work queue--" + i;
            channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());

            try {
                Thread.sleep(i*5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                System.out.println("消息【" + msg + "】发送成功");
            }
        }

        //关闭通道
        channel.close();

        //关闭连接
        connection.close();
    }
}
