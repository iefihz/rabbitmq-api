package com.he.rabbitmq.confirm;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * confirm机制（单条发送后确认）-生产者01
 */
public class CfProducer01 {

    public static final String QUEUE_NAME = "test_cf_queue";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

        Connection connection = ConnectionUtils.getConnection();

        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        String msg = "hello cf single";

        //开启confirm模式
        channel.confirmSelect();

        //发送一条消息
        channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());

        //等待确认
        if (channel.waitForConfirms()) {
            System.out.println("消息【" + msg + "】发送成功");
        } else {
            System.out.println("消息【" + msg + "】发送失败");
        }

        //关闭通道
        channel.close();

        //关闭连接
        connection.close();
    }
}
