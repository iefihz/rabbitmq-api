package com.he.rabbitmq.tx;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消息确认机制之事务机制-生产者
 */
public class TxProducer {

    public static final String QUEUE_NAME = "test_tx_queue";

    public static void main(String[] args) throws IOException, TimeoutException {

        Connection connection = ConnectionUtils.getConnection();

        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        String msg = "hello tx";

        //开启事务
        channel.txSelect();
        try {
            channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());

            int i = 1/0;

            //提交事务
            channel.txCommit();

            System.out.println("消息【" + msg + "】发送成功");
        } catch (Exception e) {

            //回滚事务
            channel.txRollback();
            System.out.println("消息【" + msg + "】发送失败，回滚！！！");
        }

        //关闭通道
        channel.close();

        //关闭连接
        connection.close();
    }
}
