package com.he.rabbitmq.dlx;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 死信队列-生产者，例如：消息过期后变成死信
 */
public class DLXProducer {

    /**
     * 普通交换机名称，不是死信队列
     */
    public static final String EXCHANGE_NAME = "test_dlx_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {

        Connection connection = ConnectionUtils.getConnection();

        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic", true, false, null);

        channel.confirmSelect();

        channel.addConfirmListener(new ConfirmListener() {
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("========消息ack========");
            }

            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("========消息Nack========");
            }
        });

        String msg = "Hello dlx";

        AMQP.BasicProperties properties = new AMQP.BasicProperties()
                .builder()
                .contentEncoding("utf-8")
                .deliveryMode(2)          //1.不是持久化消息  2.持久化消息
                .expiration("10000")        //设置10s过期，也可以声明队列时，设置队列内消息的过期时间
                .build();
        channel.basicPublish(EXCHANGE_NAME, "dlx.add", properties, msg.getBytes());
        System.out.println("[DLXProducer]: " + msg);

        channel.close();

        connection.close();
    }
}
