package com.he.rabbitmq.ps;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 发布订阅模式的消费者01
 */
public class Consumer01 {

    /**
     * 队列名称
     */
    private static final String QUEUE_NAME = "test_exchange_mail";

    /**
     * 交换机名称
     */
    private static final String EXCHANGE_NAME = "test_exchange_fanout";

    public static void main(String[] args) throws IOException, TimeoutException {

        //获取连接
        Connection connection = ConnectionUtils.getConnection();

        //使用连接创建一个通道
        final Channel channel = connection.createChannel();

        //使用通道进行交换机声明
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout", false, false, null);

        //使用通道声明队列
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        //绑定队列到交换机上
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "");

        //每次只发送一条消息到消费者
        channel.basicQos(1);

        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                System.out.println("消息【" + new String(body) + "】已被mail消费");

                //手动消息确认
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        //消息监听，消息应答方式为false
        channel.basicConsume(QUEUE_NAME, false, consumer);

        System.out.println("===消费者mail监听队列已就位===");
    }
}
