package com.he.rabbitmq.routing;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;

/**
 * 路由模式-消费者01，exchange type 必须为direct
 */
public class Consumer01 {

    /**
     * 交换机名称
     */
    public static final String EXCHANGE_NAME = "test_routing_exchange";

    /**
     * 队列名称
     */
    public static final String QUEUE_NAME = "test_routing_queue01";

    public static void main(String[] args) throws IOException, TimeoutException {

        //获取连接
        Connection connection = ConnectionUtils.getConnection();

        //创建通道
        final Channel channel = connection.createChannel();

        //声明一个类型为direct的交换机
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");

        //声明队列
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

        //队列绑定，以routingKey进行消息区分
        CopyOnWriteArrayList<String> routingKeys = new CopyOnWriteArrayList<String>();
        routingKeys.add("error");
        for (String routingKey : routingKeys) {
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, routingKey);
        }

        //每次只给消费者分发一条消息
        channel.basicQos(1);

        //创建消费者，并重写消费者的handleDelivery方法
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                System.out.println("Routing_Consumer01: " + new String(body));

                //消息应答
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        //消息监听
        channel.basicConsume(QUEUE_NAME, false, consumer);

        System.out.println("===Routing_Consumer01监听队列已就位===");
    }
}
