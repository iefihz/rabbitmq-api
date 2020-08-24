package com.he.rabbitmq.work.fairdispatch;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 工作队列消费者02-公平分发
 */
public class Consumer02 {

    /**
     * 队列名称
     */
    private static final String QUEUE_NAME = "test_work_queue";

    public static void main(String[] args) throws IOException, TimeoutException {

        //获取连接
        Connection connection = ConnectionUtils.getConnection();

        //使用连接创建一个通道
        final Channel channel = connection.createChannel();

        //使用通道声明队列
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        //每次只发一条消息给消费者，需要消费者手动确认
        channel.basicQos(1);

        //创建消费者，并重写handleDelivery方法
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                //消费者02模拟需要1s来处理消息
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                channel.basicAck(envelope.getDeliveryTag(), false);

                System.out.println("消息【" + new String(body) + "】已被消费者02消费");
            }
        };

        //监听队列
        channel.basicConsume(QUEUE_NAME, false, consumer);

        System.out.println("===消费者02监听队列已就位===");
    }
}
