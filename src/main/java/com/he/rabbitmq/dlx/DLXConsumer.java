package com.he.rabbitmq.dlx;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 死信队列-消费者
 */
public class DLXConsumer {

    /**
     * 普通交换机名称，不是死信队列
     */
    public static final String EXCHANGE_NAME = "test_dlx_exchange";

    /**
     * 普通队列，不是死信队列的queue
     */
    public static final String QUEUE_NAME = "test_dlx_queue";

    /**
     * 死信队列
     */
    public static final String EXCHANGE_DLX_NAME = "dlx.exchange";

    /**
     * 死信队列的queue
     */
    public static final String QUEUE_DLX_NAME = "dlx.queue";

    public static void main(String[] args) throws IOException, TimeoutException {

        Connection connection = ConnectionUtils.getConnection();

        final Channel channel = connection.createChannel();

        //普通队列、交换机的声明和绑定
        channel.exchangeDeclare(EXCHANGE_NAME, "topic", true, false, null);

        /**
         * x-dead-letter-exchange属性为死信队列（本质是一个交换机）的名称，并在普通队列声明时添加进去
         */
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("x-dead-letter-exchange", EXCHANGE_DLX_NAME);
        channel.queueDeclare(QUEUE_NAME, true, false, false, properties);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "dlx.#");

        //死信队列、交换机的声明和绑定
        channel.exchangeDeclare(EXCHANGE_DLX_NAME, "topic", true, false, null);
        channel.queueDeclare(QUEUE_DLX_NAME, true, false, false, null);
        //死信队列的路由键必须为#
        channel.queueBind(QUEUE_DLX_NAME, EXCHANGE_DLX_NAME, "#");

        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                System.out.println("[DLXConsumer]: " + new String(body));

                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(QUEUE_NAME, false, consumer);

    }
}
