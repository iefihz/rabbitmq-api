package com.he.rabbitmq.confirm;

import com.he.rabbitmq.util.ConnectionUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

/**
 * confirm机制（异步）-生产者03
 */
public class CfProducer03 {

    public static final String QUEUE_NAME = "test_cf_queue";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

        Connection connection = ConnectionUtils.getConnection();

        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        //开启confirm模式
        channel.confirmSelect();

        final SortedSet<Long> confirmSet = Collections.synchronizedSortedSet(new TreeSet<Long>());

        //通道添加消息确认监听
        channel.addConfirmListener(new ConfirmListener() {

            //消息发送成功的回调（这里只做简单的移除SortedSet里面的发送成功的消息的id）
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {

                if (multiple) {
                    //多条的情况
                    System.out.println("=======handleAck-multiple======");
                    confirmSet.headSet(deliveryTag +1).clear();
                } else {
                    //单条的情况
                    System.out.println("=======handleAck======not-multiple");
                    confirmSet.remove(deliveryTag);
                }
            }

            //消息发送失败的回调（这里只做简单的移除SortedSet里面的发送失败的消息的id，可以自行实现1s、3s、10s重试之类的）
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                if (multiple) {
                    System.out.println("=======handleNack-multiple======");
                    confirmSet.headSet(deliveryTag + 1).clear();
                } else {
                    System.out.println("=======handleNack======not-multiple");
                    confirmSet.remove(deliveryTag);
                }
            }
        });

        String msg = "hello cf single";

        for (;;) {

            //获取下一个消息的序列号
            long nextPublishSeqNo = channel.getNextPublishSeqNo();

            //消息发送
            channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());

            //把这个消息的序列号存到SortedSet中
            confirmSet.add(nextPublishSeqNo);

        }

    }
}
