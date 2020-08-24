package com.he.rabbitmq.util;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ连接工具类
 */
public class ConnectionUtils {

    /**
     * 获取连接
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    public static Connection getConnection() throws IOException, TimeoutException {

        //创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();

        //设置服务器地址
        factory.setHost("192.168.199.128");

        //设置虚拟主机，类似msql的db
        factory.setVirtualHost("/he");

        //设置AMPQ协议的端口号
        factory.setPort(5672);

        //设置用户名
        factory.setUsername("he");

        //设置密码
        factory.setPassword("123");

        return factory.newConnection();
    }

}
