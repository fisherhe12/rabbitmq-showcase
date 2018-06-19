package com.github.fisherhe12.common;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

/**
 * MQ公共工具类
 *
 * @author fisher
 * @date 2017-05-09
 */
public class RabbitMQUtil {

    private static final String USER_NAME = "fisher";
    private static final String PASSWORD = "fisher";
    private static final String HOST = "172.18.8.20";

    /**
     * 获取连接
     */
    public static Connection getConnection() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(HOST);
        connectionFactory.setUsername(USER_NAME);
        connectionFactory.setPassword(PASSWORD);

        //默认AMQP连接端口5672
        //connectionFactory.setPort(5672);
        // 设置其他的VHost
        // connectionFactory.setVirtualHost("/");

        try {
            return connectionFactory.newConnection();
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("Could not get connection!", e);
        }
    }


    public static Consumer buildConsumer(Channel channel, boolean autoAck, long timeMillis) throws
            UnsupportedEncodingException {
        return new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {

                String message = new String(body, "UTF-8");
                System.out.println("消费者接受消息:" + message);

                try {
                    Thread.sleep(timeMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    if (!autoAck) {
                        channel.basicAck(envelope.getDeliveryTag(), false);
                    }
                }
            }
        };
    }


}
