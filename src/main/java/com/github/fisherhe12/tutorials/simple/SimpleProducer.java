package com.github.fisherhe12.tutorials.simple;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 点对点模式的生产者
 *
 * @author fisher
 * @date 2017-05-10
 */
public class SimpleProducer {
    private static final String MSG = "This is a simple msg...";
    private static final String SIMPLE_QUEUE_NAME = "simple-queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitMQUtil.getConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(SIMPLE_QUEUE_NAME, true, false, false, null);
        channel.basicPublish("", SIMPLE_QUEUE_NAME, null, MSG.getBytes());

        channel.close();
        connection.close();
    }
}
