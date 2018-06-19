package com.github.fisherhe12.tutorials.simple;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * 点对点模式的消费者
 *
 * @author fisher
 * @date 2017-05-10
 */
public class SimpleConsumer {
    private static final String SIMPLE_QUEUE_NAME = "simple-queue";

    public static void main(String[] args) throws IOException {
        Connection connection = RabbitMQUtil.getConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(SIMPLE_QUEUE_NAME, true, false, false, null);
        Consumer consumer = buildConsumer(channel);
        channel.basicConsume(SIMPLE_QUEUE_NAME, true, consumer);
    }

    private static Consumer buildConsumer(Channel channel) {
        return new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {

                String message = new String(body, "UTF-8");
                System.out.println("接受消息:" + message);
            }
        };
    }
}
