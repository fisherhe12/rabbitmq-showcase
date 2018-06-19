package com.github.fisherhe12.tutorials.topics;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;

import java.io.IOException;

/**
 * 实现主题模式的消费者
 * <p>
 * 1.与direct区别在于routing-key可以进行通配
 *
 * @author fisher
 * @date 2018-05-11
 */
public class TopicConsumer {

    private static final String TOPIC_EXCHANGE_NAME = "topic_exchange";


    static class Receiver1 {

        public static void main(String[] args) throws IOException {
            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, TOPIC_EXCHANGE_NAME, "user.add");
            channel.queueBind(queueName, TOPIC_EXCHANGE_NAME, "user.delete");

            Consumer consumer = RabbitMQUtil.buildConsumer(channel, true, 100);

            channel.basicConsume(queueName, true, consumer);
        }
    }


    static class Receiver2 {
        public static void main(String[] args) throws IOException {
            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, TOPIC_EXCHANGE_NAME, "user.#");

            Consumer consumer = RabbitMQUtil.buildConsumer(channel, true, 200);
            channel.basicConsume(queueName, true, consumer);
        }
    }
}
