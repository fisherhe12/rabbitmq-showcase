package com.github.fisherhe12.tutorials.routing;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;

import java.io.IOException;

/**
 * 实现路由模式的消费者
 * <p>
 * 1.ExchangeL类型使用Direct
 * 2.Exchange绑定队列的同时指定Routing-key;Exchange可以进行多次绑定
 * 3.Routing-key一对一匹配
 *
 * @author fisher
 * @date 2018-05-11
 */
public class RoutingConsumer {
    private static final String ERROR_ROUTING_KEY = "error";
    private static final String INFO_ROUTING_KEY = "info";
    private static final String WARN_ROUTING_KEY = "warn";
    private static final String ROUTING_EXCHANGE_NAME = "routing_exchange";


    static class Receiver1 {

        public static void main(String[] args) throws IOException {
            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(ROUTING_EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, ROUTING_EXCHANGE_NAME, INFO_ROUTING_KEY);
            channel.queueBind(queueName, ROUTING_EXCHANGE_NAME, ERROR_ROUTING_KEY);

            Consumer consumer = RabbitMQUtil.buildConsumer(channel, true, 100);

            channel.basicConsume(queueName, true, consumer);
        }
    }


    static class Receiver2 {
        public static void main(String[] args) throws IOException {
            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(ROUTING_EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, ROUTING_EXCHANGE_NAME, ERROR_ROUTING_KEY);
            channel.queueBind(queueName, ROUTING_EXCHANGE_NAME, WARN_ROUTING_KEY);

            Consumer consumer = RabbitMQUtil.buildConsumer(channel, true, 200);

            channel.basicConsume(queueName, true, consumer);

        }
    }
}
