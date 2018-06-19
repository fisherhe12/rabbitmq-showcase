package com.github.fisherhe12.tutorials.ps;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;

import java.io.IOException;

/**
 * Publish-Subscribe
 * 订阅者实现
 * <p>
 * 1.Exchange需要绑定具体的队列
 * 2.fanout类型的Exchange可以不用指定Routing-Key
 *
 * @author fisher
 * @date 2017-05-11
 */
public class Subscriber {
    private static final String EXCHANGE_FANOUT_NAME = "exchange_fanout";


    static class Receiver1 {
        public static void main(String[] args) throws IOException {

            Connection connection = RabbitMQUtil.getConnection();

            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_FANOUT_NAME, BuiltinExchangeType.FANOUT);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_FANOUT_NAME, "");

            Consumer consumer = RabbitMQUtil.buildConsumer(channel, true, 200);

            channel.basicConsume(queueName, true, consumer);

        }
    }


    static class Receiver2 {
        public static void main(String[] args) throws IOException {
            Connection connection = RabbitMQUtil.getConnection();

            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_FANOUT_NAME, BuiltinExchangeType.FANOUT);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_FANOUT_NAME, "");

            Consumer consumer = RabbitMQUtil.buildConsumer(channel, true, 100);

            channel.basicConsume(queueName, true, consumer);

        }
    }

}
