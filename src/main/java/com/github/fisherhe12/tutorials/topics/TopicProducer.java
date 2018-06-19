package com.github.fisherhe12.tutorials.topics;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 实现主题模式的生产者
 * <p>
 * 1.Routing-key通配模式,通配符--#:匹配一个或者多个,*:匹配一个
 *
 * @author fisher
 * @date 2018-05-11
 */
public class TopicProducer {

    private static final String TOPIC_EXCHANGE_NAME = "topic_exchange";
    private static final String USER_SAVE_ROUTING_KEY = "user.add";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitMQUtil.getConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String msg = "Send add user msg...";

        channel.basicPublish(TOPIC_EXCHANGE_NAME, USER_SAVE_ROUTING_KEY, null, msg.getBytes());

        channel.close();
        connection.close();

    }
}
