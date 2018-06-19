package com.github.fisherhe12.tutorials.routing;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 实现路由模式的生产者
 * <p>
 * 1.ExchangeL类型使用Direct
 * 2.Routing-key一对一匹配
 *
 * @author fisher
 * @date 2018-05-11
 */
public class RoutingProducer {

    private static final String ROUTING_EXCHANGE_NAME = "routing_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitMQUtil.getConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(ROUTING_EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        String msg = "This is warn msg";

        channel.basicPublish(ROUTING_EXCHANGE_NAME, "warn", null, msg.getBytes());

        channel.close();

        connection.close();
    }
}
