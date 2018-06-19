package com.github.fisherhe12.tutorials.ps;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Publish-Subscribe
 * 发布者实现
 * <p>
 * 1.Exchange采用fanout类型
 * 2.如果没有队列与exchange进行绑定,消息会丢失;同理,当没有消费者监听队列时,可以安全的丢弃队列的消息。
 * @author fisher
 * @date 2017-05-11
 */
public class Publisher {


    private static final String EXCHANGE_FANOUT_NAME = "exchange_fanout";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitMQUtil.getConnection();
        Channel channel = connection.createChannel();

        //--只声明Exchange,不声明队列以及绑定队列
        channel.exchangeDeclare(EXCHANGE_FANOUT_NAME, BuiltinExchangeType.FANOUT);

        String msg = "This is publish/subscribe msg...";

        channel.basicPublish(EXCHANGE_FANOUT_NAME, "", null, msg.getBytes());

        System.out.println("[x] Sent:" + msg);

        channel.close();
        connection.close();

    }
}
