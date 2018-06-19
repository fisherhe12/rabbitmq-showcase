package com.github.fisherhe12.tutorials.delayqueue;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 延迟队列消息发布者
 * <p>
 * 1.安装rabbitmq_delayed_message_exchange插件
 * 2.exchange类型设置成x-delayed-message
 * 3.发布消息时候设置header属性x-delay
 *
 * @author fisher
 * @date 2017-06-19
 */
public class DelayQueuePublisher {
    private static final String DELAY_QUEUE_EXCHANGE = "delay_queue-exchange";
    private static final String DELAY_QUEUE_ROUTING_KEY = "delay_queue-routing-key";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitMQUtil.getConnection();
        Channel channel = connection.createChannel();

        // 声明一个延迟消息的Exchange
        Map<String, Object> params = new HashMap<>(1);
        params.put("x-delayed-type", "direct");
        channel.exchangeDeclare(DELAY_QUEUE_EXCHANGE, "x-delayed-message", true, false, params);


        System.out.println("消息发送时间:" +LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        String msg = "Send msg ....";
        Map<String, Object> headerParams = new HashMap<>(1);
        headerParams.put("x-delay", 3000);
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().headers(headerParams).build();
        channel.basicPublish(DELAY_QUEUE_EXCHANGE, DELAY_QUEUE_ROUTING_KEY, properties, msg.getBytes());

        channel.close();
        connection.close();

    }
}
