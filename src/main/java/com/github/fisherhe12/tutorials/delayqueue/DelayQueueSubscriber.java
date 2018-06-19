package com.github.fisherhe12.tutorials.delayqueue;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 延迟队列消息发布者
 * <p>
 * 1.exchange类型设置成x-delayed-message
 * 2.绑定routing-key到队列上
 *
 * @author fisher
 * @date 2017-06-19
 */
public class DelayQueueSubscriber {
    private static final String DELAY_QUEUE_EXCHANGE = "delay_queue-exchange";
    private static final String DELAY_QUEUE_ROUTING_KEY = "delay_queue-routing-key";

    public static void main(String[] args) throws IOException {

        Connection connection = RabbitMQUtil.getConnection();
        Channel channel = connection.createChannel();


        // 声明一个延迟消息的Exchange
        Map<String, Object> params = new HashMap<>(1);
        params.put("x-delayed-type", "direct");
        channel.exchangeDeclare(DELAY_QUEUE_EXCHANGE, "x-delayed-message", true, false, params);

        String queue = channel.queueDeclare().getQueue();
        channel.queueBind(queue, DELAY_QUEUE_EXCHANGE, DELAY_QUEUE_ROUTING_KEY);
        channel.basicConsume(queue, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("消费者接受消息:" + message);
                System.out.println("消费者接受消息时间:" + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            }
        });



    }
}
