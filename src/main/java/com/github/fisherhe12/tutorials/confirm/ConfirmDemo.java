package com.github.fisherhe12.tutorials.confirm;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消息事务确认机制示例
 * <p>
 * 解决问题:生产者有没有成功发送消息到MQ服务器
 * <p>
 * 实现方式:Confirm模式
 *
 * @author fisher
 */
public class ConfirmDemo {
    private static final String CONFIRM_QUEUE = "confirm-queue";


    static class ConfirmProducer {

        public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(CONFIRM_QUEUE, false, false, false, null);

            String msg = "Send confirm msg...";
            //--生产者设成confirm模式
            channel.confirmSelect();
            channel.basicPublish("", CONFIRM_QUEUE, null, msg.getBytes());

            //--发送确认,根据waitForConfirms来判断消息成功送达到服务器
            if (!channel.waitForConfirms()) {
                System.out.println("Send confirm msg failure!");
            } else {
                System.out.println("Send confirm msg success!");
            }

            channel.close();
            connection.close();

        }

        static class ConfirmConsumer {
            public static void main(String[] args) throws IOException {
                Connection connection = RabbitMQUtil.getConnection();
                Channel channel = connection.createChannel();
                channel.queueDeclare(CONFIRM_QUEUE, false, false, false, null);

                Consumer consumer = RabbitMQUtil.buildConsumer(channel, true, 200);

                channel.basicConsume(CONFIRM_QUEUE, true, consumer);
            }
        }


    }
}
