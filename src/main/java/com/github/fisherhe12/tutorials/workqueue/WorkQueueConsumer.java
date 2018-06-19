package com.github.fisherhe12.tutorials.workqueue;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;

import java.io.IOException;

/**
 * Work-Queue
 * 基于工作队列的消费者实现
 *
 * @author fisher
 * @date 2017-05-10
 */
public class WorkQueueConsumer {
    private static final String WORK_QUEUE_NAME = "work-queue";


    /**
     * 消费端采用fair分发策略
     * 1.通过basicQo设置prefetch=1
     * 2.关闭自动ACK并进行手动确认消息
     */
    static class FairConsumer {
        public static void main(String[] args) throws IOException {
            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(WORK_QUEUE_NAME, false, false, false, null);

            Consumer consumer = RabbitMQUtil.buildConsumer(channel, false, 2000);
            channel.basicQos(1);
            channel.basicConsume(WORK_QUEUE_NAME, false, consumer);
        }
    }


    static class FairConsumer1 {
        public static void main(String[] args) throws IOException {
            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(WORK_QUEUE_NAME, false, false, false, null);
            Consumer consumer = RabbitMQUtil.buildConsumer(channel, false, 1000);

            channel.basicQos(1);
            channel.basicConsume(WORK_QUEUE_NAME, false, consumer);
        }
    }



    /**
     * 消费端采用默认的round-robin分发策略,消息均匀的分发在每个消费者
     */
    static class RoundRobinConsumer {
        public static void main(String[] args) throws IOException {
            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(WORK_QUEUE_NAME, false, false, false, null);

            Consumer consumer = RabbitMQUtil.buildConsumer(channel, true, 1000);

            channel.basicConsume(WORK_QUEUE_NAME, true, consumer);
        }

    }


    static class RoundRobinConsumer1 {
        public static void main(String[] args) throws IOException {
            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(WORK_QUEUE_NAME, false, false, false, null);

            Consumer consumer = RabbitMQUtil.buildConsumer(channel, true, 2000);

            channel.basicConsume(WORK_QUEUE_NAME, true, consumer);

        }

    }
}
