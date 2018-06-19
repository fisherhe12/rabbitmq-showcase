package com.github.fisherhe12.tutorials.workqueue;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Work-Queue
 * 基于工作队列的生产者实现
 *
 * @author fisher
 * @date 2017-05-10
 */
public class WorkQueueProducer {
    private static final String WORK_QUEUE_NAME = "work-queue";

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        Connection connection = RabbitMQUtil.getConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(WORK_QUEUE_NAME, true, false, false, null);

        for (int i = 0; i < 100; i++) {
            String msg = "Produce work-queue msg :" + i;

            System.out.println("生产者发送消息:" + msg);

            channel.basicPublish("", WORK_QUEUE_NAME, null, msg.getBytes());
            Thread.sleep(100);
        }

        channel.close();
        connection.close();

    }
}
