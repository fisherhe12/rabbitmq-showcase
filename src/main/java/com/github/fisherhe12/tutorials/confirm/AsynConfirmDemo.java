package com.github.fisherhe12.tutorials.confirm;

import com.github.fisherhe12.common.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

/**
 * 消息事务确认机制示例
 * 异步confirm确认
 *
 * @author fisher
 */
public class AsynConfirmDemo {
    private static final String ASYNC_CONFIRM_QUEUE = "async—confirm-queue";


    static class ConfirmProducer {

        public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(ASYNC_CONFIRM_QUEUE, false, false, false, null);

            //--未确认消息标识集合--//
            SortedSet<Long> unconfirmedSet =
                    Collections.synchronizedSortedSet(new TreeSet<Long>());

            channel.addConfirmListener(new ConfirmListener() {
                //--处理成功的--//
                @Override
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    if (multiple) {
                        System.out.println("---handleAck---multiple");
                        unconfirmedSet.headSet(deliveryTag + 1).clear();
                    } else {
                        System.out.println("---handleAck---single");
                        unconfirmedSet.remove(deliveryTag);
                    }
                }

                //--处理失败的--//
                @Override
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    if (multiple) {
                        System.out.println("---handleNack---multiple----"+deliveryTag);
                        unconfirmedSet.headSet(deliveryTag + 1).clear();
                    } else {
                        System.out.println("---handleNack---single");
                        unconfirmedSet.remove(deliveryTag);
                    }
                }
            });
            //--生产者设成confirm模式
            channel.confirmSelect();

            String msg = "Send async confirm msg ";
            for (long i = 0; i < 20; ++i) {
                long seqNo = channel.getNextPublishSeqNo();
                unconfirmedSet.add(seqNo);
                channel.basicPublish("", ASYNC_CONFIRM_QUEUE, null, msg.getBytes());
            }

            channel.close();
            connection.close();
        }

        static class ConfirmConsumer {
            public static void main(String[] args) throws IOException {
                Connection connection = RabbitMQUtil.getConnection();

                Channel channel = connection.createChannel();
                channel.queueDeclare(ASYNC_CONFIRM_QUEUE, false, false, false, null);

                Consumer consumer = RabbitMQUtil.buildConsumer(channel, true, 300);

                channel.basicConsume(ASYNC_CONFIRM_QUEUE, true, consumer);
            }
        }


    }

}
