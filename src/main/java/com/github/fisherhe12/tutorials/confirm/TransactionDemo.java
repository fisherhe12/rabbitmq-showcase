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
 * 实现方式:AMQP实现事务机制
 *
 * @author fisher
 */
public class TransactionDemo {
    private static final String TRANSACTION_QUEUE = "transaction-queue";


    static class TxProducer {

        public static void main(String[] args) throws IOException, TimeoutException {

            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(TRANSACTION_QUEUE, false, false, false, null);

            String msg = "send tx msg...";

            try {
                channel.txSelect();
                //--模拟运行期异常--//
                mockRuntimeException();
                channel.basicPublish("", TRANSACTION_QUEUE, null, msg.getBytes());
                channel.txCommit();
            } catch (IOException e) {
                channel.txRollback();
                System.out.println("rollback!");
            } finally {
                channel.close();
                connection.close();
            }


        }

        private static void mockRuntimeException() {
            throw new RuntimeException("这是一个模拟的运行期异常");
        }

    }


    static class TxConsumer {
        public static void main(String[] args) throws IOException {
            Connection connection = RabbitMQUtil.getConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(TRANSACTION_QUEUE, false, false, false, null);

            Consumer consumer = RabbitMQUtil.buildConsumer(channel, true, 100);

            channel.basicConsume(TRANSACTION_QUEUE, true, consumer);
        }
    }
}
