package org.rabbitmq.test007.delay.queue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeoutException;

/**
 * Created by Admin on 2017/5/17.
 */
public class Receive {
    // 队列名称
    private final static String QUEUE_NAME = "delay_queue";
    private final static String EXCHANGE_NAME = "delay_exchange";
    private final static String ROUTING_KEY = "key_delay";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

//        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

        System.out.println("****************WAIT***************");

        channel.basicConsume(QUEUE_NAME, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "utf-8");
                System.out.println("message:" + message);
                System.out.println("now:\t" + formatter.format(LocalDateTime.now()));
            }
        });
//        channel.basicConsume(QUEUE_NAME, true, queueingConsumer);
//
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
//
//        try {
//            System.out.println("****************WAIT***************");
//            while (true) {
//                QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
//                String message = new String(delivery.getBody(), "utf-8");
//                System.out.println("message:" + message);
//                System.out.println("now:\t" + formatter.format(LocalDateTime.now()));
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
}
