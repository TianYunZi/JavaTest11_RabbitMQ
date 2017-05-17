package org.rabbitmq.test007.delay.queue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created by Admin on 2017/5/17.
 * 消息发送端
 */
public class Send {

    //队列名称
    private final static String EXCHANGE_NAME = "delay_exchange";
    private final static String ROUTING_KEY = "key_delay";

    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接连接到RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        // 声明x-delayed-type类型的exchange
        Map<String, Object> map = new HashMap<>();
        map.put("x-delayed-type", "direct");
        channel.exchangeDeclare(EXCHANGE_NAME, "x-delayed-message", true, false, map);

        Map<String, Object> headers = new HashMap<>();
        //设置在2016/11/04,16:45:12向消费端推送本条消息
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime timeToPublish = LocalDateTime.of(2017, 5, 17, 15, 33, 0);
        String readyToPushContent = "publish at " + formatter.format(now) + "\t deliver at" + formatter.format(timeToPublish);
        headers.put("x-delay", Duration.between(now, timeToPublish).toMillis());
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().headers(headers).build();
        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, properties, readyToPushContent.getBytes("utf-8"));

        // 关闭频道和连接
        channel.close();
        connection.close();
    }
}
