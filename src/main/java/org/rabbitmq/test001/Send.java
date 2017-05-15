package org.rabbitmq.test001;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by Admin on 2017/5/15.
 */
public class Send {

    private static final String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = "Hello,world ！";
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        System.out.println("send " + message);

        //释放资源
        channel.close();
        connection.close();

    }
}
