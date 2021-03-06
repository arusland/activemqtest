package io.arusland.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class SendMaxMessageApp {

    public static void main(String[] args) throws Exception {
        thread(new HelloWorldConsumer(), false);
        Thread.sleep(1000);

        thread(new HelloWorldProducer(), false);
    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    public static class HelloWorldProducer implements Runnable {
        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue("TEST.FOO");

                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                // Create a messages
                //String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
                long lastTime = System.currentTimeMillis();
                String text = RandomStringUtils.random(49*1024*1024, "ячмсмраиырвиарваиыоваикокирыкк");
                System.out.println("PRODUCER: Create string in " + (System.currentTimeMillis() - lastTime));
                lastTime = System.currentTimeMillis();
                TextMessage message = session.createTextMessage(text);
                System.out.println("PRODUCER: Create message in " + (System.currentTimeMillis() - lastTime));
                lastTime = System.currentTimeMillis();

                // Tell the producer to send the message
                //System.out.println("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName());
                producer.send(message);

                System.out.println("PRODUCER: Sent message in " + (System.currentTimeMillis() - lastTime));

                producer.send(session.createTextMessage("exit"));

                // Clean up
                session.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }
    }

    public static class HelloWorldConsumer implements Runnable, ExceptionListener {
        public void run() {
            try {

                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                connection.setExceptionListener(this);

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue("TEST.FOO");

                // Create a MessageConsumer from the Session to the Topic or Queue
                MessageConsumer consumer = session.createConsumer(destination);

                while (true) {
                    // Wait for a message
                    Message message = consumer.receive();

                    if (message instanceof TextMessage) {

                        TextMessage textMessage = (TextMessage) message;
                        long lastTime = System.currentTimeMillis();

                        String text = textMessage.getText();
                        System.out.println("RECEIVER: Get text in " + (System.currentTimeMillis() - lastTime));

                        System.out.println("RECEIVER: Received: " + StringUtils.abbreviate(text, 100));
                        System.out.println("RECEIVER: Received " + text.length() + " chars");

                        if ("exit".equals(text)) {
                            break;
                        }
                    } else {
                        System.out.println("RECEIVER: Received: " + message);
                    }
                }

                consumer.close();
                session.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }
}