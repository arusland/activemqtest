package io.arusland.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.RandomStringUtils;

import javax.jms.*;

public class ThroughputTestApp {

    public static final int MSG_COUNT = 1000;
    public static final int MSG_COUNT_LARGE = 1_000_000;

    public static void main(String[] args) throws Exception {
        System.out.println("Start testing in DeliveryMode=DeliveryMode.PERSISTENT");
        thread(new HelloWorldProducer(DeliveryMode.PERSISTENT, MSG_COUNT), false);
        Thread thread = thread(new HelloWorldConsumer(), false);

        thread.join();

        System.out.println("------------------------------------------------------");
        System.out.println("Start testing in DeliveryMode=DeliveryMode.NON_PERSISTENT");
        thread(new HelloWorldProducer(DeliveryMode.NON_PERSISTENT, MSG_COUNT), false);
        thread = thread(new HelloWorldConsumer(), false);

        thread.join();

        System.out.println("------------------------------------------------------");
        System.out.println("Start testing in DeliveryMode=DeliveryMode.NON_PERSISTENT");
        thread(new HelloWorldProducer(DeliveryMode.NON_PERSISTENT, MSG_COUNT_LARGE), false);
        thread(new HelloWorldConsumer(), false);
    }


    public static Thread thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();

        return brokerThread;
    }

    public static class HelloWorldProducer implements Runnable {
        private final int deliveryMode;
        private final long msgCount;

        public HelloWorldProducer(int deliveryMode, long msgCount) {
            this.deliveryMode = deliveryMode;
            this.msgCount = msgCount;
        }

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
                producer.setDeliveryMode(deliveryMode);

                // Create a messages
                System.out.println("Sending messages: " + msgCount);
                String text = RandomStringUtils.random(4*1024, "ячмсмраиырвиарваиыоваикокирыкк");
                long lastTime = System.currentTimeMillis();

                for (int i = 0; i < msgCount; i++) {
                    TextMessage message = session.createTextMessage(text);
                    producer.send(message);
                }

                long elapsed = System.currentTimeMillis() - lastTime;
                System.out.println("Sent messages in " + elapsed + " ms");
                System.out.println("Throughput: " + msgCount * 1000/Math.max(elapsed, 1) + " msg/sec");

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

                long lastTime = System.currentTimeMillis();

                while (true) {
                    // Wait for a message
                    Message message = consumer.receive();

                    if (message instanceof TextMessage) {

                        TextMessage textMessage = (TextMessage) message;
                        String text = textMessage.getText();


                        if ("exit".equals(text)) {
                            break;
                        }
                    }
                }

                System.out.println("Received all in " + (System.currentTimeMillis() - lastTime) + " ms");

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