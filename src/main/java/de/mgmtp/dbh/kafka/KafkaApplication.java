package de.mgmtp.dbh.kafka;


    import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

    import org.apache.commons.lang3.RandomStringUtils;
    import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

    @SpringBootApplication
    public class KafkaApplication {

        public static void main(String[] args) throws Exception {

            ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);

            MessageProducer producer = context.getBean(MessageProducer.class);
            MessageListener listener = context.getBean(MessageListener.class);

            /*
             * Sending message to 'order' topic. This will send
             * and recieved a java object with the help of
             * greetingKafkaListenerContainerFactory.
             */
            producer.sendOrderMessage(new Order(RandomStringUtils.randomAlphanumeric(10)));
            listener.orderLatch.await(10, TimeUnit.SECONDS);

            context.close();
        }

        @Bean
        public MessageProducer messageProducer() {
            return new MessageProducer();
        }

        @Bean
        public MessageListener messageListener() {
            return new MessageListener();
        }

        public static class MessageProducer {

            @Autowired
            private KafkaTemplate<String, String> kafkaTemplate;

            @Autowired
            private KafkaTemplate<String, Order> orderKafkaTemplate;


            @Value(value = "${order.topic.name}")
            private String orderTopicName;

            public void sendMessage(String message) {

                ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(orderTopicName, message);

                future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

                    @Override
                    public void onSuccess(SendResult<String, String> result) {
                        System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
                    }
                    @Override
                    public void onFailure(Throwable ex) {
                        System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
                    }
                });
            }



            public void sendOrderMessage(Order order) {
                System.out.println("Sent new OrderMessage " + order);
                orderKafkaTemplate.send(orderTopicName, order);
            }
        }

        public static class MessageListener {



            private CountDownLatch orderLatch = new CountDownLatch(1);


            @KafkaListener(topics = "${order.topic.name}", containerFactory = "orderKafkaListenerContainerFactory")
            public void orderListener(Order order) {
                System.out.println("Recieved new Order message: " + order);
                this.orderLatch.countDown();
            }

        }

    }


