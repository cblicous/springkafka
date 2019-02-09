package de.mgmtp.dbh.kafka;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) throws Exception {

        ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);

        MessageListener listener = context.getBean(MessageListener.class);

       //  context.close();
    }


    @Bean
    public MessageListener messageListener() {
        return new MessageListener();
    }


    public static class MessageListener {

        @KafkaListener(topics = "${order.topic.name}", containerFactory = "orderKafkaListenerContainerFactory")
        public void orderListener(Order order) {
            System.out.println("Recieved new Order message: " + order);
        }

    }

}


