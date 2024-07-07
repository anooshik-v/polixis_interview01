import am.anooshik.demo.model.Message;
import am.anooshik.demo.repository.MessageRepository;
import am.anooshik.demo.service.KafkaConsumerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = {am.anooshik.demo.DemoApplication.class})
@EmbeddedKafka(partitions = 1, topics = {"quickstart-events"})
public class KafkaConsumerServiceTest {

   @Autowired private KafkaConsumerService kafkaConsumerService;

   @Autowired private MessageRepository messageRepository;

   @AfterEach
   public void deleteMessages(){
       messageRepository.deleteAll();

   }

    @Test
    public void testConsume() {
        // Produce a message to Kafka topic
        String message = "\"test message\"";
        kafkaConsumerService.consume(message);

        // Verify that message is saved in the database
        Message savedMessage = messageRepository.findAll().get(0);
        assertThat(savedMessage.getContent()).isEqualTo(message);
        assertThat(savedMessage.getTopic()).isEqualTo("quickstart-events");

    }

    @Test
    public void testConsumeJsonFields() {
        // Produce a message to Kafka topic
        String jsonString = "{ \"subject\": \"John\", \"age\": 30, \"field\": \"New York\" }";
        kafkaConsumerService.consume(jsonString);

        // Verify that message is saved in the database
        Message savedMessage = messageRepository.findAll().get(0);
        assertThat(savedMessage.getContent()).isEqualTo(jsonString);
        assertThat(savedMessage.getTopic()).isEqualTo("quickstart-events");
        assertThat(savedMessage.getSubject()).isEqualTo("John");
        assertThat(savedMessage.getField()).isEqualTo("New York");

    }
}