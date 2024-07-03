import am.anooshik.demo.model.Message;
import am.anooshik.demo.repository.MessageRepository;
import am.anooshik.demo.service.KafkaConsumerService;
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


    @Test
    public void testConsume() {
        // Produce a message to Kafka topic
        String message = "test message";
        kafkaConsumerService.consume(message);

        // Verify that message is saved in the database
        Message savedMessage = messageRepository.findAll().get(0);
        assertThat(savedMessage.getContent()).isEqualTo(message);
        assertThat(savedMessage.getTopic()).isEqualTo("quickstart-events");

    }
}