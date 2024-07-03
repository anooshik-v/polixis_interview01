package am.anooshik.demo.service;

import am.anooshik.demo.model.Message;
import am.anooshik.demo.repository.MessageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class KafkaConsumerService {

    @Autowired
    private MessageRepository messageRepository;

    @KafkaListener(topics = "quickstart-events")
    public void consume(String message) {
        Message msg = new Message();
        msg.setContent(message);
        msg.setDateAdded(LocalDateTime.now()   );
        msg.setTopic("quickstart-events");
        messageRepository.save(msg);
    }
}