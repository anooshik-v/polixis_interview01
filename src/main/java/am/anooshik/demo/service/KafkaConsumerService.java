package am.anooshik.demo.service;

import am.anooshik.demo.model.Message;
import am.anooshik.demo.repository.MessageRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;

@Service
public class KafkaConsumerService {

    @Autowired
    private MessageRepository messageRepository;

    @KafkaListener(topics = "quickstart-events")
    public void consume(String message) {
        Message msg = extractParameters(message);
        // parse json content

        msg.setContent(message);
        msg.setDateAdded(LocalDateTime.now()   );
        msg.setTopic("quickstart-events");
        messageRepository.save(msg);
    }
    public Message extractParameters(String jsonString) {
        ObjectMapper objectMapper = new ObjectMapper();
        Message message=new Message();
        try {
            JsonNode rootNode = objectMapper.readTree(jsonString);
            Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode.fields();

            while (fieldsIterator.hasNext()) {
                Map.Entry<String, JsonNode> field = fieldsIterator.next();
                if(field.getKey().equals("subject")){
                    message.setSubject(field.getValue().textValue());
                }else    if(field.getKey().equals("field")) {
                    message.setField(field.getValue().textValue());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  message;
    }
}