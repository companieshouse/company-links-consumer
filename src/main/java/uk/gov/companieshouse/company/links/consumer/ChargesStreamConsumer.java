package uk.gov.companieshouse.company.links.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.processor.ChargesStreamProcessor;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class ChargesStreamConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ChargesStreamConsumer.class);

    private final ChargesStreamProcessor chargesProcessor;

    @Autowired
    public ChargesStreamConsumer(ChargesStreamProcessor chargesProcessor) {
        this.chargesProcessor = chargesProcessor;
    }

    /**
     * Receives Main topic messages.
     */
    @KafkaListener(topics = "${company-links.consumer.charges.topic.main}", 
            groupId = "${company-links.consumer.charges.group-id}",
            autoStartup = "${company-links.consumer.charges.enable}")
    public void receive(Message<ResourceChangedData> resourceChangedMessage)
            throws JsonProcessingException {
        logger.info(
                String.format("A new message read from MAIN topic with payload: %s",
                        resourceChangedMessage.getPayload()));
        chargesProcessor.process(resourceChangedMessage);
    }

}
