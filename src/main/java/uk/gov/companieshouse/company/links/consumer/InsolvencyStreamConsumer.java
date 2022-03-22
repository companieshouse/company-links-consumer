package uk.gov.companieshouse.company.links.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.processor.InsolvencyStreamProcessor;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class InsolvencyStreamConsumer {

    private static final Logger logger = LoggerFactory.getLogger(InsolvencyStreamConsumer.class);

    private final InsolvencyStreamProcessor insolvencyProcessor;

    @Autowired
    public InsolvencyStreamConsumer(InsolvencyStreamProcessor insolvencyProcessor) {
        this.insolvencyProcessor = insolvencyProcessor;
    }

    /**
     * Receives Main topic messages.
     */
    @KafkaListener(topics = "${insolvency.stream.topic.main}",
            groupId = "${insolvency.stream.group-id}",
            containerFactory = "listenerContainerFactory")
    public void receive(Message<ResourceChangedData> resourceChangedMessage) {
        logger.info(String.format("A new message read from MAIN topic with payload: %s", 
                resourceChangedMessage.getPayload())
        );
        insolvencyProcessor.process(resourceChangedMessage);
    }
}
