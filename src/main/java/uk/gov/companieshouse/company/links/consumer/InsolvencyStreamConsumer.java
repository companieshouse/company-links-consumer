package uk.gov.companieshouse.company.links.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import uk.gov.companieshouse.company.links.processor.InsolvencyStreamProcessor;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class InsolvencyStreamConsumer {

    private final InsolvencyStreamProcessor insolvencyProcessor;
    private static final Logger logger = LoggerFactory.getLogger("InsolvencyStreamConsumer");

    @Autowired
    public InsolvencyStreamConsumer(InsolvencyStreamProcessor insolvencyProcessor) {
        this.insolvencyProcessor = insolvencyProcessor;
    }

    /**
     * Receives Main topic messages.
     */
    @KafkaListener(
            id = "${company-links.consumer.insolvency.topic}-consumer",
            topics = "${company-links.consumer.insolvency.topic}",
            groupId = "${company-links.consumer.insolvency.group-id}",
            autoStartup = "${company-links.consumer.insolvency.enable}",
            containerFactory = "listenerContainerFactory"
            )
    public void receive(Message<ResourceChangedData> resourceChangedMessage) {
        logger.info(
                "A new message read from Stream-insolvency topic with payload: "
                        + resourceChangedMessage.getPayload());
        insolvencyProcessor.process(resourceChangedMessage);
    }

}
