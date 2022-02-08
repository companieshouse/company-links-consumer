package uk.gov.companieshouse.company.links.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.processor.InsolvencyStreamProcessor;
import uk.gov.companieshouse.delta.ChsDelta;

@Component
public class InsolvencyStreamConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsolvencyStreamConsumer.class);

    private final InsolvencyStreamProcessor deltaProcessor;

    @Autowired
    public InsolvencyStreamConsumer(InsolvencyStreamProcessor deltaProcessor) {
        this.deltaProcessor = deltaProcessor;
    }

    /**
     * Receives Main topic messages.
     */
    @KafkaListener(topics = "${insolvency.stream.topic.main}",
            groupId = "insolvency.stream.topic.main",
            autoStartup = "${company-links.consumer.insolvency.enable}")
    @Retryable
    public void receive(Message<ChsDelta> chsDeltaMessage) {
        LOGGER.info(
                "A new message read from MAIN topic with payload: " + chsDeltaMessage.getPayload());
        deltaProcessor.process(chsDeltaMessage);
    }

    /**
     * Receives Retry topic messages.
     */
    //TODO is groupId is same as topicId.
    @KafkaListener(topics = "${insolvency.stream.topic.retry}",
            groupId = "insolvency.stream.topic.retry",
            autoStartup = "${company-links.consumer.insolvency.enable}")
    public void retry(Message<ChsDelta> chsDeltaMessage) {
        LOGGER.info(
                String.format("A new message read from RETRY topic with payload:%s and headers:%s ",
                        chsDeltaMessage.getPayload(), chsDeltaMessage.getHeaders()));
        deltaProcessor.process(chsDeltaMessage);
    }
}
