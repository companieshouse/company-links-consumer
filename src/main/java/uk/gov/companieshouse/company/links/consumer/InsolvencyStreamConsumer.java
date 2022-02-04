package uk.gov.companieshouse.company.links.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.processor.InsolvencyStreamProcessor;
import uk.gov.companieshouse.delta.ChsDelta;

//TODO Do we need to load a component based on the property value as suggested by 'Dom'
// Action: Zaid to confirm
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
            groupId = "insolvency.stream.topic.main")
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
            groupId = "insolvency.stream.topic.retry")
    public void retry(Message<ChsDelta> chsDeltaMessage) {
        LOGGER.info(
                String.format("A new message read from RETRY topic with payload:%s and headers:%s ",
                        chsDeltaMessage.getPayload(), chsDeltaMessage.getHeaders()));
        deltaProcessor.process(chsDeltaMessage);
    }

    /**
     * Receives Error topic messages.
     */
    @KafkaListener(topics = "${insolvency.stream.topic.error}",
            groupId = "insolvency.stream.topic.error")
    public void error(Message<ChsDelta> chsDeltaMessage) {
        LOGGER.info(
                String.format("A new message read from ERROR topic with payload:%s and headers:%s ",
                        chsDeltaMessage.getPayload(), chsDeltaMessage.getHeaders()));
        deltaProcessor.process(chsDeltaMessage);
    }
}
