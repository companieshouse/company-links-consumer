package uk.gov.companieshouse.company.links.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.processor.ChargesStreamProcessor;
import uk.gov.companieshouse.delta.ChsDelta;

@Component
public class ChargesStreamConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChargesStreamConsumer.class);

    private final ChargesStreamProcessor streamProcessor;

    @Autowired
    public ChargesStreamConsumer(ChargesStreamProcessor streamProcessor) {
        this.streamProcessor = streamProcessor;
    }

    /**
     * Receives Main topic messages.
     */
    // TODO is groupId is same as topicId.  // name of component will be groupId
    @KafkaListener(topics = "${charges.stream.topic.main}", groupId = "charges.stream.topic.main",
            autoStartup = "${company-links.consumer.charges.enable}")
    @Retryable
    public void receive(Message<ChsDelta> chsDeltaMessage) {
        LOGGER.info(
                "A new message read from MAIN topic with payload: " + chsDeltaMessage.getPayload());
        streamProcessor.process(chsDeltaMessage);
    }

    /**
     * Receives Retry topic messages.
     */
    @KafkaListener(topics = "${charges.stream.topic.retry}", groupId = "charges.stream.topic.retry",
            autoStartup = "${company-links.consumer.charges.enable}")
    public void retry(Message<ChsDelta> chsDeltaMessage) {
        LOGGER.info(
                String.format("A new message read from RETRY topic with payload:%s and headers:%s ",
                        chsDeltaMessage.getPayload(), chsDeltaMessage.getHeaders()));
        streamProcessor.process(chsDeltaMessage);
    }
}
