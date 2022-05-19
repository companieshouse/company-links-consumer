package uk.gov.companieshouse.company.links.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.FixedDelayStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
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
    @RetryableTopic(attempts = "${company-links.consumer.charges.attempts}",
            backoff = @Backoff(delayExpression =
                    "${company-links.consumer.charges.backoff-delay}"),
            fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC,
            retryTopicSuffix = "-${company-links.consumer.charges.group-id}-retry",
            dltTopicSuffix = "-${company-links.consumer.charges.group-id}-error",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "false",
            exclude = NonRetryableErrorException.class)
    @KafkaListener(
            id = "${company-links.consumer.charges.topic}-consumer",
            topics = "${company-links.consumer.charges.topic}",
            groupId = "${company-links.consumer.charges.group-id}",
            autoStartup = "${company-links.consumer.charges.enable}",
            containerFactory = "listenerContainerFactory")
    public void receive(Message<ResourceChangedData> resourceChangedMessage,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partition,
                        @Header(KafkaHeaders.OFFSET) String offset) {
        ResourceChangedData payload = resourceChangedMessage.getPayload();
        String logContext = payload.getContextId();

        logger.info(String.format("A new message successfully picked up "
                + "from %s topic with contextId: %s", topic, logContext));
        try {
            final boolean deleteEventType = "deleted"
                    .equalsIgnoreCase(payload.getEvent().getType());

            if (deleteEventType) {
                chargesProcessor.processDelete(resourceChangedMessage);
            } else {
                chargesProcessor.process(resourceChangedMessage);
            }
        } catch (Exception exception) {
            logger.error(String.format("Exception occurred while processing the topic %s "
                            + "with contextId %s, exception thrown is %s",
                    topic, logContext, exception));
            throw exception;
        }

    }

}
