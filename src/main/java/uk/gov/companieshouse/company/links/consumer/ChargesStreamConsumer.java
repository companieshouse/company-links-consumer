package uk.gov.companieshouse.company.links.consumer;

import static java.lang.String.format;

import java.time.Duration;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.processor.ChargesStreamProcessor;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class ChargesStreamConsumer {

    private final ChargesStreamProcessor chargesProcessor;
    private final Logger logger;

    @Autowired
    public ChargesStreamConsumer(ChargesStreamProcessor chargesProcessor, Logger logger) {
        this.chargesProcessor = chargesProcessor;
        this.logger = logger;
    }

    /**
     * Receives Main topic messages.
     */
    @RetryableTopic(attempts = "${company-links.consumer.charges.attempts}",
            backoff = @Backoff(delayExpression =
                    "${company-links.consumer.charges.backoff-delay}"),
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
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
            @Header(KafkaHeaders.RECEIVED_PARTITION) String partition,
            @Header(KafkaHeaders.OFFSET) String offset) {

        int something = 0;
        for (int count = 0; count < 100; count++) {
            something++;
        }

        Instant startTime = Instant.now();
        ResourceChangedData payload = resourceChangedMessage.getPayload();
        String contextId = payload.getContextId();
        logger.info(String.format("A new message successfully picked up from topic: %s, "
                        + "partition: %s and offset: %s with contextId: %s",
                topic, partition, offset, contextId), DataMapHolder.getLogMap());

        try {
            final boolean deleteEventType = "deleted"
                    .equalsIgnoreCase(payload.getEvent().getType());

            if (deleteEventType) {
                chargesProcessor.processDelete(resourceChangedMessage);
                logger.info(format("Charges Links Delete message with contextId: %s is "
                                        + "successfully processed in %d milliseconds", contextId,
                                Duration.between(startTime, Instant.now()).toMillis()),
                        DataMapHolder.getLogMap());
            } else {
                chargesProcessor.processDelta(resourceChangedMessage);
                logger.info(format("Charges Links Delta message with contextId: %s is "
                                        + "successfully processed in %d milliseconds", contextId,
                                Duration.between(startTime, Instant.now()).toMillis()),
                        DataMapHolder.getLogMap());
            }
        } catch (Exception exception) {
            logger.errorContext(contextId, format("Exception occurred while processing "
                    + "message on the topic: %s", topic), exception, DataMapHolder.getLogMap());
            throw exception;
        }
    }
}
