package uk.gov.companieshouse.company.links.consumer;

import static java.lang.String.format;

import java.time.Duration;
import java.time.Instant;
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
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.logging.LogKafkaConsumerMessage;
import uk.gov.companieshouse.company.links.processor.InsolvencyStreamProcessor;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class InsolvencyStreamConsumer {

    private final InsolvencyStreamProcessor insolvencyProcessor;
    private final Logger logger;

    @Autowired
    public InsolvencyStreamConsumer(InsolvencyStreamProcessor insolvencyProcessor, Logger logger) {
        this.insolvencyProcessor = insolvencyProcessor;
        this.logger = logger;
    }

    /**
     * Receives Main topic messages.
     */
    @RetryableTopic(attempts = "${company-links.consumer.insolvency.attempts}",
            backoff = @Backoff(delayExpression =
                    "${company-links.consumer.insolvency.backoff-delay}"),
            fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC,
            retryTopicSuffix = "-${company-links.consumer.insolvency.group-id}-retry",
            dltTopicSuffix = "-${company-links.consumer.insolvency.group-id}-error",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "false",
            exclude = NonRetryableErrorException.class)
    @KafkaListener(
            id = "${company-links.consumer.insolvency.topic}-consumer",
            topics = "${company-links.consumer.insolvency.topic}",
            groupId = "${company-links.consumer.insolvency.group-id}",
            autoStartup = "${company-links.consumer.insolvency.enable}",
            containerFactory = "listenerContainerFactory")
    @LogKafkaConsumerMessage
    public void receive(Message<ResourceChangedData> resourceChangedMessage,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partition,
            @Header(KafkaHeaders.OFFSET) String offset) {
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
                insolvencyProcessor.processDelete(resourceChangedMessage);
                logger.info(format("Insolvency Links Delete message with contextId: %s is "
                                        + "successfully processed in %d milliseconds", contextId,
                                Duration.between(startTime, Instant.now()).toMillis()),
                        DataMapHolder.getLogMap());
            } else {
                insolvencyProcessor.processDelta(resourceChangedMessage);
                logger.info(format("Insolvency Links Delta message with contextId: %s is "
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
