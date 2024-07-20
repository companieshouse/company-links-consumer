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
import uk.gov.companieshouse.company.links.processor.RegistersStreamProcessor;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class RegistersStreamConsumer {

    private final RegistersStreamProcessor registersProcessor;
    private final Logger logger;

    @Autowired
    public RegistersStreamConsumer(RegistersStreamProcessor registersProcessor, Logger logger) {
        this.registersProcessor = registersProcessor;
        this.logger = logger;
    }

    /**
     * Receives Main topic messages.
     */
    @RetryableTopic(attempts = "${company-links.consumer.registers.attempts}",
            backoff = @Backoff(delayExpression =
                    "${company-links.consumer.registers.backoff-delay}"),
            fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC,
            retryTopicSuffix = "-${company-links.consumer.registers.group-id}-retry",
            dltTopicSuffix = "-${company-links.consumer.registers.group-id}-error",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "false",
            exclude = NonRetryableErrorException.class)
    @KafkaListener(
            id = "${company-links.consumer.registers.topic}-consumer",
            topics = "${company-links.consumer.registers.topic}",
            groupId = "${company-links.consumer.registers.group-id}",
            autoStartup = "${company-links.consumer.registers.enable}",
            containerFactory = "listenerContainerFactory")
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
                registersProcessor.processDelete(resourceChangedMessage);
                logger.info(format("Registers Links Delete message with contextId: %s is "
                                        + "successfully processed in %d milliseconds", contextId,
                                Duration.between(startTime, Instant.now()).toMillis()),
                        DataMapHolder.getLogMap());
            } else {
                registersProcessor.processDelta(resourceChangedMessage);
                logger.info(format("Registers Links Delta message with contextId: %s is "
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
