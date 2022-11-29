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
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.processor.ExemptionsRouter;
import uk.gov.companieshouse.company.links.type.ResourceChange;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

public class ExemptionsStreamConsumer  {

    private final Logger logger;
    private final ExemptionsRouter exemptionsRouter;

    @Autowired
    public ExemptionsStreamConsumer(Logger logger, ExemptionsRouter exemptionsRouter) {
        this.logger = logger;
        this.exemptionsRouter = exemptionsRouter;
    }

    /**
     * Receives Main topic messages.
     */
    @RetryableTopic(attempts = "${company-links.consumer.exemptions.attempts}",
            backoff = @Backoff(delayExpression =
                    "${company-links.consumer.exemptions.backoff-delay}"),
            fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC,
            retryTopicSuffix = "-${company-links.consumer.exemptions.group-id}-retry",
            dltTopicSuffix = "-${company-links.consumer.exemptions.group-id}-error",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "false",
            exclude = NonRetryableErrorException.class)
    @KafkaListener(
            id = "${company-links.consumer.exemptions.topic}-consumer",
            topics = "${company-links.consumer.exemptions.topic}",
            groupId = "${company-links.consumer.exemptions.group-id}",
            autoStartup = "${company-links.consumer.exemptions.enable}",
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
                topic, partition, offset, contextId));

        try {
            exemptionsRouter.route(new ResourceChange(payload));
            logger.info(format("Company exemptions message with contextId: %s is "
                            + "successfully processed in %d milliseconds", contextId,
                    Duration.between(startTime, Instant.now()).toMillis()));
        } catch (Exception exception) {
            logger.errorContext(contextId, format("Exception occurred while processing "
                    + "message on the topic: %s", topic), exception, null);
            throw exception;
        }
    }

}
