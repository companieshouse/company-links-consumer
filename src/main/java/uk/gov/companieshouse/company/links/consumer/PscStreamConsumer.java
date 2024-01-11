package uk.gov.companieshouse.company.links.consumer;

import java.time.Duration;
import java.time.Instant;
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
import uk.gov.companieshouse.company.links.processor.LinkRouter;
import uk.gov.companieshouse.company.links.type.ResourceChange;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class PscStreamConsumer {
    private final Logger logger;
    private final LinkRouter pscRouter;

    public PscStreamConsumer(Logger logger, LinkRouter pscRouter) {
        this.logger = logger;
        this.pscRouter = pscRouter;
    }

    /**
     * Receives Main topic messages.
     */
    @RetryableTopic(attempts = "${company-links.consumer.pscs.attempts}",
            backoff = @Backoff(delayExpression =
                    "${company-links.consumer.pscs.backoff-delay}"),
            fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC,
            retryTopicSuffix = "-${company-links.consumer.pscs.group-id}-retry",
            dltTopicSuffix = "-${company-links.consumer.pscs.group-id}-error",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "false",
            exclude = NonRetryableErrorException.class)
    @KafkaListener(
            id = "${company-links.consumer.pscs.topic}-consumer",
            topics = "${company-links.consumer.pscs.topic}",
            groupId = "${company-links.consumer.pscs.group-id}",
            autoStartup = "${company-links.consumer.pscs.enable}",
            containerFactory = "listenerContainerFactory")
    public void receive(Message<ResourceChangedData> resourceChangedMessage,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partition,
                        @Header(KafkaHeaders.OFFSET) String offset) {
        Instant startTime = Instant.now();
        ResourceChangedData payload = resourceChangedMessage.getPayload();
        String contextId = payload.getContextId();
        logger.info(String.format("A new message successfully picked up from topic: %s, "
                        + "partition: %s and offest %s with contextId: %s",
                topic, partition, offset, contextId));
        try {
            pscRouter.route(new ResourceChange(payload), "pscs");
            logger.info(String.format("PSC message with contextId: %s is "
                            + "successfully processed in %d milliseconds",
                    contextId, Duration.between(startTime, Instant.now()).toMillis()));
        } catch (Exception ex) {
            logger.errorContext(contextId, String.format("Exception occurred while processing "
                    + "message on the topic: %s", topic), ex, null);
            throw ex;
        }
    }
}
