package uk.gov.companieshouse.company.links.consumer;

import static java.lang.String.format;

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
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.processor.LinkRouter;
import uk.gov.companieshouse.company.links.type.ResourceChange;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class FilingHistoryStreamConsumer {

    private final Logger logger;
    private final LinkRouter router;

    public FilingHistoryStreamConsumer(Logger logger, LinkRouter router) {
        this.logger = logger;
        this.router = router;
    }

    /**
     * Receives Main topic messages.
     */
    @RetryableTopic(attempts = "${company-links.consumer.filing_history.attempts}",
            backoff = @Backoff(delayExpression =
                    "${company-links.consumer.filing_history.backoff-delay}"),
            fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC,
            retryTopicSuffix = "-${company-links.consumer.filing_history.group-id}-retry",
            dltTopicSuffix = "-${company-links.consumer.filing_history.group-id}-error",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "false",
            exclude = NonRetryableErrorException.class)
    @KafkaListener(
            id = "${company-links.consumer.filing_history.topic}-consumer",
            topics = "${company-links.consumer.filing_history.topic}",
            groupId = "${company-links.consumer.filing_history.group-id}",
            autoStartup = "${company-links.consumer.filing_history.enable}",
            containerFactory = "listenerContainerFactory")
    public void receive(Message<ResourceChangedData> resourceChangedMessage,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partition,
            @Header(KafkaHeaders.OFFSET) String offset) {
        Instant startTime = Instant.now();
        ResourceChangedData payload = resourceChangedMessage.getPayload();
        String contextId = payload.getContextId();
        logger.info("Message read from topic", DataMapHolder.getLogMap());

        try {
            router.route(new ResourceChange(payload), "filing-history");
            logger.info(format("Company filing history message successfully processed in "
                                    + "%d milliseconds",
                            Duration.between(startTime, Instant.now()).toMillis()),
                    DataMapHolder.getLogMap());
        } catch (Exception exception) {
            logger.errorContext(contextId, format("Exception occurred while processing "
                            + "message on topic: %s", topic),
                    exception, DataMapHolder.getLogMap());
            throw exception;
        }
    }
}
