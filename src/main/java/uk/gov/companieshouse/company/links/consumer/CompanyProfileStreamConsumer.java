package uk.gov.companieshouse.company.links.consumer;

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
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class CompanyProfileStreamConsumer {

    private final Logger logger;
    private final LinkRouter companyProfileRouter;

    public CompanyProfileStreamConsumer(Logger logger, LinkRouter companyProfileRouter) {
        this.logger = logger;
        this.companyProfileRouter = companyProfileRouter;
    }

    /**
     * Receives Main topic messages.
     */
    @RetryableTopic(
            attempts = "${company-links.consumer.company-profile.attempts}",
            backoff = @Backoff(delayExpression =
                    "${company-links.consumer.company-profile.backoff-delay}"),
            fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC,
            retryTopicSuffix = "-${company-links.consumer.company-profile.group-id}-retry",
            dltTopicSuffix = "-${company-links.consumer.company-profile.group-id}-error",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "false",
            exclude = NonRetryableErrorException.class)
    @KafkaListener(
            id = "${company-links.consumer.company-profile.topic}-consumer",
            topics = "${company-links.consumer.company-profile.topic}",
            groupId = "${company-links.consumer.company-profile.group-id}",
            autoStartup = "${company-links.consumer.company-profile.enable}",
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
    }
}
