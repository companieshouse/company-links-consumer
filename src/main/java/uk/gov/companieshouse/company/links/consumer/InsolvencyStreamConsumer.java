package uk.gov.companieshouse.company.links.consumer;

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
import uk.gov.companieshouse.company.links.processor.InsolvencyStreamProcessor;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class InsolvencyStreamConsumer {

    private final InsolvencyStreamProcessor insolvencyProcessor;
    private static final Logger logger = LoggerFactory.getLogger("InsolvencyStreamConsumer");

    @Autowired
    public InsolvencyStreamConsumer(InsolvencyStreamProcessor insolvencyProcessor) {
        this.insolvencyProcessor = insolvencyProcessor;
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
    public void receive(Message<ResourceChangedData> resourceChangedMessage,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partition,
                        @Header(KafkaHeaders.OFFSET) String offset) {
        logger.info(
                "A new message read from Stream-insolvency topic with payload: "
                        + resourceChangedMessage.getPayload());
        try {
            insolvencyProcessor.process(resourceChangedMessage);
        } catch (Exception exception) {
            logger.error(String.format("Exception occurred while processing the topic %s "
                    + "with message %s, exception thrown is %s",
                    topic, resourceChangedMessage, exception));
            throw exception;
        }
    }

}
