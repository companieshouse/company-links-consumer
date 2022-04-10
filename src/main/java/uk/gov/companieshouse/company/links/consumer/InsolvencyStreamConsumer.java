package uk.gov.companieshouse.company.links.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.FixedDelayStrategy;
import org.springframework.messaging.Message;
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
    public final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Default constructor.
     */
    @Autowired
    public InsolvencyStreamConsumer(InsolvencyStreamProcessor insolvencyProcessor,
                                    KafkaTemplate<String, Object> kafkaTemplate) {
        this.insolvencyProcessor = insolvencyProcessor;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Receives Main topic messages.
     */
    @RetryableTopic(attempts = "${company-links.consumer.insolvency.attempts}",
            backoff = @Backoff(delayExpression = "${company-links.consumer.insolvency."
                    + "backoff-delay}"),
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
            containerFactory = "listenerContainerFactory"
            )
    public void receive(Message<ResourceChangedData> message) {
        logger.trace(String.format("A new message with payload:%s  ", message.getPayload()));
        try {
            insolvencyProcessor.process(message);
        } catch (Exception exception) {
            logger.error(String.format("Exception occurred while processing the message %s, "
                    + "exception thrown is %s", message, exception));
            throw exception;
        }
    }

}
