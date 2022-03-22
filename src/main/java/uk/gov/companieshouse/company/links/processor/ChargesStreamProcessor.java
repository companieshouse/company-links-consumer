package uk.gov.companieshouse.company.links.processor;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.exception.RetryErrorException;
import uk.gov.companieshouse.company.links.producer.ChargesStreamProducer;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class ChargesStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChargesStreamProcessor.class);
    private final ChargesStreamProducer chargesStreamProducer;

    /**
     * Construct an insolvency stream processor.
     */
    @Autowired
    public ChargesStreamProcessor(ChargesStreamProducer chargesStreamProducer) {
        this.chargesStreamProducer = chargesStreamProducer;
    }

    /**
     * Process a ResourceChangedData message.
     */
    public void process(Message<ResourceChangedData> resourceChangedMessage) {
        try {
            MessageHeaders headers = resourceChangedMessage.getHeaders();
            final String receivedTopic =
                    Objects.requireNonNull(headers.get(KafkaHeaders.RECEIVED_TOPIC)).toString();
            //TODO need to check where we set this property.
            //TODO We need to create a new one for this processor
            final boolean isRetry = headers.containsKey("INSOLVENCY_DELTA_RETRY_COUNT");

        } catch (RetryErrorException ex) {
            retry(resourceChangedMessage);
        } catch (Exception ex) {
            handleErrors(resourceChangedMessage);
            // send to error topic
        }
    }

    @Override
    public void retry(Message<ResourceChangedData> resourceChangedMessage) {
        // Retry functionality added in a future ticket
    }

    @Override
    private void handleErrors(Message<ResourceChangedData> resourceChangedMessage) {
        // Error functionality added in a future ticket
    }

}
