package uk.gov.companieshouse.company.links.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.producer.InsolvencyStreamProducer;
import uk.gov.companieshouse.delta.ChsDelta;
import uk.gov.companieshouse.company.links.exception.RetryErrorException;

import java.util.Objects;


@Component
public class InsolvencyStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsolvencyStreamProcessor.class);
    private final InsolvencyStreamProducer insolvencyStreamProducer;

    @Autowired
    public InsolvencyStreamProcessor(InsolvencyStreamProducer insolvencyStreamProducer) {
        this.insolvencyStreamProducer = insolvencyStreamProducer;
    }

    //TODO What model should we use here? Is it a Avro?
    public void process(Message<ChsDelta> chsDelta) {
        try {
            MessageHeaders headers = chsDelta.getHeaders();
            final String receivedTopic = Objects.requireNonNull(headers.get(KafkaHeaders.RECEIVED_TOPIC)).toString();
            //TODO need to check where we set this property. We need to create a new one for this processor
            final boolean isRetry = headers.containsKey("INSOLVENCY_DELTA_RETRY_COUNT");
            final ChsDelta payload = chsDelta.getPayload();

        } catch (RetryErrorException ex) {
            retry(chsDelta);
        } catch (Exception ex) {
            handleErrors(chsDelta);
            // send to error topic
        }
    }

    public void retry(Message<ChsDelta> chsDelta) {

    }

    private void handleErrors(Message<ChsDelta> chsDelta) {

    }

}
