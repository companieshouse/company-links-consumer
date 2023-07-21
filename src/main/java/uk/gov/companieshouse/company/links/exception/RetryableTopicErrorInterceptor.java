package uk.gov.companieshouse.company.links.exception;

import static java.lang.String.format;
import static org.springframework.kafka.support.KafkaHeaders.EXCEPTION_CAUSE_FQCN;
import static org.springframework.kafka.support.KafkaHeaders.EXCEPTION_STACKTRACE;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import uk.gov.companieshouse.company.links.config.LoggingConfig;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;

public class RetryableTopicErrorInterceptor implements ProducerInterceptor<String, Object> {

    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> message) {
        String nextTopic = message.topic().contains("-error") ? getNextErrorTopic(message)
                : message.topic();
        LoggingConfig.getLogger().info(format("Moving record into new topic: %s with value: %s",
                nextTopic, message.value()), DataMapHolder.getLogMap());
        if (nextTopic.contains("-invalid")) {
            return new ProducerRecord<>(nextTopic, message.key(), message.value());
        }

        return message;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception ex) {
        // Do nothing
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public void configure(Map<String, ?> map) {
        // Do nothing
    }

    private String getNextErrorTopic(ProducerRecord<String, Object> message) {
        Header header1 = message.headers().lastHeader(EXCEPTION_CAUSE_FQCN);
        Header header2 = message.headers().lastHeader(EXCEPTION_STACKTRACE);
        return ((header1 != null
                && new String(header1.value()).contains(NonRetryableErrorException.class.getName()))
                || (header2 != null
                && new String(header2.value()).contains(
                NonRetryableErrorException.class.getName())))
                ? message.topic().replace("-error", "-invalid") : message.topic();
    }
}
