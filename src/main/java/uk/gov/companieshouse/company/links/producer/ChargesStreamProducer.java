package uk.gov.companieshouse.company.links.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.kafka.message.Message;
import uk.gov.companieshouse.kafka.producer.Acks;
import uk.gov.companieshouse.kafka.producer.CHKafkaProducer;
import uk.gov.companieshouse.kafka.producer.ProducerConfig;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutionException;

//TODO Do we need separate classes for insolvency and charges
@Component
public class ChargesStreamProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChargesStreamProducer.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private CHKafkaProducer chKafkaProducer;

    @PostConstruct
    public void init() {
        LOGGER.debug("Configuring CH Kafka producer");
        final ProducerConfig config = createProducerConfig();
        config.setRoundRobinPartitioner(true);
        config.setAcks(Acks.WAIT_FOR_ALL);
        config.setRetries(10);
        config.setRequestTimeoutMilliseconds(3000);
        chKafkaProducer = new CHKafkaProducer(config);
    }

    public void send(Message message) {
        try {
            chKafkaProducer.send(message);
        } catch (ExecutionException | InterruptedException ex) {
            LOGGER.error("Error while sending the message", ex);
        }
    }

    protected ProducerConfig createProducerConfig() {
        final ProducerConfig config = new ProducerConfig();
        config.setBrokerAddresses(bootstrapServers.split(","));
        return config;
    }
}
