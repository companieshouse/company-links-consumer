package uk.gov.companieshouse.company.links.config;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import uk.gov.companieshouse.company.links.consumer.KafkaMessageConsumerAspect;
import uk.gov.companieshouse.company.links.consumer.ResettableCountDownLatch;
import uk.gov.companieshouse.company.links.exception.RetryableTopicErrorInterceptor;
import uk.gov.companieshouse.company.links.serialization.ResourceChangedDataDeserializer;
import uk.gov.companieshouse.company.links.serialization.ResourceChangedDataSerializer;
import uk.gov.companieshouse.stream.ResourceChangedData;

@TestConfiguration
public class KafkaTestContainerConfig {

    private final ResourceChangedDataSerializer resourceChangedDataSerializer;
    private final ResourceChangedDataDeserializer resourceChangedDataDeserializer;

    @Autowired
    public KafkaTestContainerConfig(ResourceChangedDataSerializer resourceChangedDataSerializer,
                                    ResourceChangedDataDeserializer resourceChangedDataDeserializer) {
        this.resourceChangedDataSerializer = resourceChangedDataSerializer;
        this.resourceChangedDataDeserializer = resourceChangedDataDeserializer;
    }

    @Bean
    public KafkaContainer kafkaContainer() {
        KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
        kafkaContainer.setWaitStrategy(Wait.defaultWaitStrategy()
                .withStartupTimeout(Duration.of(300, SECONDS)));
        kafkaContainer.start();
        return kafkaContainer;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, ResourceChangedData> listenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ResourceChangedData> factory
                = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory());
        factory.getContainerProperties().setIdleBetweenPolls(0);
        factory.getContainerProperties().setPollTimeout(10L);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        return factory;
    }

    @Bean
    public ConsumerFactory<String, ResourceChangedData> kafkaConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(kafkaContainer()), new StringDeserializer(),
                new ErrorHandlingDeserializer<>(resourceChangedDataDeserializer));
    }

    @Bean
    public Map<String, Object> consumerConfigs(KafkaContainer kafkaContainer) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "company-links-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, ResourceChangedDataDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return props;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory(KafkaContainer kafkaContainer) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ResourceChangedDataSerializer.class);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                RetryableTopicErrorInterceptor.class.getName());

        return new DefaultKafkaProducerFactory<>(
                props, new StringSerializer(), resourceChangedDataSerializer);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory(kafkaContainer()));
    }

    @Bean
    public KafkaConsumer<String, Object> invalidTopicConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer().getBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "company-links-test-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("stream-company-insolvency-company-links-consumer-invalid",
                "stream-company-insolvency-company-links-consumer-error",
                "stream-company-charges-company-links-consumer-invalid",
                "stream-company-charges-company-links-consumer-error",
                "stream-company-exemptions-company-links-consumer-error",
                "stream-company-exemptions-company-links-consumer-invalid",
                "stream-company-exemptions-company-links-consumer-retry",
                "stream-company-officers-company-links-consumer-error",
                "stream-company-officers-company-links-consumer-invalid",
                "stream-company-officers-company-links-consumer-retry",
                "stream-psc-statements-company-links-consumer-error",
                "stream-psc-statements-company-links-consumer-invalid",
                "stream-psc-statements-company-links-consumer-retry",
                "stream-company-psc-company-links-consumer-error",
                "stream-company-psc-company-links-consumer-invalid",
                "stream-company-psc-company-links-consumer-retry",
                "stream-filing-history-company-links-consumer-error",
                "stream-filing-history-company-links-consumer-invalid",
                "stream-filing-history-company-links-consumer-retry"));
        return consumer;
    }

    @Bean
    public KafkaMessageConsumerAspect kafkaMessageConsumerAspect(ResettableCountDownLatch resettableCountDownLatch) {
        return new KafkaMessageConsumerAspect(resettableCountDownLatch);
    }
}