package uk.gov.companieshouse.company.links.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import uk.gov.companieshouse.company.links.config.KafkaTestContainerConfig;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.util.Arrays;

@SpringBootTest
@DirtiesContext
@Import(KafkaTestContainerConfig.class)
@ActiveProfiles({"test"})
 class InsolvencyStreamConsumerITest {

    @Autowired
    public KafkaTemplate<String, ResourceChangedData> kafkaTemplate;

    @Value("${company-links.consumer.insolvency.topic.main}")
    private String mainTopic;

    void testSendingKafkaMessage() {
        EventRecord event = EventRecord.newBuilder()
                .setType("changed")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        ResourceChangedData resourceChanged = ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId("12345678")
                .setResourceKind("company-insolvency")
                .setResourceUri("/company/12345678/insolvency")
                .setData("{ \"key\": \"value\" }")
                .setEvent(event)
                .build();

        kafkaTemplate.send(mainTopic, resourceChanged);
    }
}
