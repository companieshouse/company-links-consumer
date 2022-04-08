package uk.gov.companieshouse.company.links.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.FileCopyUtils;
import uk.gov.companieshouse.company.links.config.KafkaTestContainerConfig;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Objects;

@SpringBootTest
@DirtiesContext
@Import(KafkaTestContainerConfig.class)
@ActiveProfiles({"test"})
 class InsolvencyStreamConsumerITest {

    @Autowired
    public KafkaTemplate<String, ResourceChangedData> kafkaTemplate;

    @Value("${company-links.consumer.insolvency.topic.main}")
    private String mainTopic;

    @Test
    void testSendingKafkaMessage() throws IOException {
        EventRecord event = EventRecord.newBuilder()
                .setType("changed")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        String insolvencyData = getInsolvencyData("insolvency-record.json");

        ResourceChangedData resourceChanged = ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId("12345678")
                .setResourceKind("company-insolvency")
                .setResourceUri("/company/12345678/insolvency")
                .setData(insolvencyData)
                .setEvent(event)
                .build();

        kafkaTemplate.send(mainTopic, resourceChanged);
    }

    private String getInsolvencyData(String filename) throws IOException {
        InputStreamReader exampleChargesJsonPayload = new InputStreamReader(
                Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                        .getResourceAsStream(filename)));
        return FileCopyUtils.copyToString(exampleChargesJsonPayload);
    }
}
