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
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;

@SpringBootTest
@DirtiesContext
@Import(KafkaTestContainerConfig.class)
@ActiveProfiles({"test"})
class ChargesStreamConsumerITest {

    @Autowired
    public KafkaTemplate<String, ResourceChangedData> kafkaTemplate;

    @Value("${company-links.consumer.charges.topic}")
    private String mainTopic;

    private TestData testData = new TestData();

    @Test
    void testSendingKafkaMessage() throws IOException {

        ResourceChangedData resourceChanged = testData.
                getResourceChangedData("charges-record.json");

        kafkaTemplate.send(mainTopic, resourceChanged);
    }

}
