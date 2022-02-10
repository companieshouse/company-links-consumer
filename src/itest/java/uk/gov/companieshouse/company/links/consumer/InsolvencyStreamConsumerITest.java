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
import uk.gov.companieshouse.delta.ChsDelta;

@SpringBootTest
@DirtiesContext
@Import(KafkaTestContainerConfig.class)
@ActiveProfiles({"test"})
public class InsolvencyStreamConsumerITest {

    @Autowired
    public KafkaTemplate<String, ChsDelta> kafkaTemplate;

    @Value("${insolvency.stream.topic.main}")
    private String mainTopic;

    @Test
    public void testSendingKafkaMessage() {
        ChsDelta chsDelta = new ChsDelta("{ \"key\": \"value\" }", 1, "some_id");
        kafkaTemplate.send(mainTopic, chsDelta);
    }
}
