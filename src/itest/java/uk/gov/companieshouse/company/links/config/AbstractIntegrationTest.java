package uk.gov.companieshouse.company.links.config;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(KafkaTestContainerConfig.class)
@ActiveProfiles({"test"})
public abstract class AbstractIntegrationTest {
}
