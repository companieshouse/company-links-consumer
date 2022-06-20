package uk.gov.companieshouse.company.links;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import uk.gov.companieshouse.company.links.config.KafkaTestContainerConfig;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(KafkaTestContainerConfig.class)
@ActiveProfiles({"test"})
public abstract class AbstractIntegrationTest {
}
