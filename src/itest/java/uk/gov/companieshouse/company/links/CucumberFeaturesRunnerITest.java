package uk.gov.companieshouse.company.links;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import io.cucumber.spring.CucumberContextConfiguration;
import org.junit.runner.RunWith;
import uk.gov.companieshouse.company.links.config.AbstractIntegrationTest;

@RunWith(Cucumber.class)
@CucumberOptions(
        features = "src/itest/resources/features/company-links-charges-error-retry.feature",
        plugin = {"pretty",
                "json:target/cucumber-report.json"
        },
        glue = {"uk.gov.companieshouse.company.links"})
@CucumberContextConfiguration
public class
CucumberFeaturesRunnerITest extends AbstractIntegrationTest {
}
