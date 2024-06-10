package uk.gov.companieshouse.company.links.steps;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.ResourceUtils;
import uk.gov.companieshouse.company.links.config.WiremockTestConfig;
import uk.gov.companieshouse.delta.ChsDelta;
import uk.gov.companieshouse.logging.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static org.assertj.core.api.Assertions.assertThat;

public class RetrySteps {

    private static WireMockServer wireMockServer;
    private String output;

    @Autowired
    private Logger logger;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;

    @When("the consumer receives a message with company number {string} but the api returns a {int}")
    public void theConsumerReceivesMessageButDataApiReturns(String companyNumber, int responseCode) throws Exception{
        WiremockTestConfig.stubGetCompanyProfile(companyNumber, responseCode,
                "profile-with-all-links");
        stubPatchOfficersCompanyProfile(companyNumber, responseCode);

        ChsDelta delta = new ChsDelta(loadFileFromName("profile-data-with-all-links.json"), 1, "1", false);
        kafkaTemplate.send("stream-company-officers", delta);

        countDown();
    }

    @Then("^the message should retry (\\d*) times and then error$")
    public void theMessageShouldRetryAndError(int retries) {
        ConsumerRecords<String, Object> records = KafkaTestUtils.getRecords(kafkaConsumer);
        Iterable<ConsumerRecord<String, Object>> retryRecords =  records.records("stream-company-officers-retry");
        Iterable<ConsumerRecord<String, Object>> errorRecords =  records.records("stream-company-officers-error");

        int actualRetries = (int) StreamSupport.stream(retryRecords.spliterator(), false).count();
        int errors = (int) StreamSupport.stream(errorRecords.spliterator(), false).count();

        assertThat(actualRetries).isEqualTo(retries);
        assertThat(errors).isEqualTo(1);
    }

    private String loadFileFromName(String fileName) {
        try {
            return FileUtils.readFileToString(ResourceUtils.getFile("classpath:stubs/"+fileName), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unable to locate file %s", fileName));
        }
    }

    private void countDown() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    public static void stubPatchOfficersCompanyProfile(String companyNumber, int statusCode) {
        stubFor(
                patch(urlEqualTo("/company/" + companyNumber + "/links"))
                        .withRequestBody(containing("/company/" + companyNumber + "/officers"))
                        .willReturn(aResponse()
                                .withStatus(statusCode)));
    }
}
