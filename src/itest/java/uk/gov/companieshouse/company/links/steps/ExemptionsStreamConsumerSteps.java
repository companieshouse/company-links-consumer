package uk.gov.companieshouse.company.links.steps;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.ResourceUtils;
import uk.gov.companieshouse.company.links.consumer.ResettableCountDownLatch;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.companieshouse.company.links.config.WiremockTestConfig.setupWiremock;
import static uk.gov.companieshouse.company.links.data.TestData.RESOURCE_KIND_EXEMPTIONS;

public class ExemptionsStreamConsumerSteps {
    private static final int CONSUME_MESSAGE_TIMEOUT = 5;
    private static final long GET_RECORDS_TIMEOUT = 5000L;

    @Value("${company-links.consumer.exemptions.topic}")
    private String mainTopic;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;

    @Autowired
    private ResettableCountDownLatch resettableCountDownLatch;

    @BeforeEach
    public void beforeEach() {
        resettableCountDownLatch.resetLatch(4);
    }

    @Given("Company links consumer is available")
    public void companyLinksConsumerIsRunning() {
        setupWiremock();
    }

    @And("The response code {int} will be returned from the PATCH request for {string}")
    public void stubPatchExemptionsLink(int responseCode, String companyNumber) {
        stubPatchLink(companyNumber, responseCode);
    }

    @When("A valid message is consumed for {string}")
    public void consumeValidMessage(String companyNumber) throws InterruptedException {
        kafkaTemplate.send(mainTopic, createValidMessage(companyNumber, RESOURCE_KIND_EXEMPTIONS));
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @When("An invalid message is consumed")
    public void consumeInvalidMessage() throws InterruptedException {
        kafkaTemplate.send(mainTopic, "invalid message");
        kafkaTemplate.flush();
    }

    @When("A message is consumed for {string} with invalid event type")
    public void consumeValidMessageWithInvalidEventType(String companyNumber) throws InterruptedException {
        kafkaTemplate.send(mainTopic, createMessageWithInvalidEventType(companyNumber, RESOURCE_KIND_EXEMPTIONS));
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @Then("A PATCH request is sent to the add company exemptions link endpoint for {string}")
    public void verifyPatchEndpointIsCalled(String companyNumber) {
        verify(1, patchRequestedFor(urlEqualTo(String.format("/company/%s/links/exemptions", companyNumber))));
    }

    @Then("The message is placed on the appropriate topic: {string}")
    public void verifyMessageIsPlaceOnCorrectTopic(String topic) {
        String requiredTopic = String.format("stream-company-exemptions-company-links-consumer-%s", topic);

        ConsumerRecords<String, Object> records = KafkaTestUtils.getRecords(kafkaConsumer, GET_RECORDS_TIMEOUT);
        String actualTopic = records.records(requiredTopic).iterator().next().topic();
        assertThat(actualTopic).isEqualTo(requiredTopic);
    }

    private void stubPatchLink(String companyNumber, int responseCode) {
        stubFor(
                patch(urlEqualTo("/company/" + companyNumber + "/links/exemptions"))
                        .willReturn(aResponse()
                                .withStatus(responseCode)));
    }

    private void assertMessageConsumed() throws InterruptedException {
        assertThat(resettableCountDownLatch.getCountDownLatch()
                .await(CONSUME_MESSAGE_TIMEOUT, TimeUnit.SECONDS))
                .isTrue();
    }

    private ResourceChangedData createValidMessage(String companyNumber, String kind) {
        EventRecord event = EventRecord.newBuilder()
                .setType("changed")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        return ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(companyNumber)
                .setResourceKind(kind)
                .setResourceUri(String.format("/company/%s/exemptions", companyNumber))
                .setData("{ \"key\": \"value\" }")
                .setEvent(event)
                .build();
    }

    private ResourceChangedData createMessageWithInvalidEventType(String companyNumber, String kind) {
        EventRecord event = EventRecord.newBuilder()
                .setType("invalid")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        return ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(companyNumber)
                .setResourceKind(kind)
                .setResourceUri(String.format("/company/%s/exemptions", companyNumber))
                .setData("")
                .setEvent(event)
                .build();
    }

    private String loadFileForCoNumber(String fileName, String companyNumber) {
        try {
            String templateText = FileUtils.readFileToString(ResourceUtils.getFile("classpath:stubs/"+fileName), StandardCharsets.UTF_8);
            return String.format(templateText, companyNumber, companyNumber); // extra args are ignored
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unable to locate file %s", fileName));
        }
    }
}