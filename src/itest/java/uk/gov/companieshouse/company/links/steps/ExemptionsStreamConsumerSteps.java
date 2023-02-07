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
    private static final String COMPANY_NUMBER = "00006400";

    private int statusCode;

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
        statusCode = 200;
    }

    @Given("Company links consumer is available")
    public void companyLinksConsumerIsRunning() {
        setupWiremock();
    }

    @And("The user is unauthorized")
    public void stubUnauthorizedPatchRequest() {
        statusCode = 401;
    }

    @And("The company profile api is unavailable")
    public void stubServiceUnavailablePatchRequest() {
        statusCode = 503;
    }

    @When("A valid message is consumed")
    public void consumeValidMessage() throws InterruptedException {
        stubPatchLink(COMPANY_NUMBER, statusCode);
        kafkaTemplate.send(mainTopic, createValidMessage(COMPANY_NUMBER, RESOURCE_KIND_EXEMPTIONS));
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @When("An invalid message is consumed")
    public void consumeInvalidMessage() throws InterruptedException {
        kafkaTemplate.send(mainTopic, "invalid message");
        kafkaTemplate.flush();
    }

    @When("A message is consumed with invalid event type")
    public void consumeValidMessageWithInvalidEventType() throws InterruptedException {
        kafkaTemplate.send(mainTopic, createMessageWithInvalidEventType(COMPANY_NUMBER, RESOURCE_KIND_EXEMPTIONS));
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @Then("A PATCH request is sent to the API")
    public void verifyPatchEndpointIsCalled() {
        verify(1, patchRequestedFor(urlEqualTo(String.format("/company/%s/links/exemptions", COMPANY_NUMBER))));
    }

    @Then("No messages are placed on the invalid, error or retry topics")
    public void verifyConsumerRecordsAreEmpty() {
        ConsumerRecords<String, Object> records = KafkaTestUtils.getRecords(kafkaConsumer, GET_RECORDS_TIMEOUT);
        assertThat(records).isEmpty();
    }

    @Then("The message is placed on the {string} topic")
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
}