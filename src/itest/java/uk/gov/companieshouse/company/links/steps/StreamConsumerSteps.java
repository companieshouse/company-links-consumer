package uk.gov.companieshouse.company.links.steps;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import uk.gov.companieshouse.company.links.consumer.ResettableCountDownLatch;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.patchRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.companieshouse.company.links.config.WiremockTestConfig.setupWiremock;

public class StreamConsumerSteps {
    private static final int CONSUME_MESSAGE_TIMEOUT = 5;
    private static final long GET_RECORDS_TIMEOUT = 5000L;
    private static final String COMPANY_NUMBER = "00006400";
    private static final String RETRY_TOPIC_ATTEMPTS_KEY = "retry_topic-attempts";
    private static final int RETRY_ATTEMPTS = 4;
    private int statusCode;
    private String patchUrl;
    private String mainTopic;
    private String topicPrefix;
    private String deltaType;
    private String resourceUri;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;

    @Autowired
    private ResettableCountDownLatch resettableCountDownLatch;

    @Given("Company links consumer is available")
    public void companyLinksConsumerIsRunning() {
        resettableCountDownLatch.resetLatch(4);
        statusCode = 200;
        setupWiremock();
        kafkaConsumer.poll(Duration.ofSeconds(1));
    }

    @And("The user is unauthorized")
    public void stubUnauthorizedPatchRequest() {
        statusCode = 401;
    }

    @And("The company profile api is unavailable")
    public void stubServiceUnavailablePatchRequest() {
        statusCode = 503;
    }

    @When("A valid {string} message is consumed from the {string} stream")
    public void consumeValidMessage(String eventType, String deltaType) throws InterruptedException {
        this.deltaType = deltaType;
        initialiseVariablesToDeltaType();

        stubPatchLink(statusCode, eventType);
        kafkaTemplate.send(mainTopic, createValidMessage(eventType));
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @When("An invalid message is consumed from the {string} stream")
    public void consumeInvalidMessage(String deltaType) throws InterruptedException {
        this.deltaType = deltaType;
        initialiseVariablesToDeltaType();

        kafkaTemplate.send(mainTopic, "invalid message");
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @When("A message is consumed with invalid event type from the {string} stream")
    public void consumeValidMessageWithInvalidEventType(String deltaType) throws InterruptedException {
        this.deltaType = deltaType;
        initialiseVariablesToDeltaType();

        kafkaTemplate.send(mainTopic, createMessageWithInvalidEventType());
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @Then("A PATCH request is sent to the API")
    public void verifyAddPatchEndpointIsCalled() {
        verify(1, patchRequestedFor(urlEqualTo(patchUrl)));
    }

    @Then("No messages are placed on the invalid, error or retry topics")
    public void verifyConsumerRecordsAreEmpty() {
        ConsumerRecords<String, Object> records = KafkaTestUtils.getRecords(kafkaConsumer, GET_RECORDS_TIMEOUT);
        assertThat(records).isEmpty();
    }

    @Then("The message is placed on the {string} topic")
    public void verifyMessageIsPlaceOnCorrectTopic(String topicSuffix) {
        String requiredTopic = String.format("%s-%s", topicPrefix, topicSuffix);

        switch (topicSuffix) {
            case "invalid":
                ConsumerRecord<String, Object> invalidRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, requiredTopic, GET_RECORDS_TIMEOUT);
                String invalidTopic = invalidRecord.topic();
                assertThat(invalidTopic).isEqualTo(requiredTopic);
                break;
            case "retry":
                ConsumerRecords<String, Object> retryRecord = KafkaTestUtils.getRecords(kafkaConsumer, GET_RECORDS_TIMEOUT);
                String retryTopic = retryRecord.records(requiredTopic).iterator().next().topic();
                assertThat(retryTopic).isEqualTo(requiredTopic);
                break;
            case "error":
                ConsumerRecord<String, Object> errorRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, requiredTopic, GET_RECORDS_TIMEOUT);
                assertThat(errorRecord.topic()).isEqualTo(requiredTopic);

                List<Header> retryList = StreamSupport.stream(errorRecord.headers().spliterator(), false)
                        .filter(header -> header.key().equalsIgnoreCase(RETRY_TOPIC_ATTEMPTS_KEY))
                        .collect(Collectors.toList());
                assertThat(retryList.size()).isEqualTo(RETRY_ATTEMPTS);
                break;
        }
    }

    private void stubPatchLink(int responseCode, String eventType) {
        switch (eventType) {
            case "changed":
                patchUrl = String.format("/company/%s/links/%s", COMPANY_NUMBER, deltaType);
                break;
            case "deleted":
                patchUrl = String.format("/company/%s/links/%s/delete", COMPANY_NUMBER, deltaType);
                break;
        }

        stubFor(
                patch(urlEqualTo(patchUrl))
                        .willReturn(aResponse()
                                .withStatus(responseCode)));
    }

    private void assertMessageConsumed() throws InterruptedException {
        assertThat(resettableCountDownLatch.getCountDownLatch()
                .await(CONSUME_MESSAGE_TIMEOUT, TimeUnit.SECONDS))
                .isTrue();
    }

    private ResourceChangedData createValidMessage(String eventType) {
        EventRecord event = EventRecord.newBuilder()
                .setType(eventType)
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        return ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(COMPANY_NUMBER)
                .setResourceKind(deltaType)
                .setResourceUri(resourceUri)
                .setData("{ \"key\": \"value\" }")
                .setEvent(event)
                .build();
    }

    private ResourceChangedData createMessageWithInvalidEventType() {
        EventRecord event = EventRecord.newBuilder()
                .setType("invalid")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        return ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(COMPANY_NUMBER)
                .setResourceKind(deltaType)
                .setResourceUri(resourceUri)
                .setData("")
                .setEvent(event)
                .build();
    }

    private void initialiseVariablesToDeltaType() {
        mainTopic = String.format("stream-company-%s", deltaType);
        topicPrefix = String.format("%s-company-links-consumer", mainTopic);
        resourceUri = String.format("company/%s/%s", COMPANY_NUMBER, deltaType);
    }
}