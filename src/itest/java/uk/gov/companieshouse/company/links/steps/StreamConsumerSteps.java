package uk.gov.companieshouse.company.links.steps;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.patchRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.companieshouse.company.links.config.WiremockTestConfig.setupWiremock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import uk.gov.companieshouse.api.appointment.ItemLinkTypes;
import uk.gov.companieshouse.api.appointment.OfficerList;
import uk.gov.companieshouse.api.appointment.OfficerSummary;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.company.links.config.CucumberContext;
import uk.gov.companieshouse.company.links.consumer.ResettableCountDownLatch;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

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

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TestRestTemplate restTemplate;

    @Given("Company links consumer is available")
    public void companyLinksConsumerIsRunning() {
        resettableCountDownLatch.resetLatch(4);
        statusCode = HttpStatus.OK.value();
        setupWiremock();
        kafkaConsumer.poll(Duration.ofSeconds(1));
    }

    @And("The user is unauthorized")
    public void stubUnauthorizedPatchRequest() {
        statusCode = HttpStatus.UNAUTHORIZED.value();
    }

    @And("The company profile api is unavailable")
    public void stubServiceUnavailablePatchRequest() {
        statusCode = HttpStatus.SERVICE_UNAVAILABLE.value();
    }

    @When("A valid {string} message is consumed from the {string} stream")
    public void consumeValidMessage(String eventType, String deltaType) throws InterruptedException {
        this.deltaType = deltaType;
        initialiseVariablesUsingDeltaType();

        stubPatchLink(statusCode, eventType);
        kafkaTemplate.send(mainTopic, createValidMessage(eventType));
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @When("A valid {string} message consumed causes a conflict from the {string} stream")
    public void consumeValidMessageAndThrowsAConflictException(String eventType, String deltaType) throws InterruptedException {
        this.deltaType = deltaType;
        initialiseVariablesUsingDeltaType();

        stubPatchLink(HttpStatus.CONFLICT.value(), eventType);
        kafkaTemplate.send(mainTopic, createValidMessage(eventType));
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @When("An invalid message is consumed from the {string} stream")
    public void consumeInvalidMessage(String deltaType) throws InterruptedException {
        this.deltaType = deltaType;
        initialiseVariablesUsingDeltaType();

        kafkaTemplate.send(mainTopic, "invalid message");
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @When("I send GET request with company number {string}")
    public void i_send_get_request_with_company_number(String companyNumber) throws InterruptedException {
        String uri = "/company/{company_number}/links/persons-with-significant-control";

        HttpHeaders headers = new HttpHeaders();
        headers.add("ERIC-Identity", "SOME_IDENTITY");
        headers.add("ERIC-Identity-Type", "key");

        ResponseEntity<Data> response = restTemplate.exchange(
                uri, HttpMethod.GET, new HttpEntity<>(headers),
                Data.class, companyNumber);

        CucumberContext.CONTEXT.set("statusCode", response.getStatusCodeValue());
        CucumberContext.CONTEXT.set("getResponseBody", response.getBody());

        kafkaTemplate.send("stream-company-profile", "not found");
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @When("A message is consumed with invalid event type from the {string} stream")
    public void consumeValidMessageWithInvalidEventType(String deltaType) throws InterruptedException {
        this.deltaType = deltaType;
        initialiseVariablesUsingDeltaType();

        kafkaTemplate.send(mainTopic, createMessageWithInvalidEventType());
        kafkaTemplate.flush();

        assertMessageConsumed();
    }

    @Then("A PATCH request is sent to the API")
    public void verifyAddPatchEndpointIsCalled() {
        verify(1, patchRequestedFor(urlEqualTo(patchUrl)));
    }

    @Then("A PATCH request is NOT sent to the API")
    public void verifyAddPatchEndpointIsNotCalled() {
        verify(0, patchRequestedFor(urlEqualTo(patchUrl)));
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
                System.out.println("Retry list: " + retryList );
                assertThat(retryList).hasSize(RETRY_ATTEMPTS);
                break;
            default:
                throw new IllegalArgumentException(String.format("Suffix: '%s' is not a valid topic suffix. " +
                        "Please use 'invalid', 'retry' or 'error' as the topic suffix.", topicSuffix));
        }
    }

    private void stubPatchLink(int responseCode, String eventType) {
        String apiCall;
        if (deltaType.equals("statements")) {
            apiCall = "persons-with-significant-control-statements";
        } else if (deltaType.equals("pscs")) {
            apiCall = "persons-with-significant-control";
        } else if (deltaType.equals("filing-history")) {
            apiCall = "filing-history";
        } else if (deltaType.equals("company-profile")) {
            apiCall = "persons-with-significant-control";
        } else {
            apiCall = deltaType;
        }
        switch (eventType) {
            case "changed":
                patchUrl = String.format("/company/%s/links/%s", COMPANY_NUMBER, apiCall);
                break;
            case "deleted":
                patchUrl = String.format("/company/%s/links/%s/delete", COMPANY_NUMBER, apiCall);
                break;
            default:
                throw new IllegalArgumentException(String.format("Event type: '%s' is not a valid event type, " +
                        "this will result in the patch url being null. Please use either 'changed' or 'deleted'.", eventType));
        }

        stubFor(
                patch(urlEqualTo(patchUrl))
                        .willReturn(aResponse()
                                .withStatus(responseCode)
                                .withHeader("Content-Type","application/json")
                                .withBody(createResponeBody(responseCode))));
    }

    public String createResponeBody(int responseCode){
        if(responseCode == 409){
            return "{\"error\": \"Conflict: the resource already exists.\"}";
        }
        else{
            return "{}";
        }
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

    private void initialiseVariablesUsingDeltaType() {
        if (deltaType.equals("statements")) {
            mainTopic = "stream-psc-statements";
            resourceUri = String.format("company/%s/persons-with-significant-control-statements", COMPANY_NUMBER);
        } else if (deltaType.equals("pscs")) {
            mainTopic = "stream-company-psc";
            resourceUri = String.format("company/%s/persons-with-significant-control",
                    COMPANY_NUMBER);
        } else if (deltaType.equals("filing-history")) {
            mainTopic = "stream-filing-history";
            resourceUri = String.format("company/%s/filing-history", COMPANY_NUMBER);
        } else if (deltaType.equals("company-profile")) {
            mainTopic = "stream-company-profile";
            resourceUri = String.format("company/%s/links", COMPANY_NUMBER);
        } else {
            mainTopic = String.format("stream-company-%s", deltaType);
            resourceUri = String.format("company/%s/%s", COMPANY_NUMBER, deltaType);
        }
        topicPrefix = String.format("%s-company-links-consumer", mainTopic);
    }

    @And("The number of {string} remaining in the company is {int}")
    public void theNumberOfOfficersInTheCompanyIs(String payloadType, int count) {
        String apiCall;
        switch (payloadType) {
            case "officers":
                apiCall = "officers";
                break;
            case "statements":
                apiCall = "persons-with-significant-control-statements";
                break;
            case "pscs":
                apiCall = "persons-with-significant-control";
                break;
            case "filing_history":
                apiCall = "filing-history";
                break;
            default:
                throw new IllegalArgumentException("payloadType passed in is invalid");
        }
        String url = String.format("/company/%s/%s", COMPANY_NUMBER, apiCall);

        if (count == 0) {
            stubFor(
                    get(urlEqualTo(url))
                            .willReturn(aResponse()
                                    .withStatus(HttpStatus.NOT_FOUND.value())));
        } else {
            OfficerList officerList = new OfficerList()
                    .totalResults(count);
            for (int i = 0; i < count; i++) {
                officerList.getItems().add(new OfficerSummary()
                        .links(new ItemLinkTypes()
                                .self(String.format("/company/%s/%s",
                                        COMPANY_NUMBER, UUID.randomUUID()))));
            }

            try {
                stubFor(
                        get(urlEqualTo(url))
                                .willReturn(aResponse()
                                        .withBody(objectMapper.writeValueAsString(officerList))
                                        .withStatus(HttpStatus.OK.value())));
            } catch (JsonProcessingException ex) {
                throw new UncheckedIOException(ex);
            }
        }
    }

    @And("The company appointments api is unavailable")
    public void stubCompanyAppointmentsApiUnavailable() {
        stubFor(
                get(urlEqualTo(String.format("/company/%s/officers", COMPANY_NUMBER)))
                        .willReturn(aResponse()
                                .withBody("{}")
                                .withStatus(HttpStatus.NOT_FOUND.value())));
    }
}