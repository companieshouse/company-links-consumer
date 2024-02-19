package uk.gov.companieshouse.company.links.steps;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import uk.gov.companieshouse.company.links.config.WiremockTestConfig;
import uk.gov.companieshouse.company.links.consumer.ResettableCountDownLatch;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.patchRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class ChargesStreamRetryStepDefs {

    public static final String RETRY_TOPIC_ATTEMPTS = "retry_topic-attempts";

    private UUID uuid;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;
    private String companyNumber = "00006400";
    @Autowired
    private CompanyProfileService companyProfileService;
    @Autowired
    private ResettableCountDownLatch resettableCountDownLatch;

    @Given("Company Links Consumer component is successfully running")
    public void company_links_consumer_component_is_successfully_running() {
        WiremockTestConfig.setupWiremock();
        assertThat(companyProfileService).isNotNull();
    }

    @Given("Stubbed Company profile API endpoint will return {int} http response code")
    public void stubbed_company_profile_api_endpoint_will_return_http_response_code(
            Integer statusCode) {
        WiremockTestConfig.stubGetCompanyProfile(companyNumber, statusCode,
                "profile-with-null-attribute");
    }

    @When("A non-avro format random message is sent to the Kafka topic {string}")
    public void a_non_avro_format_random_message_is_sent_to_the_kafka_topic(String topicName)
            throws InterruptedException {
        kafkaTemplate.send(topicName, "invalid message");
        kafkaTemplate.flush();
        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Then("The message is successfully consumed only once from the {string} topic")
    public void the_message_is_successfully_consumed_only_once_from_the_topic(String topicName) {
        assertThatThrownBy(() -> KafkaTestUtils.getSingleRecord(kafkaConsumer,
                topicName, 5000L)).hasStackTraceContaining("No records found for topic");
    }

    @Then("Failed to process and immediately moved the message into {string} topic")
    public void failed_to_process_and_immediately_moved_the_message_into_topic(String topicName) {
        failed_to_process_and_immediately_sent_the_the_message_into_topic_for_attempts(0,
                topicName);
    }

    @Then("Metrics Data API endpoint is never invoked")
    public void metrics_data_api_endpoint_is_never_invoked() {
        verify(0, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/metrics")));
    }

    @When("A valid message in avro format message with an invalid url format is sent in resource_uri attribute to the Kafka topic {string}")
    public void a_valid_message_in_avro_format_message_with_an_invalid_url_format_is_sent_in_resource_uri_attribute_to_the_kafka_topic(
            String topicName)
            throws InterruptedException {
        kafkaTemplate.send(topicName, createChargesMessage(this.companyNumber, ""));
        kafkaTemplate.flush();
        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Given("Stubbed Company Profile API PATCH endpoint will return {int} bad request http response code")
    public void stubbed_company_profile_api_patch_endpoint_will_return_bad_request_http_response_code(
            Integer statusCode) {
        WiremockTestConfig.stubGetCompanyProfile(companyNumber, statusCode,
                "profile-with-insolvency-links");
        WiremockTestConfig.stubPatchCompanyProfile(companyNumber, statusCode);
    }

    @Given("Stubbed Company Profile API PATCH endpoint throws an internal exception")
    public void stubbed_company_profile_api_patch_endpoint_throws_an_internal_exception() {
        WiremockTestConfig.stubGetCompanyProfile(companyNumber, HttpStatus.OK.value(),
                "profile-with-insolvency-links");
        WiremockTestConfig.stubPatchCompanyProfile(companyNumber,
                HttpStatus.INTERNAL_SERVER_ERROR.value());
    }

    @When("A valid Avro message with valid json payload is sent to the Kafka topic {string} topic")
    public void a_valid_avro_message_with_valid_json_payload_is_sent_to_the_kafka_topic(
            String topicName)
            throws InterruptedException {
        var resourceUri = "/company/" + companyNumber + "/charges";
        kafkaTemplate.send(topicName, createChargesMessage(this.companyNumber, resourceUri));
        kafkaTemplate.flush();
        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @When("a delete event is sent to {string} topic")
    public void a_delete_event_is_sent_topic(String topic) throws InterruptedException {
        this.uuid = UUID.randomUUID();
        WiremockTestConfig.stubGetConsumerLinksWithProfileLinks(this.companyNumber, Integer.parseInt("200"));
        stubFor(
                patch(urlEqualTo("/company/" + this.companyNumber + "/links")).withId(this.uuid)
                        .withRequestBody(containing("00006400"))
                        .willReturn(aResponse()
                                .withStatus(200)));

        sendMessage(topic, deleteMessage(companyNumber));
        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Given("Stubbed Company Profile API GET endpoint is down")
    public void stubbed_company_profile_api_get_endpoint_is_down() {
        WiremockTestConfig.stubGetCompanyProfile(companyNumber,
                HttpStatus.SERVICE_UNAVAILABLE.value(), "");
    }

    @Then("The message should be retried with {int} attempts and on retry exhaustion the message is finally sent into {string} topic")
    public void failed_to_process_and_immediately_sent_the_the_message_into_topic_for_attempts(
            Integer numberOfAttempts, String topicName) {
        ConsumerRecord<String, Object>
                singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, topicName, 5000L);

        assertThat(singleRecord.value()).isNotNull();

        List<Header> retryList = StreamSupport.stream(singleRecord.headers().spliterator(), false)
                .filter(header -> header.key().equalsIgnoreCase(RETRY_TOPIC_ATTEMPTS))
                .collect(Collectors.toList());
        assertThat(retryList).hasSize(Integer.parseInt(numberOfAttempts.toString()));
    }

    @And("calling the GET insolvency-data-api with companyNumber {string} returns status code {string}")
    public void call_to_insolvency_data_api_with_company_number_returns_status_code_And(String companyNumber, String statusCode) {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubGetInsolvency(companyNumber, Integer.parseInt(statusCode), "");
    }

    private ResourceChangedData createChargesMessage(String companyNumber, String resourceUri) {
        EventRecord event = EventRecord.newBuilder()
                .setType("changed")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        return ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(companyNumber)
                .setResourceKind("company-charges")
                .setResourceUri(resourceUri)
                .setData("{ \"key\": \"value\" }")
                .setEvent(event)
                .build();
    }

    private void sendMessage(String topicName, ResourceChangedData companyNumber) {
        kafkaTemplate.send(topicName, companyNumber);
        kafkaTemplate.flush();
    }

    private ResourceChangedData deleteMessage(String companyNumber) {
        EventRecord event = EventRecord.newBuilder()
                .setType("deleted")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        return ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(companyNumber)
                .setResourceKind("company-insolvency")
                .setResourceUri("/company/"+companyNumber+"/links")
                .setData("{ \"key\": \"value\" }")
                .setEvent(event)
                .build();
    }
}
