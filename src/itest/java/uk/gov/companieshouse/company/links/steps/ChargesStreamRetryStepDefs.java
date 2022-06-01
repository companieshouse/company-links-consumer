package uk.gov.companieshouse.company.links.steps;

import static com.github.tomakehurst.wiremock.client.WireMock.patchRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import uk.gov.companieshouse.company.links.config.WiremockTestConfig;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

public class ChargesStreamRetryStepDefs {


    public static final String RETRY_TOPIC_ATTEMPTS = "retry_topic-attempts";
    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;
    private String companyNumber = "00006400";
    @Autowired
    private CompanyProfileService companyProfileService;

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
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @Then("The message is successfully consumed only once from the {string} topic")
    public void the_message_is_successfully_consumed_only_once_from_the_topic(String topicName) {
        assertThatThrownBy(() -> KafkaTestUtils.getSingleRecord(kafkaConsumer,
                topicName)).hasStackTraceContaining("No records found for topic");
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

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @Given("Stubbed Company Profile API PATCH endpoint will return {int} bad request http response code")
    public void stubbed_company_profile_api_patch_endpoint_will_return_bad_request_http_response_code(
            Integer statusCode) {
        WiremockTestConfig.stubGetCompanyProfile(companyNumber, statusCode,
                "profile-with-insolvency-links");
        WiremockTestConfig.stubPatchCompanyProfile(companyNumber, statusCode.intValue());
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
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
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
                singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, topicName);

        assertThat(singleRecord.value()).isNotNull();

        List<Header> retryList = StreamSupport.stream(singleRecord.headers().spliterator(), false)
                .filter(header -> header.key().equalsIgnoreCase(RETRY_TOPIC_ATTEMPTS))
                .collect(Collectors.toList());
        assertThat(retryList.size()).isEqualTo(Integer.parseInt(numberOfAttempts.toString()));
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
}
