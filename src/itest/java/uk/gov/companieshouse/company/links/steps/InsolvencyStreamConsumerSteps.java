package uk.gov.companieshouse.company.links.steps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.github.tomakehurst.wiremock.admin.model.ServeEventQuery;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.cucumber.java.Before;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.company.links.config.WiremockTestConfig;
import uk.gov.companieshouse.company.links.consumer.ResettableCountDownLatch;
import uk.gov.companieshouse.company.links.service.CompanyInsolvencyService;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.getAllServeEvents;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.patchRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.removeAllMappings;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;

public class InsolvencyStreamConsumerSteps {

    private static final String RETRY_TOPIC_ATTEMPTS = "retry_topic-attempts";
    private static final int RETRY_COUNT = 5;

    private String companyNumber;

    private UUID uuid;

    @Autowired
    private CompanyProfileService companyProfileService;

    @Autowired
    private CompanyInsolvencyService companyInsolvencyService;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;

    @Autowired
    private ResettableCountDownLatch resettableCountDownLatch;

    @Autowired
    private ObjectMapper objectMapper;

    @Before
    public void beforeEach() {
        resettableCountDownLatch.resetLatch(4);
    }

    @Given("Company links consumer api service is running")
    public void company_links_consumer_api_service_is_running() {
        WiremockTestConfig.setupWiremock();
        assertThat(companyProfileService).isNotNull();
        WiremockTestConfig.setupWiremock();
    }

    @And("Company insolvency api service is running")
    public void company_insolvency_api_service_is_running() {
        assertThat(companyInsolvencyService).isNotNull();
        WiremockTestConfig.setupWiremock();
    }

    @When("a message is published to {string} topic for companyNumber {string} to update links")
    public void a_message_is_published_to_topic_for_company_number_to_update_links(String topicName, String companyNumber)
            throws InterruptedException {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubUpdateConsumerLinks(companyNumber,"profile-with-out-links.json");
        WiremockTestConfig.stubGetInsolvency(companyNumber, 200, "insolvency_output");
        sendMessage(topicName, createMessage(this.companyNumber, topicName));
        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @When("a message is published to the {string} topic for companyNumber {string} to update links")
    public void a_message_is_published_to_topic_for_company_number_to_update_links_410(String topicName, String companyNumber)
            throws InterruptedException {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubUpdateConsumerLinks(companyNumber,"profile-with-out-links.json");
        WiremockTestConfig.stubGetInsolvency(companyNumber, 410, null);
        kafkaTemplate.send(topicName, createMessage(this.companyNumber, topicName));

        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @When("a message is published to {string} topic for companyNumber {string} to update links with a null attribute")
    public void a_message_is_published_to_topic_for_company_number_to_update_links_with_a_null_attribute(String topicName, String companyNumber)
            throws InterruptedException {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubUpdateConsumerLinks(companyNumber,"profile-with-null-attribute.json");
        sendMessage(topicName, createMessage(this.companyNumber, topicName));

        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @When("a message is published to {string} topic for companyNumber {string} to check for links with status code {string}")
    public void a_message_is_published_to_topic_for_company_number_to_check_for_links_with_status_code(String topicName, String companyNumber, String statusCode)
            throws InterruptedException {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubGetConsumerLinksWithProfileLinks(companyNumber, Integer.parseInt(statusCode));
        sendMessage(topicName, createMessage(companyNumber, topicName));

        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @When("calling GET insolvency-data-api with companyNumber {string} returns status code {string} and insolvency is gone")
    public void call_to_insolvency_data_api_with_company_number_returns_status_code(String companyNumber, String statusCode) {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubGetInsolvency(companyNumber, Integer.parseInt(statusCode), "");
    }

    @And("calling a GET insolvency-data-api with companyNumber {string} returns status code {string} and insolvency is gone")
    public void call_to_insolvency_data_api_with_company_number_returns_status_code_And(String companyNumber, String statusCode) {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubGetInsolvency(companyNumber, Integer.parseInt(statusCode), "");
    }

    @When("calling GET insolvency-data-api with companyNumber {string} returns status code \"200\"")
    public void call_to_insolvency_data_api_with_company_number_returns_status_code_200(String companyNumber) {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubGetInsolvency(companyNumber, 200, "");
    }

    @When("a delete event is sent to kafka topic stream insolvency")
    public void a_delete_event_is_sent_to_kafka_topic_stream_insolvency(String topic) throws InterruptedException {
        removeAllMappings();
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

    @When("a delete event is sent to {string} topic for companyNumber {string} which has no links")
    public void a_delete_event_is_sent_to_topic_for_company_number_which_has_no_links(String topic, String companyNumber) throws InterruptedException {
        this.companyNumber = companyNumber;
        removeAllMappings();
        this.uuid = UUID.randomUUID();
        WiremockTestConfig.stubGetCompanyProfile(this.companyNumber, 200, "profile-with-out-links");
        stubFor(
                patch(urlEqualTo("/company/" + this.companyNumber + "/links")).withId(this.uuid)
                        .withRequestBody(containing("00006400"))
                        .willReturn(aResponse()
                                .withStatus(200)));

        sendMessage(topic, deleteMessage(this.companyNumber));
        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @When("a delete event is sent {string} topic")
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

    @Then("verify the company link is removed from company profile")
    public void verify_the_company_link_is_removed_from_company_profile()
            throws JsonProcessingException {
        ServeEvent serveEvent = findServeEvents()
                .orElseThrow()
                .get(0);
        String actual = serveEvent.getRequest().getBodyAsString();
        String expected = WiremockTestConfig.loadFile("profile-with-insolvency-links-delete.json");
        CompanyProfile expectedCompanyProfile = objectMapper.readValue(expected, CompanyProfile.class);
        CompanyProfile actualCompanyProfile = objectMapper.readValue(actual, CompanyProfile.class);
        assertThat(expectedCompanyProfile).isEqualTo(actualCompanyProfile);
    }

    @Then("verify the patch endpoint is never invoked to delete company links")
    public void verify_the_patch_endpoint_is_never_invoked_to_delete_company_links() {
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(0, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(0, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/insolvency")));
    }

    @Then("the Company Links Consumer should send a GET request to the Company Profile API")
    public void the_company_links_consumer_should_send_a_get_request_to_the_company_profile_api() {
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(0, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/insolvency")));
        verify(0, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));

    }

    @Then("the Company Links Consumer should send a PATCH request to the Company Profile API")
    public void the_company_links_consumer_should_send_a_patch_request_to_the_company_profile_api() {
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/insolvency")));
        verify(1, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links"))
                .withRequestBody(containing("/company/" + this.companyNumber + "/insolvency")));
    }

    @When("a non-avro message is published to {string} topic and failed to process")
    public void a_non_avro_message_is_published_to_topic_and_failed_to_process(String topicName) throws InterruptedException{
        kafkaTemplate.send(topicName,"invalid message");
        kafkaTemplate.flush();

        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @When("a valid message is published to {string} topic with invalid json")
    public void a_valid_message_is_published_to_topic_with_invalid_json(String topicName) throws InterruptedException {
        ResourceChangedData invalidJsonData = invalidJson();
        sendMessage(topicName, invalidJsonData);

        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Then("the message should be moved to topic {string}")
    public void the_message_should_be_moved_to_topic(String topic) {
        ConsumerRecord<String, Object> singleRecord =
                KafkaTestUtils.getSingleRecord(kafkaConsumer, topic, 5000L);
        assertThat(singleRecord.value()).isNotNull();
    }

    @Then("the message should be moved to topic {string} after retry attempts of {string}")
    public void the_message_should_be_moved_to_topic_after_retry_specified_attempts(String topic, String retryAttempts) {
        ConsumerRecord<String, Object> singleRecord =
                KafkaTestUtils.getSingleRecord(kafkaConsumer, topic, 5000L);

        assertThat(singleRecord.value()).isNotNull();

        List<Header> retryList = StreamSupport.stream(singleRecord.headers().spliterator(), false)
                .filter(header -> header.key().equalsIgnoreCase(RETRY_TOPIC_ATTEMPTS))
                .collect(Collectors.toList());
        assertThat(retryList).hasSize(Integer.parseInt(retryAttempts));
    }

    private void sendMessage(String topicName, ResourceChangedData companyNumber) {
        kafkaTemplate.send(topicName, companyNumber);
        kafkaTemplate.flush();
    }

    private ResourceChangedData createMessage(String companyNumber, String topicName) {
        EventRecord event = EventRecord.newBuilder()
                .setType("changed")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        return ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(companyNumber)
                .setResourceKind(topicName.contains("insolvency") ? "company-insolvency" : "charges-insolvency")
                .setResourceUri(topicName.contains("insolvency") ? "/company/"+companyNumber+"/links" : "/company/"+companyNumber+"/charges" )
                .setData("{ \"key\": \"value\" }")
                .setEvent(event)
                .build();
    }

    private ResourceChangedData invalidJson() {
        EventRecord event = EventRecord.newBuilder()
                .setType("changed")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        return ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId("")
                .setResourceKind("company-insolvency")
                .setResourceUri("/company/00006400/links")
                .setData("")
                .setEvent(event)
                .build();
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

    private Optional<List<ServeEvent>> findServeEvents() {
        for (int i = 0; i < RETRY_COUNT; i++) {
            List<ServeEvent> events = getAllServeEvents(ServeEventQuery.forStubMapping(this.uuid));
            if (!events.isEmpty()) {
                return Optional.of(events);
            } else {
                try {
                    Thread.sleep(1_000); // NOSONAR
                } catch (InterruptedException ignore) {
                }
            }
        }

        return Optional.empty();
    }
}
