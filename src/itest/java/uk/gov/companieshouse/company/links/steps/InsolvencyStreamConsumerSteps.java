package uk.gov.companieshouse.company.links.steps;

import com.google.gson.Gson;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import com.github.tomakehurst.wiremock.admin.model.ServeEventQuery;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import uk.gov.companieshouse.company.links.config.WiremockTestConfig;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

public class InsolvencyStreamConsumerSteps {

    public static final String RETRY_TOPIC_ATTEMPTS = "retry_topic-attempts";

    private String companyNumber;

    private UUID uuid;

    @Autowired
    private CompanyProfileService companyProfileService;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;


    @Given("Company links consumer api service is running")
    public void company_links_consumer_api_service_is_running() {
        WiremockTestConfig.setupWiremock();
        assertThat(companyProfileService).isNotNull();
        WiremockTestConfig.setupWiremock();
    }

    @When("a message is published to {string} topic for companyNumber {string} to update links")
    public void a_message_is_published_to_topic_for_company_number_to_update_links(String topicName, String companyNumber)
            throws InterruptedException {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubUpdateConsumerLinks(companyNumber,"profile-with-out-links.json");
        kafkaTemplate.send(topicName, createMessage(this.companyNumber, topicName));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a message is published to {string} topic for companyNumber {string} to update links with a null attribute")
    public void a_message_is_published_to_topic_for_company_number_to_update_links_with_a_null_attribute(String topicName, String companyNumber)
            throws InterruptedException {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubUpdateConsumerLinks(companyNumber,"profile-with-null-attribute.json");
        kafkaTemplate.send(topicName, createMessage(this.companyNumber, topicName));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }


    @When("a message is published to {string} topic for companyNumber {string} to check for links with status code {string}")
    public void a_message_is_published_to_topic_for_company_number_to_check_for_links_with_status_code(String topicName, String companyNumber, String statusCode)
            throws InterruptedException {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubGetConsumerLinksWithProfileLinks(companyNumber, Integer.parseInt(statusCode));

        kafkaTemplate.send(topicName, createMessage(companyNumber, topicName));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
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

        kafkaTemplate.send(topic, deleteMessage(companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
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

        kafkaTemplate.send(topic, deleteMessage(this.companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
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

        kafkaTemplate.send(topic, deleteMessage(companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @Then("verify the company link is removed from company profile")
    public void verify_the_company_link_is_removed_from_company_profile() {
        List<ServeEvent> serveEvents = getAllServeEvents(ServeEventQuery.forStubMapping(this.uuid));
        ServeEvent serveEvent = serveEvents.get(0);
        String actual = serveEvent.getRequest().getBodyAsString();

        String expected = WiremockTestConfig.loadFile("profile-with-insolvency-links-delete.json");
        assertThat(expected).isEqualTo(actual);
    }


    @Then("verify the patch endpoint is never invoked to delete company links")
    public void verify_the_patch_endpoint_is_never_invoked_to_delete_company_links() {
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(0, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
    }

    @Then("the Company Links Consumer should send a GET request to the Company Profile API")
    public void the_company_links_consumer_should_send_a_get_request_to_the_company_profile_api() {
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(0, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));

    }

    @Then("the Company Links Consumer should send a PATCH request to the Company Profile API")
    public void the_company_links_consumer_should_send_a_patch_request_to_the_company_profile_api() {
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(1, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links"))
                .withRequestBody(containing("/company/" + this.companyNumber + "/insolvency")));

    }

    @When("a non-avro message is published to {string} topic and failed to process")
    public void a_non_avro_message_is_published_to_topic_and_failed_to_process(String topicName) throws InterruptedException{
        kafkaTemplate.send(topicName,"invalid message");

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a valid message is published to {string} topic with invalid json")
    public void a_valid_message_is_published_to_topic_with_invalid_json(String topicName) throws InterruptedException {
        ResourceChangedData invalidJsonData = invalidJson();
        kafkaTemplate.send(topicName, invalidJsonData);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @Then("the message should be moved to topic {string}")
    public void the_message_should_be_moved_to_topic(String topic) {
        ConsumerRecord<String, Object> singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, topic);
        assertThat(singleRecord.value()).isNotNull();
    }

    @Then("the message should be moved to topic {string} after retry attempts of {string}")
    public void the_message_should_be_moved_to_topic_after_retry_specified_attempts(String topic, String retryAttempts) {
        ConsumerRecord<String, Object> singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, topic);

        assertThat(singleRecord.value()).isNotNull();

        List<Header> retryList = StreamSupport.stream(singleRecord.headers().spliterator(), false)
                .filter(header -> header.key().equalsIgnoreCase(RETRY_TOPIC_ATTEMPTS))
                .collect(Collectors.toList());
        assertThat(retryList.size()).isEqualTo(Integer.parseInt(retryAttempts));
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

}
