package uk.gov.companieshouse.company.links.steps;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.admin.model.ServeEventQuery;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.ResourceUtils;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

public class CompanyLinksSteps {

    @Value("${wiremock.server.port}")
    private String port;

    @Value("${company-links.consumer.insolvency.topic}")
    private String topic;

    private static WireMockServer wireMockServer;

    private String companyNumber;

    private UUID uuid;

    @Autowired
    private CompanyProfileService companyProfileService;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;

    @Given("company insolvency links exist for companyNumber {string}")
    public void company_insolvency_links_exist_for_company_number(String companyNumber) throws InterruptedException {
        this.companyNumber = companyNumber;
        configureWiremock();

        stubUpdateConsumerLinks(false);
        kafkaTemplate.send(topic, createMessage(this.companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @Given("company insolvency links does not exist for companyNumber {string}")
    public void company_insolvency_links_does_not_exist_for_company_number(String companyNumber) throws InterruptedException {
        this.companyNumber = companyNumber;
        configureWiremock();

        stubGetCompanyInsolvencyWithoutLinks();
        kafkaTemplate.send(topic, createMessage(this.companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a message is published for companyNumber {string} to update links")
    public void a_message_is_published_for_company_number_to_update_links(String companyNumber) throws InterruptedException {
        this.companyNumber = companyNumber;
        configureWiremock();

        stubUpdateConsumerLinks(false);
        kafkaTemplate.send(topic, createMessage(this.companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a message is published for companyNumber {string} to update links with a null attribute")
    public void a_message_is_published_for_company_number_to_update_links_with_a_null_attribute(String companyNumber)
            throws InterruptedException {
        this.companyNumber = companyNumber;
        configureWiremock();

        stubUpdateConsumerLinks(true);
        kafkaTemplate.send(topic, createMessage(this.companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a message is published for companyNumber {string} to check for links with status code {string}")
    public void a_message_is_published_for_company_number_to_check_for_links_with_status_code(String companyNumber, String statusCode) throws InterruptedException {
        this.companyNumber = companyNumber;
        configureWiremock();
        stubGetConsumerLinks(Integer.parseInt(statusCode));

        kafkaTemplate.send(topic, createMessage(companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a delete event is sent to kafka topic stream insolvency")
    public void a_delete_event_is_sent_to_kafka_topic_stream_insolvency() throws InterruptedException {
        removeAllMappings();
        this.uuid = UUID.randomUUID();
        stubGetConsumerLinks(200);
        stubFor(
                patch(urlEqualTo("/company/" + this.companyNumber + "/links")).withId(this.uuid)
                        .withRequestBody(containing("00006400"))
                        .willReturn(aResponse()
                                .withStatus(200)));

        kafkaTemplate.send(topic, deleteMessage(companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a delete event is sent to kafka topic stream insolvency companyNumber {string} which has no links")
    public void a_delete_event_is_sent_to_kafka_topic_stream_insolvency_company_number_which_has_no_links(String companyNumber) throws InterruptedException {
        configureWiremock();
        this.companyNumber = companyNumber;
        stubGetCompanyInsolvencyWithoutLinks();

        kafkaTemplate.send(topic, deleteMessage(companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @Then("verify the patch endpoint is never invoked to delete company links")
    public void verify_the_patch_endpoint_is_never_invoked_to_delete_company_links() {
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(0, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));

        wireMockServer.stop();
    }

    @Then("the Company Links Consumer should send a GET request to the Company Profile API")
    public void the_company_links_consumer_should_send_a_get_request_to_the_company_profile_api() {
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(0, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));

        wireMockServer.stop();
    }

    @Then("the Company Links Consumer should send a PATCH request to the Company Profile API")
    public void the_company_links_consumer_should_send_a_patch_request_to_the_company_profile_api() {
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(1, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links"))
                .withRequestBody(containing("/company/" + this.companyNumber + "/insolvency")));

        wireMockServer.stop();
    }

    @When("a non-avro message is published and failed to process")
    public void a_non_avro_message_is_published_and_failed_to_process() throws InterruptedException {
        configureWiremock();
        kafkaTemplate.send(topic, "invalid message");

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a valid message is published with invalid json")
    public void a_valid_message_is_published_with_invalid_json() throws InterruptedException {
        configureWiremock();
        ResourceChangedData invalidJsonData = invalidJson();
        kafkaTemplate.send(topic, invalidJsonData);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @Then("the message should be moved to topic {string}")
    public void the_message_should_be_moved_to_topic(String topic) {
        ConsumerRecord<String, Object> singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, topic);

        assertThat(singleRecord.value()).isNotNull();

        wireMockServer.stop();
    }

    @Then("verify the company link is removed from company profile")
    public void verify_the_company_link_is_removed_from_company_profile() {
        List<ServeEvent> serveEvents = getAllServeEvents(ServeEventQuery.forStubMapping(this.uuid));
        ServeEvent serveEvent = serveEvents.get(0);
        String actual = serveEvent.getRequest().getBodyAsString();

        String expected = loadFile("profile-with-insolvency-links-delete.json");
        assertThat(expected).isEqualTo(actual);

        wireMockServer.stop();
    }

    private void configureWiremock() {
        wireMockServer = new WireMockServer(Integer.parseInt(port));
        wireMockServer.start();
        configureFor("localhost", Integer.parseInt(port));
    }

    private void stubUpdateConsumerLinks(boolean nullAttributeFlag) {
        String response = loadFile("profile-with-out-links.json");
        if (nullAttributeFlag) {
            response = loadFile("profile-with-null-attribute.json");
        }

        stubFor(
                get(urlEqualTo("/company/" + this.companyNumber + "/links"))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));

        stubFor(
                patch(urlEqualTo("/company/" + this.companyNumber + "/links"))
                        .withRequestBody(containing("/company/" + this.companyNumber + "/insolvency"))
                        .willReturn(aResponse()
                                .withStatus(200)));
    }

    private void stubGetCompanyInsolvencyWithoutLinks() {
        String response = loadFile("profile-with-out-links.json");

        stubFor(
                get(urlEqualTo("/company/" + this.companyNumber + "/links"))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));
    }

    private void stubGetConsumerLinks(int statusCode) {
        String response = loadFile("profile-with-insolvency-links.json");
        stubFor(
                get(urlEqualTo("/company/" + this.companyNumber + "/links"))
                        .willReturn(aResponse()
                                .withStatus(statusCode)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));
    }

    private ResourceChangedData createMessage(String companyNumber) {
        EventRecord event = EventRecord.newBuilder()
                .setType("changed")
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

    private String loadFile(String fileName) {
        try {
            return FileUtils.readFileToString(ResourceUtils.getFile("classpath:stubs/"+fileName), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unable to locate file %s", fileName));
        }
    }

}
