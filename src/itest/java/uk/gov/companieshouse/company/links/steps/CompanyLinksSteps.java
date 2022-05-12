package uk.gov.companieshouse.company.links.steps;

import io.cucumber.java.After;
import io.cucumber.java.Before;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.github.tomakehurst.wiremock.WireMockServer;
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
import uk.gov.companieshouse.company.links.config.WiremockTestConfig;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

public class CompanyLinksSteps {

    public static final String RETRY_TOPIC_ATTEMPTS = "retry_topic-attempts";

    @Value("${company-links.consumer.insolvency.topic}")
    private String insolvancyTopic;

    @Value("${company-links.consumer.charges.topic}")
    private String chargesTopic;

    private String companyNumber;

    @Autowired
    private CompanyProfileService companyProfileService;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;

    @Before
    public static void before_each() {
        WiremockTestConfig.setupWiremock();
    }

    @After
    public static void after_each() {
        WiremockTestConfig.stop();
    }

    @Given("Company links consumer api service is running")
    public void company_links_consumer_api_service_is_running() {
        assertThat(companyProfileService).isNotNull();
    }

    @When("a message is published for companyNumber {string} to update links")
    public void a_message_is_published_for_company_number_to_update_links(String companyNumber) throws InterruptedException {
        this.companyNumber = companyNumber;


        WiremockTestConfig.stubUpdateConsumerLinks(companyNumber,false);
        kafkaTemplate.send(insolvancyTopic, createMessage(this.companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a message is published for companyNumber {string} to update links with a null attribute")
    public void a_message_is_published_for_company_number_to_update_links_with_a_null_attribute(String companyNumber)
            throws InterruptedException {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubUpdateConsumerLinks(companyNumber,true);
        kafkaTemplate.send(insolvancyTopic, createMessage(this.companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a message is published for companyNumber {string} to check for links with status code {string}")
    public void a_message_is_published_for_company_number_to_check_for_links_with_status_code(String companyNumber, String statusCode) throws InterruptedException {
        this.companyNumber = companyNumber;
        WiremockTestConfig.stubGetConsumerLinksWithProfileLinks(companyNumber, Integer.parseInt(statusCode));

        kafkaTemplate.send(insolvancyTopic, createMessage(companyNumber));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
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

    @When("a non-avro message is published and failed to process")
    public void a_non_avro_message_is_published_and_failed_to_process() throws InterruptedException {
        kafkaTemplate.send(insolvancyTopic, "invalid message");

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a non-avro message is published to charges topic and failed to process")
    public void a_non_avro_message_is_published_to_charges_topic_and_failed_to_process() throws InterruptedException {
        kafkaTemplate.send(chargesTopic,"invalid message");

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a valid message is published with invalid json")
    public void a_valid_message_is_published_with_invalid_json() throws InterruptedException {
        ResourceChangedData invalidJsonData = invalidJson();
        kafkaTemplate.send(insolvancyTopic, invalidJsonData);

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


}
