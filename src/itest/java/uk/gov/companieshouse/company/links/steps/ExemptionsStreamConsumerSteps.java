package uk.gov.companieshouse.company.links.steps;

import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.ResourceUtils;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.consumer.ResettableCountDownLatch;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.companieshouse.company.links.config.WiremockTestConfig.getWiremockEvents;
import static uk.gov.companieshouse.company.links.config.WiremockTestConfig.setupWiremock;
import static uk.gov.companieshouse.company.links.data.TestData.CONTEXT_ID;
import static uk.gov.companieshouse.company.links.data.TestData.RESOURCE_KIND_EXEMPTIONS;

public class ExemptionsStreamConsumerSteps {
    private static final String COMPANY_NUMBER = "00006400";
    private static final int CONSUME_MESSAGE_TIMEOUT = 5; // TODO - Revert to 5
    private static final String INVALID_TOPIC = "stream-company-exemptions-company-links-consumer-invalid";
    private static final String RETRY_TOPIC = "stream-company-exemptions-company-links-consumer-retry";

    @Value("${company-links.consumer.exemptions.topic}")
    private String topic;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;

    @Autowired
    private ResettableCountDownLatch resettableCountDownLatch;

    @Given("Company links consumer is available")
    public void companyLinksConsumerIsRunning() {
        setupWiremock();
        //resettableCountDownLatch.resetLatch(4);
    }

    @And("The response code {int} will be returned from the PATCH request for {string}")
    public void stubPatchExemptionsLink(int responseCode, String companyNumber) {
        stubPatchLink(companyNumber, responseCode);
    }

    @And("The company profile for {string} does not contain an exemptions link")
    public void companyProfileHasExemptionsLink(String companyNumber) {

    }

    @When("A valid message is consumed for {string}")
    public void consumeValidMessage(String companyNumber) throws InterruptedException {
        kafkaTemplate.send(topic, createValidMessage(companyNumber, RESOURCE_KIND_EXEMPTIONS));
        kafkaTemplate.flush();

        //resettableCountDownLatch.resetLatch(4);

        assertMessageConsumed();
    }

    @When("An invalid message is consumed")
    public void consumeInvalidMessage() throws InterruptedException {
        kafkaTemplate.send(topic, "invalid message");
        kafkaTemplate.flush();

        //resettableCountDownLatch.resetLatch(4);

        //assertMessageConsumed();
    }

    @When("A message is consumed for {string} with invalid event type")
    public void consumeValidMessageWithInvalidEventType(String companyNumber) throws InterruptedException {
        kafkaTemplate.send(topic, createMessageWithInvalidEventType(companyNumber, RESOURCE_KIND_EXEMPTIONS));
        kafkaTemplate.flush();

        //resettableCountDownLatch.resetLatch(4);

        assertMessageConsumed();
    }

    @Then("A PATCH request is sent to the add company exemptions link endpoint for {string}")
    public void verifyPatchEndpointIsCalled(String companyNumber) {
        verify(1, patchRequestedFor(urlEqualTo(String.format("/company/%s/links/exemptions", companyNumber))));
    }

    @Then("The message is placed on the retry topic")
    public void verifyMessageIsPlacedOnRetryTopic() {
        ConsumerRecords<String, Object> records = KafkaTestUtils.getRecords(kafkaConsumer, 500L);
        String actualTopic = records.records(RETRY_TOPIC).iterator().next().topic();
        assertThat(actualTopic).isEqualTo(RETRY_TOPIC);
    }

    @Then("The message is placed on the invalid topic")
    public void verifyMessageIsPlacedOnInvalidTopic() {
//        ConsumerRecord<String, Object> record = KafkaTestUtils.getSingleRecord(kafkaConsumer, INVALID_TOPIC, 500L);
//        assertThat(record).isNotNull();

        ConsumerRecords<String, Object> records = KafkaTestUtils.getRecords(kafkaConsumer, 500L);
        String actualTopic = records.records(INVALID_TOPIC).iterator().next().topic();
        assertThat(actualTopic).isEqualTo(INVALID_TOPIC);
    }

    private void stubPatchLink(String companyNumber, int responseCode) {
        stubFor(
                patch(urlEqualTo("/company/" + companyNumber + "/links/exemptions"))
                        .willReturn(aResponse()
                                .withStatus(responseCode)));
    }

    private void stubAddExemptionsLink401Unauthorized(String companyNumber) {
        stubFor(
                patch(urlEqualTo("/company/" + companyNumber + "/links/exemptions"))
                        .willReturn(aResponse()
                                .withStatus(401)));
    }

    private void assertMessageConsumed() throws InterruptedException {
        assertThat(resettableCountDownLatch.getCountDownLatch()
                .await(CONSUME_MESSAGE_TIMEOUT, TimeUnit.SECONDS))
                .isTrue();
    }

    private void stubGetRequest(String linksObject) {
        stubFor(
            get(urlEqualTo(String.format("/company/%s/links", COMPANY_NUMBER)))
                    .willReturn(aResponse()
                            .withStatus(200)
                            .withHeader("Content-Type", "application/json")
                            .withBody(linksObject)));
    }

    private void stubPatchRequest() {
        stubFor(
            patch(urlEqualTo(String.format("/company/%s/links/exemptions", COMPANY_NUMBER)))
                .withRequestBody(containing("\"exemptions\":\"/company/" +
                        COMPANY_NUMBER + "/exemptions\""))
                .willReturn(aResponse()
                        .withStatus(200)));
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