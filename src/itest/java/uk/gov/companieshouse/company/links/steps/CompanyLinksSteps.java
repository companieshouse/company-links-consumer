package uk.gov.companieshouse.company.links.steps;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.ResourceUtils;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

public class CompanyLinksSteps {

    private static WireMockServer wireMockServer;

    @Autowired
    private CompanyProfileService companyProfileService;

    @Autowired
    public KafkaTemplate<String, ResourceChangedData> kafkaTemplate;

    @Given("Company links consumer api service is running")
    public void company_links_consumer_api_service_is_running() {
        wireMockServer = new WireMockServer(8888);
        wireMockServer.start();
        configureFor("localhost", 8888);

        stubCompanyProfileServiceCalls();

        assertThat(companyProfileService).isNotNull();
    }

    @When("a message is published to the topic {string} for companyNumber {string}")
    public void a_message_is_published_to_the_topic_for_company_number(String topicName, String companyNumber) throws InterruptedException {
        kafkaTemplate.send(topicName, createMessage(companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @Then("the insolvency consumer should consume and process the message")
    public void the_insolvency_consumer_should_consume_and_process_the_message() {
        verify(1, getRequestedFor(urlPathEqualTo("/company/00006400")));
        verify(0, patchRequestedFor(urlPathEqualTo("/company/00006400/links")));

        wireMockServer.stop();
    }

    @Then("the Company Links Consumer should send a GET request to the Company Profile API")
    public void the_company_links_consumer_should_send_a_get_request_to_the_company_profile_api() {
        verify(1, getRequestedFor(urlPathEqualTo("/company/00006400")));
        verify(0, patchRequestedFor(urlPathEqualTo("/company/00006400/links")));

        wireMockServer.stop();
    }

    @Then("the Company Links Consumer should send a PATCH request to the Company Profile API")
    public void the_company_links_consumer_should_send_a_patch_request_to_the_company_profile_api() {
        verify(1, getRequestedFor(urlPathEqualTo("/company/00006401")));
        verify(1, patchRequestedFor(urlPathEqualTo("/company/00006401/links")));

        wireMockServer.stop();
    }

    private void stubCompanyProfileServiceCalls() {
        String getCompanyProfileJson = loadFile("getCompanyProfile.json");
        stubFor(
                get(urlPathEqualTo("/company/00006400"))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(getCompanyProfileJson)));

        stubFor(
                put(urlPathEqualTo("/company/00006400/links"))
                        .withRequestBody(containing("00006400"))
                        .willReturn(aResponse()
                                .withStatus(200)));

        getCompanyProfileJson = loadFile("patchCompanyProfile.json");
        stubFor(
                get(urlPathEqualTo("/company/00006401"))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(getCompanyProfileJson)));

        stubFor(
                put(urlPathEqualTo("/company/00006401/links"))
                        .withRequestBody(containing("00006401"))
                        .willReturn(aResponse()
                                .withStatus(200)));
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

    private String loadFile(String fileName) {
        try {
            return FileUtils.readFileToString(ResourceUtils.getFile("classpath:stubs/"+fileName), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unable to locate file %s", fileName));
        }
    }
}
