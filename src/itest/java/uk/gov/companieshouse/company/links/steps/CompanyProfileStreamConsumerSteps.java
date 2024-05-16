package uk.gov.companieshouse.company.links.steps;

import io.cucumber.java.Before;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.ResourceUtils;
import uk.gov.companieshouse.company.links.config.WiremockTestConfig;
import uk.gov.companieshouse.company.links.consumer.ResettableCountDownLatch;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

public class CompanyProfileStreamConsumerSteps {

    private String companyNumber;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private CompanyProfileService companyProfileService;

    @Autowired
    private ResettableCountDownLatch resettableCountDownLatch;

    @Before
    public void beforeEach() {
        resettableCountDownLatch.resetLatch(4);
    }

    @Given("Company profile exists with no {string} link for company {string}")
    public void company_profile_exists_without_one_link(String linkType, String companyNumber) {
        this.companyNumber = companyNumber;
        WiremockTestConfig.setGetAndPatchStubsFor(linkType, this.companyNumber,
                loadFileFromName(String.format("profile-without-%s-link.json", linkType)));
    }

    @Given("Company profile exists with {string} link for company {string}")
    public void company_profile_exists_with_all_link(String linkType, String companyNumber) {
        this.companyNumber = companyNumber;
        WiremockTestConfig.setGetAndPatchStubsFor(linkType, this.companyNumber,
                loadFileFromName("profile-with-all-links.json"));
    }

    @Given("Company links consumer service is running")
    public void company_links_consumer_api_service_is_running() {
        WiremockTestConfig.setupWiremock();
        assertThat(companyProfileService).isNotNull();
    }

    @And("{string} exist for company {string}")
    public void objects_exist_for_company(String linkType, String companyNumber) {
        if (linkType.equals("filing-history")){
            WiremockTestConfig.stubForGetFilingHistory(companyNumber,
                    loadFileFromName(String.format("%s-list-record.json", linkType)), 200);
        } else {
            WiremockTestConfig.stubForGet(linkType, companyNumber,
                    loadFileFromName(String.format("%s-list-record.json", linkType)), 200);
        }
    }

    @And("{string} do not exist for company {string}")
    public void objects_do_not_exist_for_company(String linkType, String companyNumber) {
        WiremockTestConfig.stubForGet(linkType, companyNumber,
                loadFileFromName("empty-list-record.json"), 200);
    }

    @And("The user is not authorized")
    public void user_unauthorized() {
        WiremockTestConfig.stubForGetPscWith401Response("00006400");
    }

    @And("The company profile api is not available")
    public void company_profile_api_not_available() {
        WiremockTestConfig.setPatchStubsForPscWith404Response("00006400");
    }

    @When("A valid avro Company Profile without {string} link message is sent to the Kafka topic {string}")
    public void send_company_profile_kafka_message(String linkType, String topicName) throws InterruptedException {
        kafkaTemplate.send(topicName, createCompanyProfileMessageWithoutOneLink(linkType, companyNumber));
        kafkaTemplate.flush();

        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @When("A valid avro Company Profile with all links message is sent to the Kafka topic {string}")
    public void send_company_profile_with_all_links_kafka_message(String topicName) throws InterruptedException {
        kafkaTemplate.send(topicName, createCompanyProfileMessageWithAllLinks(companyNumber));
        kafkaTemplate.flush();

        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @When("An invalid avro Company Profile message is sent to the Kafka topic {string}")
    public void send_company_profile_invalid_kafka_message(String topicName) throws InterruptedException{
        kafkaTemplate.send(topicName,"invalid message");
        kafkaTemplate.flush();

        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Then("The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with {string} link payload")
    public void patchCompanyProfileEndpointIsCalledForLink(String linkType) {
        verify(1, getRequestedFor(urlEqualTo(String.format("/company/%s/%s", this.companyNumber, linkType))));
        verify(1, patchRequestedFor(urlEqualTo(String.format("/company/%s/links/%s", this.companyNumber, linkType))));
    }

    @Then("The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with Filing History link payload")
    public void patchCompanyProfileEndpointIsCalledForFilingHistory() {
        verify(1, getRequestedFor(urlEqualTo(String.format("/filing-history-data-api/company/%s/filing-history", this.companyNumber))));
        verify(1, patchRequestedFor(urlEqualTo(String.format("/company/%s/links/filing-history", this.companyNumber))));
    }

    @Then("The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with {string} link payload")
    public void patchCompanyProfileEndpointNotCalled(String linkType) {
        verify(0, patchRequestedFor(urlEqualTo(String.format("/company/%s/links/%s", this.companyNumber, linkType))));
    }

    private String loadFileFromName(String fileName) {
        try {
            return FileUtils.readFileToString(ResourceUtils.getFile("classpath:stubs/"+fileName), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unable to locate file %s", fileName));
        }
    }

    private ResourceChangedData createCompanyProfileMessageWithoutOneLink(String linkType, String companyNumber) {
        EventRecord event = EventRecord.newBuilder()
                .setType("changed")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        return ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(companyNumber)
                .setResourceKind("company-profile")
                .setResourceUri("/company/"+companyNumber)
                .setData(loadFileFromName(String.format("profile-data-without-%s-link.json", linkType)))
                .setEvent(event)
                .build();
    }
    private ResourceChangedData createCompanyProfileMessageWithAllLinks(String companyNumber) {
        EventRecord event = EventRecord.newBuilder()
                .setType("changed")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        return ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(companyNumber)
                .setResourceKind("company-profile")
                .setResourceUri("/company/"+companyNumber)
                .setData(loadFileFromName("profile-data-with-all-links.json"))
                .setEvent(event)
                .build();
    }

}
