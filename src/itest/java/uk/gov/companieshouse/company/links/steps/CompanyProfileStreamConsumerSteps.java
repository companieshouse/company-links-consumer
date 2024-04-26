package uk.gov.companieshouse.company.links.steps;

import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

    @Given("Company profile exists with no PSC link for company {string}")
    public void company_profile_exists_no_psc_link(String companyNumber) {
        this.companyNumber = companyNumber;
        WiremockTestConfig.setGetAndPatchStubsFor(this.companyNumber,
                loadFileForCoNumber("profile-with-out-links.json"));
    }

    @Given("Company profile exists with PSC link for company {string}")
    public void company_profile_exists_with_psc_link(String companyNumber) {
        this.companyNumber = companyNumber;
        WiremockTestConfig.setGetAndPatchStubsFor(this.companyNumber,
                loadFileForCoNumber("profile-with-all-links.json"));
    }

    @Given("Company links consumer service is running")
    public void company_links_consumer_api_service_is_running() {
        WiremockTestConfig.setupWiremock();
        assertThat(companyProfileService).isNotNull();
    }

    @And("Psc exists for company {string}")
    public void psc_exists_for_company(String companyNumber) {
        WiremockTestConfig.stubForGetPsc(companyNumber, loadFileForCoNumber("psc-list-record.json"));
    }

    @And("Psc does not exist for company {string}")
    public void psc_does_not_exist_for_company(String companyNumber) {
        WiremockTestConfig.stubForGetPsc(companyNumber, loadFileForCoNumber("psc-list-empty-record.json"));
    }

    @When("A valid avro Company Profile message is sent to the Kafka topic {string}")
    public void send_company_profile_kafka_message(String topicName) throws InterruptedException {
        kafkaTemplate.send(topicName, createCompanyProfileMessage(companyNumber));
        kafkaTemplate.flush();

        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @When("An invalid avro Company Profile message is sent to the Kafka topic {string}")
    public void send_company_profile_invalid_kafka_message(String topicName) throws InterruptedException{
        kafkaTemplate.send(topicName,"invalid message");
        kafkaTemplate.flush();

        assertThat(resettableCountDownLatch.getCountDownLatch().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Then("The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with PSC link payload")
    public void patchCompanyProfileEndpointIsCalled() {
        List<ServeEvent> events = WiremockTestConfig.getWiremockEvents();
        assertEquals(3, events.size());
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/persons-with-significant-control")));
        verify(1, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
    }

    @Then("The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked and there were {int} total events")
    public void patchCompanyProfileEndpointNotCalled(Integer numberOfEvents) {
        List<ServeEvent> events = WiremockTestConfig.getWiremockEvents();
        assertEquals(numberOfEvents, events.size());
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(0, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
    }

    private String loadFileForCoNumber(String fileName) {
        try {
            return FileUtils.readFileToString(ResourceUtils.getFile("classpath:stubs/"+fileName), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unable to locate file %s", fileName));
        }
    }

    private ResourceChangedData createCompanyProfileMessage(String companyNumber) {
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
                .setData("{ \"key\": \"value\" }")
                .setEvent(event)
                .build();
    }

}
