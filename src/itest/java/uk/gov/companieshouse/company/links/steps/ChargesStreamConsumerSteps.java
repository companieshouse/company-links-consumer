package uk.gov.companieshouse.company.links.steps;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.patchRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.companieshouse.company.links.data.TestData.RESOURCE_KIND_CHARGES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.ResourceUtils;
import uk.gov.companieshouse.company.links.config.WiremockTestConfig;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

public class ChargesStreamConsumerSteps {

    @Value("${company-links.consumer.insolvency.topic}")
    private String topic;

    private String companyNumber;

    @Autowired
    private CompanyProfileService companyProfileService;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;

    @Given("Company profile stubbed with zero charges links for {string}")
    public void company_profile_exists_without_charges(String companyNumber) {
        this.companyNumber = companyNumber;
        setGetAndPatchStubsFor(loadFileForCoNumber("profile-with-out-charges.json", companyNumber));
     }

    @Given("Company profile stubbed with charges present for {string}")
    public void company_profile_exists_with_charges(String companyNumber) {
        this.companyNumber = companyNumber;
        setGetAndPatchStubsFor(loadFileForCoNumber("profile-with-charges-links.json", this.companyNumber));
    }

    @When("A valid avro message is sent to the Kafka topic {string}")
    public void send_kafka_message(String topicName) throws InterruptedException {
        kafkaTemplate.send(topicName, createChargesMessage(companyNumber));
        kafkaTemplate.flush();

        TimeUnit.SECONDS.sleep(1);
    }

    @Then("The message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked")
    public void patchEndpointNotCalled(){
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(0, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
    }

    @Then("The message is successfully consumed and company-profile-api PATCH endpoint is invoked with charges link payload")
    public void patchEdpointIsCalled() throws JsonProcessingException {
        List<ServeEvent> events = WiremockTestConfig.getWiremockEvents();
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(1, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));

        String requestBody = new String(events.get(0).getRequest().getBody());
        assertTrue(requestBody.contains("company_number\":\""+companyNumber));
        assertTrue(requestBody.contains("has_charges\":true"));
    }

    private void setGetAndPatchStubsFor(String response){
        stubFor(
            get(urlEqualTo("/company/" + this.companyNumber + "/links"))
                .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(response)));

        stubFor(
            patch(urlEqualTo("/company/" + this.companyNumber + "/links"))
                .withRequestBody(containing("\"charges\":\"/company/" +
                    this.companyNumber + "/charges\""))
                .willReturn(aResponse()
                    .withStatus(200)));
    }

    private ResourceChangedData createChargesMessage(String companyNumber) {
        return createBaseMessage(companyNumber, RESOURCE_KIND_CHARGES);
    }

    private ResourceChangedData createBaseMessage(String companyNumber, String kind) {
        EventRecord event = EventRecord.newBuilder()
                .setType("changed")
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        return ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(companyNumber)
                .setResourceKind(kind)
                .setResourceUri("/company/"+companyNumber+"/charges")
                .setData("{ \"key\": \"value\" }")
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

    @Given("Company links consumer api service is running And Stubbed Company Profile for {string} API GET and PATCH")
    public void companyLinksConsumerApiServiceIsRunningAndStubbedCompanyProfileForAPIGETAndPATCH(
        String companyNumber) {
        this.companyNumber = companyNumber;
        assertThat(companyProfileService).isNotNull();
    }
}
