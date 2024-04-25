package uk.gov.companieshouse.company.links.steps;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.ResourceUtils;
import uk.gov.companieshouse.company.links.consumer.ResettableCountDownLatch;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class CompanyProfileStreamConsumerSteps {

    private String companyNumber;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ResettableCountDownLatch resettableCountDownLatch;

    @Given("Company profile exists with no PSC link for company {string}")
    public void company_profile_exists_no_psc_link(String companyNumber) {
        this.companyNumber = companyNumber;
        setGetAndPatchStubsFor(loadFileForCoNumber("profile-with-out-links.json", companyNumber));
    }

    @And("Psc exists for company {string}")
    public void psc_exists_for_company(String companyNumber) {
        stubForGetPsc(companyNumber);
    }

    @When("A valid avro Company Profile message is sent to the Kafka topic {string}")
    public void send_kafka_message(String topicName) {
        kafkaTemplate.send(topicName, createCompanyProfileMessage(companyNumber));
        kafkaTemplate.flush();
    }

    @Then("The message is successfully consumed and company-profile-api PATCH endpoint is invoked with PSC link payload")
    public void patchEndpointIsCalled() {
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/persons-with-significant-control")));
        verify(1, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
    }

    private String loadFileForCoNumber(String fileName, String companyNumber) {
        try {
            String templateText = FileUtils.readFileToString(ResourceUtils.getFile("classpath:stubs/"+fileName), StandardCharsets.UTF_8);
            return String.format(templateText, companyNumber, companyNumber); // extra args are ignored
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unable to locate file %s", fileName));
        }
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
                        .withRequestBody(containing("\"company-profile\":\"/company/" +
                                this.companyNumber))
                        .willReturn(aResponse()
                                .withStatus(200)));
    }

    public static void stubForGetPsc(String companyNumber) {
        stubFor(
                get(urlEqualTo("/company/" + companyNumber + "/persons-with-significant-control"))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody("{\n" +
                                        "    \"_id\": \"ABCDEF\",\n" +
                                        "    \"company_number\": \"" + companyNumber +
                                        "\"\n}")));
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
