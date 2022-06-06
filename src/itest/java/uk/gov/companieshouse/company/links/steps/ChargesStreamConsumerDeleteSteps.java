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
import static uk.gov.companieshouse.company.links.consumer.TestData.DELETED;
import static uk.gov.companieshouse.company.links.consumer.TestData.EVENT_TYPE_DELETE;
import static uk.gov.companieshouse.company.links.consumer.TestData.RESOURCE_KIND_CHARGES;

import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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

public class ChargesStreamConsumerDeleteSteps {

    private String companyNumber;

    @Autowired
    private CompanyProfileService companyProfileService;

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public KafkaConsumer<String, Object> kafkaConsumer;

    @Given ("Company profile stubbed with charges links for {string}")
    public void stubforCompanyNumber(String companyNumber){
        this.companyNumber = companyNumber;
        setGetAndPatchStubsFor(
                loadFileForCoNumber("profile-with-charges-links.json", this.companyNumber),
                loadFileForCoNumber("charges-absent-output.json", this.companyNumber));
    }

    private String loadFileForCoNumber(String fileName, String companyNumber) {
        try {
            String templateText = FileUtils.readFileToString(ResourceUtils.getFile("classpath:stubs/"+fileName), StandardCharsets.UTF_8);
            return String.format(templateText, companyNumber, companyNumber); // extra args are ignored
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unable to locate file %s", fileName));
        }
    }

    private void setGetAndPatchStubsFor(String linksResponse, String chargesResponse){
        stubFor(
            get(urlEqualTo("/company/" + companyNumber + "/links"))
                .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(linksResponse)));

//        stubFor(
//            get(urlEqualTo("/company/" + companyNumber + "/charges"))
//                .willReturn(aResponse()
//                    .withStatus(200)
//                    .withHeader("Content-Type", "application/json")
//                    .withBody(chargesResponse)));


        stubFor(
            patch(urlEqualTo("/company/" + companyNumber + "/links"))
                .withRequestBody(containing("\"charges\":\"/company/" +
                    this.companyNumber + "/charges\""))
                .willReturn(aResponse()
                    .withStatus(200)));
    }

    @When ("A valid avro delete message for company number {string} is sent to the Kafka topic {string}")
    public void sendDeleteMessageForCompanyToTopic(String companyNumber, String topicName)
        throws InterruptedException {
        kafkaTemplate.send(topicName, createChargeDeleteMessage(companyNumber));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

    }

    private ResourceChangedData createChargeDeleteMessage(String companyNumber) {
        return createDeleteMessage(companyNumber, RESOURCE_KIND_CHARGES);
    }

    private ResourceChangedData createDeleteMessage(String companyNumber, String kind) {
        EventRecord event = EventRecord.newBuilder()
            .setType(EVENT_TYPE_DELETE)
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

    @Then ("The message is successfully consumed and company-profile-api PATCH endpoint is invoked removing charges link")
    public void messageSuccessfullyConsumedCompanyProfilePatchInvokedRemovingChargesLink(){
        List<ServeEvent> events = WiremockTestConfig.getEvents();
        assertEquals(2, events.size());
        verify(1, getRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
        verify(1, patchRequestedFor(urlEqualTo("/company/" + this.companyNumber + "/links")));
    }

}
