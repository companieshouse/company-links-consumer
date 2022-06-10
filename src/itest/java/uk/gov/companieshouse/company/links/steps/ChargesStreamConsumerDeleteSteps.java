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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.companieshouse.company.links.consumer.TestData.EVENT_TYPE_DELETE;
import static uk.gov.companieshouse.company.links.consumer.TestData.RESOURCE_KIND_CHARGES;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
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

    @And("stubbed set with {string} and {string} for {string}")
    public void stubforCompanyNumberUsingResonseFiles(String linksResponseFile, String chargesResponse, String companyNumber){
        this.companyNumber = companyNumber;
        setGetAndPatchStubsFor(
            loadFileForCoNumber(linksResponseFile, this.companyNumber),
            loadFileForCoNumber(chargesResponse, this.companyNumber));
    }

    @And("stubbed set with {string} for {string} and getCharges give {int}")
    public void stubforCompanyProfileNumberUsingResonse(String linksResponseFile, String companyNumber, int chargesResponce){
        this.companyNumber = companyNumber;
        setGetStubsForWithChargesResponse(
            loadFileForCoNumber(linksResponseFile, this.companyNumber),chargesResponce);
    }

    @And("stubbed set with {string} and {string} for {string} but patch enpoint give {int}")
    public void stubforCompanyNumberUsingResonseFiles(String linksResponseFile, String chargesResponse, String companyNumber, int patchResponse){
        this.companyNumber = companyNumber;
        setGetStubsForWithPatchResonseResponse(
            loadFileForCoNumber(linksResponseFile, companyNumber), loadFileForCoNumber(chargesResponse, companyNumber), patchResponse);
    }

    private void setGetStubsForWithPatchResonseResponse(String linksResponse, String chargesResponse, int patchResponse){
        stubFor(
            get(urlEqualTo("/company/" + companyNumber + "/links"))
                .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(linksResponse)));

        stubFor(
            get(urlEqualTo("/company/" + companyNumber + "/charges"))
                .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(chargesResponse)));


        stubFor(
            patch(urlEqualTo("/company/" + companyNumber + "/links"))
                .withRequestBody(containing("\"company_number\":\"" +
                    companyNumber + "\""))
                .willReturn(aResponse()
                    .withStatus(patchResponse)));
    }

    private String loadFileForCoNumber(String fileName, String companyNumber) {
        try {
            String templateText = FileUtils.readFileToString(ResourceUtils.getFile("classpath:stubs/"+fileName), StandardCharsets.UTF_8);
            return String.format(templateText, companyNumber, companyNumber); // extra args are ignored
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unable to locate file %s", fileName));
        }
    }

    private void setGetStubsForWithChargesResponse(String linksResponse, int chargesResponse){
        stubFor(
            get(urlEqualTo("/company/" + companyNumber + "/links"))
                .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(linksResponse)));

        stubFor(
            get(urlEqualTo("/company/" + companyNumber + "/charges"))
                .willReturn(aResponse()
                    .withStatus(chargesResponse)));
    }

    private void setGetAndPatchStubsFor(String linksResponse, String chargesResponse){
        stubFor(
            get(urlEqualTo("/company/" + companyNumber + "/links"))
                .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(linksResponse)));

        stubFor(
            get(urlEqualTo("/company/" + companyNumber + "/charges"))
                .willReturn(aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(chargesResponse)));


        stubFor(
            patch(urlEqualTo("/company/" + companyNumber + "/links"))
                .withRequestBody(containing("\"company_number\":\"" +
                    companyNumber + "\""))
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
    @SuppressWarnings("unchecked")
    public void messageSuccessfullyConsumedCompanyProfilePatchInvokedRemovingChargesLink()
        throws JsonProcessingException {
        List<ServeEvent> events = WiremockTestConfig.getWiremockEvents();
        assertEquals(3, events.size());
        verify(1, getRequestedFor(urlEqualTo("/company/" + companyNumber + "/links")));
        verify(1, getRequestedFor(urlEqualTo("/company/" + companyNumber + "/charges")));
        verify(1, patchRequestedFor(urlEqualTo("/company/" + companyNumber + "/links")));
        Optional<ServeEvent> patch = events.stream()
            .filter(a -> "PATCH".equals(a.getRequest().getMethod().value())).findFirst();
        assertTrue(patch.isPresent());
        String body = new String(patch.get().getRequest().getBody());
        assertTrue(body.contains("\"has_charges\":false"));
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, HashMap<String, Object>> map = mapper.readValue(body, HashMap.class);//
        HashMap<String, Object> data = map.get("data");
        assertFalse((Boolean)data.get("has_charges"));
        HashMap<String, Object> links = (HashMap<String, Object>)data.get("links");
        assertNull(links.get("charges"));
    }

    @Then ("The message is successfully consumed and company-profile-api PATCH endpoint is not invoked")
    public void messageSuccessfullyConsumedCompanyProfilePatchNotInvoked() {
        verify(1, getRequestedFor(urlEqualTo("/company/" + companyNumber + "/links")));
        verify(0, patchRequestedFor(urlEqualTo("/company/" + companyNumber + "/links")));
     }


    @Then ("The message fails to process and retrys {int} times bvefore being sent to the {string}")
    public void messageIsRetriedBeforeBeingSentToTopic(int retries, String errorTopic) {
        ConsumerRecord<String, Object>
            singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, errorTopic);

        assertThat(singleRecord.value()).isNotNull();
        // verify first attempt + number of retries
        verify(retries + 1, getRequestedFor(urlEqualTo("/company/" + companyNumber + "/links")));
    }

    @Then ("The message fails to process and sent to the {string}")
    public void messageIsRetriedBeforeBeingSentToTopic(String invalidTopic) {
        ConsumerRecord<String, Object>
            singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, invalidTopic);

        assertThat(singleRecord.value()).isNotNull();
        verify(1, getRequestedFor(urlEqualTo("/company/" + companyNumber + "/links")));
    }
}
