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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
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

    @Autowired
    private CompanyProfileService companyProfileService;

    @Autowired
    public KafkaTemplate<String, ResourceChangedData> kafkaTemplate;

    @Given("Company links consumer api service is running")
    public void company_links_consumer_api_service_is_running() {
        assertThat(companyProfileService).isNotNull();
    }

    @When("a message is published for companyNumber {string} to update links")
    public void a_message_is_published_for_company_number_to_update_links(String companyNumber) throws InterruptedException {
        this.companyNumber = companyNumber;
        configureWiremock();

        stubUpdateConsumerLinks();
        kafkaTemplate.send(topic, createMessage(this.companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
    }

    @When("a message is published for companyNumber {string} to check for links")
    public void a_message_is_published_for_company_number_to_check_for_links(String companyNumber) throws InterruptedException {
        this.companyNumber = companyNumber;
        configureWiremock();

        stubGetConsumerLinks();
        kafkaTemplate.send(topic, createMessage(companyNumber));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);
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


    private void configureWiremock() {
        wireMockServer = new WireMockServer(Integer.parseInt(port));
        wireMockServer.start();
        configureFor("localhost", Integer.parseInt(port));
    }

    private void stubUpdateConsumerLinks() {
        String response = loadFile("profile-with-out-links.json");

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

    private void stubGetConsumerLinks() {
        String response = loadFile("profile-with-links.json");
        stubFor(
                get(urlEqualTo("/company/" + this.companyNumber + "/links"))
                        .willReturn(aResponse()
                                .withStatus(200)
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

    private String loadFile(String fileName) {
        try {
            return FileUtils.readFileToString(ResourceUtils.getFile("classpath:stubs/"+fileName), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unable to locate file %s", fileName));
        }
    }
}
