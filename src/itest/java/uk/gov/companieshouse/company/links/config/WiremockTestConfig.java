package uk.gov.companieshouse.company.links.config;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.springframework.util.ResourceUtils;

public class WiremockTestConfig {

    private static String port = "8888";

    private static WireMockServer wireMockServer;

    public static void setupWiremock() {
        wireMockServer = new WireMockServer(Integer.parseInt(port));
        start();
        configureFor("localhost", Integer.parseInt(port));
    }

    public static void start() {
        wireMockServer.start();
    }

    public static void stop() {
        wireMockServer.resetAll();
        wireMockServer.stop();
    }

    public static void resetAll() {
        wireMockServer.resetAll();
    }
    public static void stubUpdateConsumerLinks(String companyNumber, boolean nullAttributeFlag) {
        if (nullAttributeFlag) {
            stubUpdateConsumerLinks(companyNumber, "profile-with-null-attribute");
        } else {
            stubUpdateConsumerLinks(companyNumber, "profile-with-out-links");
        }
    }

    public static void stubUpdateConsumerLinks(String companyNumber, String fileToLoad) {
        String response = loadFile(fileToLoad + ".json");

        stubFor(
                get(urlEqualTo("/company/" + companyNumber + "/links"))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));

        stubFor(
                patch(urlEqualTo("/company/" + companyNumber + "/links"))
                        .withRequestBody(containing("/company/" + companyNumber + "/insolvency"))
                        .willReturn(aResponse()
                                .withStatus(200)));

    }

    public static void setGetAndPatchStubsFor(String companyNumber, String fileToLoad){
        String response = loadFile(fileToLoad + ".json");
        stubFor(
                get(urlEqualTo("/company/" + companyNumber + "/links"))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));

        stubFor(
                patch(urlEqualTo("/company/" + companyNumber + "/links"))
                        .withRequestBody(containing("\"charges\":\"/company/" +
                                companyNumber + "/charges\""))
                        .willReturn(aResponse()
                                .withStatus(200)));
    }

    public static void stubGetConsumerLinksWithProfileLinks(String companyNumber, int statusCode) {
        stubGetConsumerLinks(companyNumber, statusCode, "profile-with-charges-links");
    }
    public static void stubGetConsumerLinks(String companyNumber, int statusCode, String fileToLoad) {
        String response = loadFile(fileToLoad+".json");
        stubFor(
                get(urlEqualTo("/company/" + companyNumber + "/links"))
                        .willReturn(aResponse()
                                .withStatus(statusCode)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));
    }

    public static String loadFile(String fileName) {
        try {
            return FileUtils.readFileToString(ResourceUtils.getFile("classpath:stubs/"+fileName), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unable to locate file %s", fileName));
        }
    }

}
