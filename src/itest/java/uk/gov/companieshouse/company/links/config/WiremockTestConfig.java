package uk.gov.companieshouse.company.links.config;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.springframework.util.ResourceUtils;

public class WiremockTestConfig {

    private static String port = "8888";

    private static WireMockServer wireMockServer;

    public static void setupWiremock() {
        if (wireMockServer == null) {
            wireMockServer = new WireMockServer(Integer.parseInt(port));
            start();
            configureFor("localhost", Integer.parseInt(port));
        } else {
            restart();
        }
    }

    public static void start() {
        wireMockServer.start();
    }

    public static void stop() {
        wireMockServer.resetAll();
        wireMockServer.stop();
    }

    public static void restart() {
        stop();
        start();
    }

    public static void stubUpdateConsumerLinks(String companyNumber, String fileToLoad) {
        String response = loadFile(fileToLoad);

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

    public static void stubGetConsumerLinksWithProfileLinks(String companyNumber, int statusCode) {
        stubGetCompanyProfile(companyNumber, statusCode, "profile-with-insolvency-links");
    }

    public static void stubPatchCompanyProfile(String companyNumber, int statusCode) {
        stubFor(
                patch(urlEqualTo("/company/" + companyNumber + "/links"))
                        .withRequestBody(containing("/company/" + companyNumber + "/insolvency"))
                        .willReturn(aResponse()
                                .withStatus(statusCode)));
    }

    public static void stubCompanyMetrics(String companyNumber, int statusCode) {
        stubFor(
                patch(urlEqualTo("/company/" + companyNumber + "/metrics"))
                        .withRequestBody(containing("/company/" + companyNumber + "/metrics"))
                        .willReturn(aResponse()
                                .withStatus(statusCode)));
    }

    public static void stubGetCompanyProfile(String companyNumber, int statusCode, String fileToLoad) {
        String response = "";
        if (fileToLoad != null && !fileToLoad.isEmpty()) {
            response = loadFile(fileToLoad+".json");
        }
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
