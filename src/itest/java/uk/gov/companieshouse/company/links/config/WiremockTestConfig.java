package uk.gov.companieshouse.company.links.config;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.springframework.http.HttpStatus;
import org.springframework.util.ResourceUtils;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class WiremockTestConfig {

    private static final String PORT = "8888";
    private static final String CHARGE_ID = "123456789000";
    private static WireMockServer wireMockServer;

    public static void setupWiremock() {
        if (wireMockServer == null) {
            wireMockServer = new WireMockServer(Integer.parseInt(PORT));
            wireMockServer.start();
            configureFor("localhost", Integer.parseInt(PORT));
        } else {
            wireMockServer.resetAll();
        }
    }

    public static List<ServeEvent> getWiremockEvents(){
        return wireMockServer.getAllServeEvents();
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

    public static void stubGetInsolvency(String companyNumber, int statusCode, String fileToLoad) {
        String stubInsolvencyResponse = "{}";
        if (fileToLoad != null && !fileToLoad.isEmpty()) {
            stubInsolvencyResponse = loadFile(fileToLoad+".json");
        }

        stubFor(
                get(urlPathMatching("/company/" + companyNumber + "/insolvency"))
                        .willReturn(aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withStatus(statusCode)
                                .withBody(stubInsolvencyResponse)));
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

    public static void stubForGetChargeDataAPI(String companyNumber) {
        stubFor(
                get(urlEqualTo("/company/" + companyNumber + "/charges/"+ CHARGE_ID))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody("{\n" +
                                        "    \"_id\": \"AbRiN\",\n" +
                                        "    \"company_number\": \"" + companyNumber +
                                        "\"\n}")));
    }

    public static void setGetAndPatchStubsFor(String linkType, String companyNumber, String response){
        stubFor(
                get(urlEqualTo(String.format("/company/%s/links", companyNumber)))
                        .willReturn(aResponse()
                                .withStatus(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));

        stubFor(
                patch(urlEqualTo(String.format("/company/%s/links/%s", companyNumber, linkType)))
                        .willReturn(aResponse()
                                .withStatus(200)));
    }

    public static void stubForGet(String linkType, String companyNumber, String response, Integer responseCode) {
        stubFor(
                get(urlEqualTo(String.format("/company/%s/%s", companyNumber, linkType)))
                        .willReturn(aResponse()
                                .withStatus(responseCode)
                                .withHeader("Content-Type", "application/json")
                                .withBody(response)));
    }

    public static void stubForGetPscWith401Response(String companyNumber) {
        stubFor(
                get(urlEqualTo(String.format("/company/%s/persons-with-significant-control", companyNumber)))
                        .willReturn(aResponse()
                                .withStatus(HttpStatus.UNAUTHORIZED.value())
                                .withHeader("Content-Type", "application/json")));
    }

    public static void setPatchStubsForPscWith404Response(String companyNumber){
        stubFor(
                patch(urlEqualTo(String.format("/company/%s/links/persons-with-significant-control", companyNumber)))
                        .willReturn(aResponse()
                                .withStatus(HttpStatus.SERVICE_UNAVAILABLE.value())));
    }

}
