package uk.gov.companieshouse.company.links.processor;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.FileCopyUtils;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

@ExtendWith(MockitoExtension.class)
class InsolvencyStreamProcessorTest {
    private static final String MOCK_COMPANY_NUMBER = "02588581";
    private InsolvencyStreamProcessor insolvencyProcessor;


    @Mock
    private CompanyProfileService companyProfileService;

    @Mock
    private Logger logger;

    @BeforeEach
    void setUp() {
        insolvencyProcessor = new InsolvencyStreamProcessor(
                companyProfileService,
                logger);
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData payload, updating insolvency links")
    void successfullyProcessResourceChangedDataInsolvencyLinksGetsUpdated() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfile());

        when(companyProfileService.getCompanyProfile("context_id", MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        insolvencyProcessor.process(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile("context_id", MOCK_COMPANY_NUMBER);
        verify(logger, times(4)).trace(anyString());
        verify(logger, atLeastOnce()).trace(
                contains("Resource changed message of kind company-insolvency"));
        verify(logger, atLeastOnce()).trace((
                String.format("Retrieved company profile for company number %s: %s",
                        MOCK_COMPANY_NUMBER, createCompanyProfile())));

        verify(logger, atLeastOnce()).trace((
                String.format("Current company profile with company number %s," +
                        " does not contain an insolvency link, attaching an insolvency link", MOCK_COMPANY_NUMBER
                )));
        verify(logger, atLeastOnce()).trace((
                String.format("Performing a PATCH with new company profile %s",
                        createCompanyProfileWithInsolvencyLinks())
                ));
    }


    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData payload, insolvency links doesn't need updating")
    void successfullyProcessResourceChangedDataInsolvencyLinksDoesntGetUpdated() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfileWithInsolvencyLinks());

        when(companyProfileService.getCompanyProfile("context_id", MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        insolvencyProcessor.process(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile("context_id", MOCK_COMPANY_NUMBER);
        verify(logger, times(3)).trace(anyString());
        verify(logger, atLeastOnce()).trace(
                contains("Resource changed message of kind company-insolvency"));
        verify(logger, atLeastOnce()).trace((
                String.format("Retrieved company profile for company number %s: %s",
                        MOCK_COMPANY_NUMBER, companyProfileApiResponse.getData())));
        verify(logger, atLeastOnce()).trace((
                String.format("Company profile with company number %s,"
                        + " already contains insolvency links, will not perform patch",
                        MOCK_COMPANY_NUMBER)
                ));
    }

    private Message<ResourceChangedData> createResourceChangedMessage() throws IOException {
        InputStreamReader exampleInsolvencyJsonPayload = new InputStreamReader(
                Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                        .getResourceAsStream("insolvency-record.json")));
        String insolvencyRecord = FileCopyUtils.copyToString(exampleInsolvencyJsonPayload);

        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(MOCK_COMPANY_NUMBER)
                .setResourceKind("company-insolvency")
                .setResourceUri(String.format("/company/%s/insolvency", MOCK_COMPANY_NUMBER))
                .setEvent(new EventRecord())
                .setData(insolvencyRecord)
                .build();

        return MessageBuilder
                .withPayload(resourceChangedData)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "test")
                .build();
    }

    private CompanyProfile createCompanyProfile() {
        Data companyProfileData = new Data();
        companyProfileData.setCompanyNumber(MOCK_COMPANY_NUMBER);

        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(companyProfileData);
        return companyProfile;
    }

    private CompanyProfile createCompanyProfileWithInsolvencyLinks() {
        Data companyProfileData = new Data();
        companyProfileData.setCompanyNumber(MOCK_COMPANY_NUMBER);
        Links links = new Links();
        links.setInsolvency(String.format("/company/%s/insolvency", MOCK_COMPANY_NUMBER));
        companyProfileData.setLinks(links);

        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(companyProfileData);
        return companyProfile;
    }

}