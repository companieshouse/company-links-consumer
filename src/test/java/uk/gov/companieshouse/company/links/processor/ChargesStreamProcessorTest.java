package uk.gov.companieshouse.company.links.processor;

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

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChargesStreamProcessorTest {
    private static final String MOCK_COMPANY_NUMBER = "03105860";
    private ChargesStreamProcessor chargesStreamProcessor;


    @Mock
    private CompanyProfileService companyProfileService;

    @Mock
    private Logger logger;

    @BeforeEach
    void setUp() {
        chargesStreamProcessor = new ChargesStreamProcessor(
                companyProfileService,
                logger);
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData payload, updating charges links")
    void successfullyProcessResourceChangedDataChargesLinksGetsUpdated() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        CompanyProfile companyProfile = createCompanyProfile();
        CompanyProfile companyProfileWithLinks = createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        final ApiResponse<Void> updatedCompanyProfileApiResponse = new ApiResponse<Void>(
                HttpStatus.OK.value(), null, null);

        when(companyProfileService.getCompanyProfile("context_id", MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        when(companyProfileService.patchCompanyProfile("context_id", MOCK_COMPANY_NUMBER, companyProfile))
                .thenReturn(updatedCompanyProfileApiResponse);

        chargesStreamProcessor.process(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile("context_id", MOCK_COMPANY_NUMBER);

        verify(companyProfileService).patchCompanyProfile("context_id", MOCK_COMPANY_NUMBER,
                companyProfileWithLinks);
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData payload, updating charges links")
    void doesCompanyProfileHaveCharges_should_return_false() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        CompanyProfile companyProfile = createCompanyProfile();
        CompanyProfile companyProfileWithLinks = createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        final ApiResponse<Void> updatedCompanyProfileApiResponse = new ApiResponse<Void>(
                HttpStatus.OK.value(), null, null);

        assertTrue(!chargesStreamProcessor.doesCompanyProfileHaveCharges(MOCK_COMPANY_NUMBER,
               companyProfile.getData().getLinks()));

    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData payload, links doesn't need updating")
    void successfullyProcessResourceChangedDataChargesDoesntGetUpdated() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        CompanyProfile companyProfileWithLinks = createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfileWithLinks);

        final ApiResponse<Void> updatedCompanyProfileApiResponse = new ApiResponse<Void>(
                HttpStatus.OK.value(), null, null);

        when(companyProfileService.getCompanyProfile("context_id", MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        chargesStreamProcessor.process(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile("context_id", MOCK_COMPANY_NUMBER);
        verify(logger, times(3)).trace(anyString());
        verify(logger, atLeastOnce()).trace(
                contains("Resource changed message of kind company-charges"));
        verify(logger, atLeastOnce()).trace((
                String.format("Retrieved company profile for company number %s: %s",
                        MOCK_COMPANY_NUMBER, companyProfileWithLinks)));

        verify(logger, times(0)).trace((
                String.format("Current company profile with company number %s," +
                        " does not contain charges link, attaching charges link", MOCK_COMPANY_NUMBER
                )));
        verify(logger, atLeastOnce()).trace((
                String.format("Company profile with company number %s," +
                        " already contains charges links, will not perform patch", MOCK_COMPANY_NUMBER
                )));
        verify(logger, times(0)).trace((
                String.format("Performing a PATCH with new company profile %s",
                        companyProfileWithLinks)
        ));

        verify(companyProfileService, times(0)).patchCompanyProfile("context_id", MOCK_COMPANY_NUMBER,
                companyProfileWithLinks);
    }

    private Message<ResourceChangedData> createResourceChangedMessage() throws IOException {
        InputStreamReader exampleChargesJsonPayload = new InputStreamReader(
                Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                        .getResourceAsStream("charges-record.json")));
        String chargesRecord = FileCopyUtils.copyToString(exampleChargesJsonPayload);

        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
                .setContextId("context_id")
                .setResourceId(MOCK_COMPANY_NUMBER)
                .setResourceKind("company-charges")
                .setResourceUri(String.format("/company/%s/charges", MOCK_COMPANY_NUMBER))
                .setEvent(new EventRecord())
                .setData(chargesRecord)
                .build();

        return MessageBuilder
                .withPayload(resourceChangedData)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "test")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION_ID, "partition_1")
                .setHeader(KafkaHeaders.OFFSET, "offset_1")
                .build();
    }

    private CompanyProfile createCompanyProfile() {
        Data companyProfileData = new Data();
        companyProfileData.setCompanyNumber(MOCK_COMPANY_NUMBER);

        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(companyProfileData);
        return companyProfile;
    }

    private CompanyProfile createCompanyProfileWithChargesLinks() {

        CompanyProfile companyProfile = createCompanyProfile();
        updateWithLinks(companyProfile);
        return companyProfile;
    }

    private void updateWithLinks(CompanyProfile companyProfile) {
        Links links = new Links();
        links.setCharges(String.format("/company/%s/charges", MOCK_COMPANY_NUMBER));
        companyProfile.getData().setLinks(links);
    }

}