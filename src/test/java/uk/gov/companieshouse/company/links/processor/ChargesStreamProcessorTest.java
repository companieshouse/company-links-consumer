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
    @DisplayName("Checks if company profile has links and should return false if there are no charges inside links")
    void doesCompanyProfileHaveCharges_should_return_false() throws IOException {
        CompanyProfile companyProfile = createCompanyProfile();

        assertTrue(!chargesStreamProcessor.doesCompanyProfileHaveCharges(MOCK_COMPANY_NUMBER,
               companyProfile.getData()));

    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData payload, links doesn't need updating")
    void successfullyProcessResourceChangedDataChargesDoesntGetUpdated() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        CompanyProfile companyProfileWithLinks = createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfileWithLinks);

        when(companyProfileService.getCompanyProfile("context_id", MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        chargesStreamProcessor.process(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile("context_id", MOCK_COMPANY_NUMBER);
        verify(companyProfileService, times(0)).patchCompanyProfile("context_id", MOCK_COMPANY_NUMBER,
                companyProfileWithLinks);
    }

    @Test
    @DisplayName("Checks if company profile has links and should return true if there are charges inside links")
    void doesCompanyProfileHaveCharges_should_return_true() throws IOException {

        CompanyProfile companyProfileWithLinks = createCompanyProfileWithChargesLinks();

        assertTrue(chargesStreamProcessor.doesCompanyProfileHaveCharges(MOCK_COMPANY_NUMBER,
                companyProfileWithLinks.getData()));

    }

    @Test
    @DisplayName("process CompanyProfile Updates where charges inside links are updated successfully")
    void processCompanyProfileUpdates_SuccessfullyChargesLinksGetsUpdated() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        CompanyProfile companyProfile = createCompanyProfile();
        CompanyProfile companyProfileWithLinks = createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        final ApiResponse<Void> updatedCompanyProfileApiResponse = new ApiResponse<Void>(
                HttpStatus.OK.value(), null, null);

        when(companyProfileService.patchCompanyProfile("context_id", MOCK_COMPANY_NUMBER,
                companyProfile))
                .thenReturn(updatedCompanyProfileApiResponse);

        chargesStreamProcessor.processCompanyProfileUpdates(
                "context_id", MOCK_COMPANY_NUMBER, companyProfileApiResponse,
                mockResourceChangedMessage.getPayload(), mockResourceChangedMessage.getHeaders());

        verify(companyProfileService).patchCompanyProfile("context_id", MOCK_COMPANY_NUMBER,
                companyProfileWithLinks);

    }

    @Test
    @DisplayName("process CompanyProfile Updates where charges inside links are not updated successfully")
    void processCompanyProfileUpdates_ChargesLinksNotUpdated() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        CompanyProfile companyProfileWithLinks = createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfileWithLinks);

        final ApiResponse<Void> updatedCompanyProfileApiResponse = new ApiResponse<Void>(
                HttpStatus.OK.value(), null, null);

        chargesStreamProcessor.processCompanyProfileUpdates(
                "context_id", MOCK_COMPANY_NUMBER, companyProfileApiResponse,
                mockResourceChangedMessage.getPayload(), mockResourceChangedMessage.getHeaders());

        verify(companyProfileService, times(0)).
                patchCompanyProfile("context_id", MOCK_COMPANY_NUMBER,
                companyProfileWithLinks);

    }

    @Test
    @DisplayName("update CompanyProfile With Charges")
    void updateCompanyProfileWithCharges() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        CompanyProfile companyProfile = createCompanyProfile();
        CompanyProfile companyProfileWithLinks = createCompanyProfileWithChargesLinks();

        final ApiResponse<Void> updatedCompanyProfileApiResponse = new ApiResponse<Void>(
                HttpStatus.OK.value(), null, null);

        when(companyProfileService.patchCompanyProfile("context_id", MOCK_COMPANY_NUMBER,
                companyProfile))
                .thenReturn(updatedCompanyProfileApiResponse);

        chargesStreamProcessor.updateCompanyProfileWithCharges("context_id", MOCK_COMPANY_NUMBER,
                companyProfile.getData(), mockResourceChangedMessage.getPayload(),
                mockResourceChangedMessage.getHeaders());

        verify(companyProfileService).patchCompanyProfile("context_id", MOCK_COMPANY_NUMBER,
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