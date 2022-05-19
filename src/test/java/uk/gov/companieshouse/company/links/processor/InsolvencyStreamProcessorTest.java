package uk.gov.companieshouse.company.links.processor;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static uk.gov.companieshouse.company.links.processor.TestData.CONTEXT_ID;

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
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
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

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        when(companyProfileService.patchCompanyProfile(any(), any(), any())).thenReturn(new ApiResponse<Void>(200, null, null));


        insolvencyProcessor.processDelta(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(logger, times(2)).trace(anyString());
        verify(logger, atLeastOnce()).trace(
                contains("Resource changed message with contextId context_id of kind company-insolvency"));
        verify(logger, atLeastOnce()).trace(String.format(
                "Performing a PATCH with company number %s for contextId %s",
                MOCK_COMPANY_NUMBER, CONTEXT_ID));

        verify(logger, times(2)).info(anyString());
        verify(logger, atLeastOnce()).info("Successfully invoked GET company-profile-api endpoint" +
                " for message with contextId context_id and company number " + MOCK_COMPANY_NUMBER);

        verify(logger, atLeastOnce()).info("Successfully invoked PATCH company-profile-api endpoint" +
                " for message with contextId context_id and company number " + MOCK_COMPANY_NUMBER);
    }


    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData payload, insolvency links doesn't need updating")
    void successfullyProcessResourceChangedDataInsolvencyLinksDoesntGetUpdated() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfileWithInsolvencyLinks());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        insolvencyProcessor.processDelta(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(logger, times(2)).trace(anyString());
        verify(logger, atLeastOnce()).trace(
                contains("Resource changed message with contextId context_id of kind company-insolvency"));
        verify(logger, atLeastOnce()).trace(contains(
                String.format("Company profile with company number %s,"
                        + " contains insolvency links, will not perform PATCH",
                        MOCK_COMPANY_NUMBER)
                ));

        verify(logger, times(1)).info(anyString());
        verify(logger, atLeastOnce()).info("Successfully invoked GET company-profile-api endpoint" +
                " for message with contextId context_id and company number " + MOCK_COMPANY_NUMBER);
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData with deleted event payload, insolvency links removed")
    void successfullyProcessResourceChangedDataWithDeletedEventPayloadInsolvencyLinksRemoved() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessageWithDeletedEvent();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfileWithoutInsolvencyLinks());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        insolvencyProcessor.processDelete(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(logger, times(2)).trace(anyString());
        verify(logger, atLeastOnce()).trace(
                contains("Resource changed message with contextId context_id for deleted event of kind company-insolvency"));
        verify(logger, atLeastOnce()).trace((
                String.format("Company profile with company number %s, does not contain insolvency links," +
                                " will not perform patch for contextId context_id",
                        MOCK_COMPANY_NUMBER)
                ));
        verify(logger, times(1)).info(anyString());
        verify(logger, atLeastOnce()).info("Successfully invoked GET company-profile-api endpoint" +
                " for message with contextId context_id and company number " + MOCK_COMPANY_NUMBER);

    }

    @Test
    @DisplayName("GET company profile returns BAD REQUEST, non retryable error")
    void getCompanyProfileReturnsBadRequest_then_nonRetryableError() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.BAD_REQUEST.value(), null, createCompanyProfileWithInsolvencyLinks());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        assertThrows(NonRetryableErrorException.class, () -> insolvencyProcessor.processDelta(mockResourceChangedMessage));
        verify(logger, times(1)).errorContext(any(), any(), any(), any());
        verify(logger, atLeastOnce()).errorContext(eq(CONTEXT_ID),
                eq("Response from GET company-profile-api"), eq(null), any());
    }

    @Test
    @DisplayName("Company number is blank, non retryable error")
    void invalidJsonThrowsNonRetryableError() throws IOException {
        InputStreamReader exampleInsolvencyJsonPayload = new InputStreamReader(
                Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                        .getResourceAsStream("insolvency-record.json")));
        String insolvencyRecord = FileCopyUtils.copyToString(exampleInsolvencyJsonPayload);

        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
                .setContextId(CONTEXT_ID)
                .setResourceId("")
                .setResourceKind("company-insolvency")
                .setResourceUri(String.format("/company/%s/insolvency", MOCK_COMPANY_NUMBER))
                .setEvent(new EventRecord())
                .setData(insolvencyRecord)
                .build();

        Message<ResourceChangedData> mockResourceChangedMessage = MessageBuilder
                .withPayload(resourceChangedData)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "test")
                .build();

        assertThrows(NonRetryableErrorException.class, () -> insolvencyProcessor.processDelta(mockResourceChangedMessage));
    }

    @Test
    @DisplayName("GET company profile returns 4xx, retryable error")
    void getCompanyProfileReturnsUnauthorized_then_nonRetryableError() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.UNAUTHORIZED.value(), null, createCompanyProfileWithInsolvencyLinks());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        assertThrows(RetryableErrorException.class, () -> insolvencyProcessor.processDelta(mockResourceChangedMessage));
        verify(logger, times(1)).errorContext(any(), any(), any(), any());
        verify(logger, atLeastOnce()).errorContext(eq(CONTEXT_ID),
                eq("Response from GET company-profile-api"), eq(null), any());
    }

    @Test
    @DisplayName("GET company profile returns internal server error, retryable error")
    void getCompanyProfileReturnsInternalServerError_then_nonRetryableError() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.INTERNAL_SERVER_ERROR.value(), null, createCompanyProfileWithInsolvencyLinks());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        assertThrows(RetryableErrorException.class, () -> insolvencyProcessor.processDelta(mockResourceChangedMessage));
        verify(logger, times(1)).errorContext(any(), any(), any(), any());
        verify(logger, atLeastOnce()).errorContext(eq(CONTEXT_ID),
                eq("Response from GET company-profile-api"), eq(null), any());
    }

    @Test
    @DisplayName("PATCH company profile returns BAD REQUEST, non retryable error")
    void patchCompanyProfileReturnsBadRequest_then_nonRetryableError() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileGetApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfile());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileGetApiResponse);

        final ApiResponse<Void> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.BAD_REQUEST.value(), null);

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any()))
                .thenReturn(companyProfileApiResponse);

        assertThrows(NonRetryableErrorException.class, () -> insolvencyProcessor.processDelta(mockResourceChangedMessage));
        verify(logger, times(1)).errorContext(any(), any(), any(), any());
        verify(logger, atLeastOnce()).errorContext(eq(CONTEXT_ID),
                eq("Response from PATCH company-profile-api"), eq(null), any());
    }

    @Test
    @DisplayName("PATCH company profile returns 4xx, retryable error")
    void patchCompanyProfileReturnsUnauthorized_then_nonRetryableError() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileGetApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfile());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileGetApiResponse);

        final ApiResponse<Void> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.UNAUTHORIZED.value(), null);

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any()))
                .thenReturn(companyProfileApiResponse);

        assertThrows(RetryableErrorException.class, () -> insolvencyProcessor.processDelta(mockResourceChangedMessage));
        verify(logger, times(1)).errorContext(any(), any(), any(), any());
        verify(logger, atLeastOnce()).errorContext(eq(CONTEXT_ID),
                eq("Response from PATCH company-profile-api"), eq(null), any());
    }

    @Test
    @DisplayName("PATCH company profile returns internal server error, retryable error")
    void patchCompanyProfileReturnsInternalServerError_then_nonRetryableError() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileGetApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfile());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileGetApiResponse);

        final ApiResponse<Void> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.INTERNAL_SERVER_ERROR.value(), null);

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any()))
                .thenReturn(companyProfileApiResponse);

        assertThrows(RetryableErrorException.class, () -> insolvencyProcessor.processDelta(mockResourceChangedMessage));
        verify(logger, times(1)).errorContext(any(), any(), any(), any());
        verify(logger, atLeastOnce()).errorContext(eq(CONTEXT_ID),
                eq("Response from PATCH company-profile-api"), eq(null), any());
    }

    private Message<ResourceChangedData> createResourceChangedMessage() throws IOException {
        InputStreamReader exampleInsolvencyJsonPayload = new InputStreamReader(
                Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                        .getResourceAsStream("insolvency-record.json")));
        String insolvencyRecord = FileCopyUtils.copyToString(exampleInsolvencyJsonPayload);

        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
                .setContextId(CONTEXT_ID)
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

    private Message<ResourceChangedData> createResourceChangedMessageWithDeletedEvent() throws IOException {
        InputStreamReader exampleInsolvencyJsonPayload = new InputStreamReader(
                Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                        .getResourceAsStream("insolvency-record.json")));
        String insolvencyRecord = FileCopyUtils.copyToString(exampleInsolvencyJsonPayload);
        EventRecord deletedEventRecord = new EventRecord();
        deletedEventRecord.setType("deleted");

        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
                .setContextId(CONTEXT_ID)
                .setResourceId(MOCK_COMPANY_NUMBER)
                .setResourceKind("company-insolvency")
                .setResourceUri(String.format("/company/%s/insolvency", MOCK_COMPANY_NUMBER))
                .setEvent(deletedEventRecord)
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
        companyProfileData.setHasInsolvencyHistory(true);
        Links links = new Links();
        links.setInsolvency(String.format("/company/%s/insolvency", MOCK_COMPANY_NUMBER));
        companyProfileData.setLinks(links);

        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(companyProfileData);
        return companyProfile;
    }

    private CompanyProfile createCompanyProfileWithoutInsolvencyLinks() {
        Data companyProfileData = new Data();
        companyProfileData.setCompanyNumber(MOCK_COMPANY_NUMBER);
        Links links = new Links();
        companyProfileData.setLinks(links);

        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(companyProfileData);
        return companyProfile;
    }

}