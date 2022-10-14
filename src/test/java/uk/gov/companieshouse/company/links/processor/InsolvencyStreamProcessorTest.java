package uk.gov.companieshouse.company.links.processor;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
import uk.gov.companieshouse.api.insolvency.CompanyInsolvency;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.service.CompanyInsolvencyService;
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
    private CompanyInsolvencyService companyInsolvencyService;

    @Mock
    private Logger logger;

    @Captor
    private ArgumentCaptor<CompanyProfile> companyProfileCaptor;

    @BeforeEach
    void setUp() {
        insolvencyProcessor = new InsolvencyStreamProcessor(
                companyProfileService,
                logger, companyInsolvencyService);
    }

    @Test
    @DisplayName("UPDATE DELTA - Successfully processes a kafka message containing a ResourceChangedData payload, updating insolvency links where none exist")
    void successfullyProcessResourceChangedDataInsolvencyLinksGetsUpdated() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfile());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        final ApiResponse<CompanyInsolvency> companyInsolvencyGetApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyInsolvency());

        when(companyProfileService.patchCompanyProfile(any(), any(), any())).thenReturn(new ApiResponse<Void>(200, null, null));

        when(companyInsolvencyService.getCompanyInsolvency(
                CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyInsolvencyGetApiResponse);

        when(companyProfileService.patchCompanyProfile(any(), any(), any())).thenReturn(new ApiResponse<Void>(200, null, null));

        insolvencyProcessor.processDelta(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(companyProfileService, times(1)).patchCompanyProfile(
                eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), companyProfileCaptor.capture());
        assertEquals(
                "/company/" + MOCK_COMPANY_NUMBER + "/insolvency",
                companyProfileCaptor.getValue().getData().getLinks().getInsolvency());

        verify(logger, atLeastOnce()).trace(
                contains("Resource changed message with contextId context_id of kind company-insolvency"));
        verify(logger, times(2)).info(anyString());
        verify(logger, atLeastOnce()).info("Successfully invoked GET company-profile-api endpoint" +
                " for message with contextId context_id and company number " + MOCK_COMPANY_NUMBER);
        verify(logger, atLeastOnce()).info("Successfully invoked PATCH company-profile-api endpoint" +
                " for message with contextId context_id and company number " + MOCK_COMPANY_NUMBER);
    }


    @Test
    @DisplayName("UPDATE DELTA - Successfully processes a kafka message containing a ResourceChangedData payload, insolvency links exist and do not need updating")
    void successfullyProcessResourceChangedDataInsolvencyLinksDoesntGetUpdated() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfileWithInsolvencyLinks());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        insolvencyProcessor.processDelta(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(companyProfileService, never()).patchCompanyProfile(anyString(), anyString(), any(CompanyProfile.class));

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
    @DisplayName("DELETE DELTA - Successfully processes a kafka message containing a ResourceChangedData with deleted event payload, insolvency links do not exist")
    void successfullyProcessResourceChangedDataWithDeletedEventPayloadInsolvencyLinksRemoved() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessageWithDeletedEvent();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfileWithoutInsolvencyLinks());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        insolvencyProcessor.processDelete(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(companyProfileService, never()).patchCompanyProfile(anyString(), anyString(), any(CompanyProfile.class));
        verify(logger, times(2)).trace(anyString());
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
    @DisplayName("DELETE DELTA - Successfully processes a kafka message containing a ResourceChangedData with deleted event payload, insolvency links exist and are deleted")
    void successfullyProcessResourceChangedDataWithDeletedEventPayloadWithInsolvencyLinks() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessageWithDeletedEvent();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfileWithInsolvencyLinks());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        final ApiResponse<CompanyInsolvency> companyInsolvencyGetApiResponse = new ApiResponse<>(
                HttpStatus.NOT_FOUND.value(), null, null);

        when(companyInsolvencyService.getCompanyInsolvency(
                CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyInsolvencyGetApiResponse);

        when(companyProfileService.patchCompanyProfile(any(), any(), any())).thenReturn(new ApiResponse<Void>(200, null, null));

        insolvencyProcessor.processDelete(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(companyProfileService, times(1)).patchCompanyProfile(
                eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), companyProfileCaptor.capture());
        assertNull(companyProfileCaptor.getValue().getData().getLinks().getInsolvency());

        verify(logger, times(2)).info(anyString());
        verify(logger, atLeastOnce()).info("Successfully invoked GET company-profile-api endpoint" +
                " for message with contextId context_id and company number " + MOCK_COMPANY_NUMBER);
        verify(logger, atLeastOnce()).info("Successfully invoked PATCH company-profile-api endpoint" +
                " for message with contextId context_id and company number " + MOCK_COMPANY_NUMBER);
    }

    @Test
    @DisplayName("DELETE DELTA - When GET insolvency returns a 200 response throw a RetryableError, links exist but are not deleted")
    void getInsolvencyReturns200_then_RetryableError() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileGetApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfileWithInsolvencyLinks());

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileGetApiResponse);

        final ApiResponse<CompanyInsolvency> companyInsolvencyGetApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, null);

        when(companyInsolvencyService.getCompanyInsolvency(
                CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyInsolvencyGetApiResponse);

        assertThrows(RetryableErrorException.class, () -> insolvencyProcessor.processDelete(mockResourceChangedMessage));
        verify(companyProfileService, never()).patchCompanyProfile(anyString(), anyString(), any(CompanyProfile.class));
        verify(logger, times(1)).errorContext(any(), any(), any(), any());
        verify(logger, atLeastOnce()).errorContext(eq(CONTEXT_ID),
                eq("Response from insolvency-data-api service, main delta is not yet deleted, throwing retry-able exception to check again"), eq(null), any());
    }

    @Test
    @DisplayName("UPDATE DELTA - Company number is blank, non retryable error")
    void updateInvalidResourceIdThrowsNonRetryableError() {
        var event = new EventRecord();
        event.setType("changed");
        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
                .setContextId(CONTEXT_ID)
                .setResourceId("")
                .setResourceKind("company-insolvency")
                .setResourceUri(String.format("/company/%s/insolvency", MOCK_COMPANY_NUMBER))
                .setEvent(new EventRecord())
                .setData("")
                .build();

        Message<ResourceChangedData> mockResourceChangedMessage = MessageBuilder
                .withPayload(resourceChangedData)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "test")
                .build();

        assertThrows(NonRetryableErrorException.class, () -> insolvencyProcessor.processDelta(mockResourceChangedMessage));
        verifyNoInteractions(companyProfileService);

    }

    @Test
    @DisplayName("DELETE DELTA - Company number is blank, non retryable error")
    void deleteInvalidResourceIdThrowsNonRetryableError() {
        var event = new EventRecord();
        event.setType("deleted");
        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
                .setContextId(CONTEXT_ID)
                .setResourceId("")
                .setResourceKind("company-insolvency")
                .setResourceUri(String.format("/company/%s/insolvency", MOCK_COMPANY_NUMBER))
                .setEvent(event)
                .setData("")
                .build();

        Message<ResourceChangedData> mockResourceChangedMessage = MessageBuilder
                .withPayload(resourceChangedData)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "test")
                .build();

        assertThrows(NonRetryableErrorException.class, () -> insolvencyProcessor.processDelete(mockResourceChangedMessage));
        verifyNoInteractions(companyProfileService);
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
    @DisplayName("GET company profile returns 410 DOCUMENT GONE throws Non Retryable error")
    void getCompanyProfileReturnsDocumentGone_then_nonRetryableError() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.GONE.value(), null, null);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        assertThrows(NonRetryableErrorException.class, () -> insolvencyProcessor.processDelta(mockResourceChangedMessage));
        verify(logger, times(1)).errorContext(any(), any(), any(), any());
        verify(logger, atLeastOnce()).errorContext(eq(CONTEXT_ID),
                eq("Response from GET company-profile-api"), eq(null), any());
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

        final ApiResponse<CompanyInsolvency> companyInsolvencyGetApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyInsolvency());

        when(companyInsolvencyService.getCompanyInsolvency(
                CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyInsolvencyGetApiResponse);

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

        final ApiResponse<CompanyInsolvency> companyInsolvencyGetApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyInsolvency());

        when(companyInsolvencyService.getCompanyInsolvency(
                CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyInsolvencyGetApiResponse);

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

        final ApiResponse<CompanyInsolvency> companyInsolvencyGetApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyInsolvency());

        when(companyInsolvencyService.getCompanyInsolvency(
                CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyInsolvencyGetApiResponse);

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

    private CompanyInsolvency createCompanyInsolvency() {

        CompanyInsolvency companyInsolvency = new CompanyInsolvency();
        companyInsolvency.setEtag("etag");
        return companyInsolvency;
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