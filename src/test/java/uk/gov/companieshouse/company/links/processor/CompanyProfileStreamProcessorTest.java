package uk.gov.companieshouse.company.links.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.company.links.consumer.CompanyProfileStreamConsumer;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.company.links.service.PscService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static uk.gov.companieshouse.company.links.processor.TestData.CONTEXT_ID;
import static uk.gov.companieshouse.company.links.processor.TestData.MOCK_COMPANY_NUMBER;

@ExtendWith(MockitoExtension.class)
class CompanyProfileStreamProcessorTest {

    private CompanyProfileStreamProcessor companyProfileStreamProcessor;
    private CompanyProfileStreamConsumer companyProfileStreamConsumer;
    @Mock
    private CompanyProfileService companyProfileService;
    @Mock
    PscService pscService;
    @Mock
    private Logger logger;
    private TestData testData;

    @BeforeEach
    void setUp() {
        companyProfileStreamProcessor = spy(new CompanyProfileStreamProcessor(
                companyProfileService,
                pscService,
                logger));
        testData = new TestData();
        DataMapHolder.initialise(CONTEXT_ID);
        companyProfileStreamConsumer = new CompanyProfileStreamConsumer(companyProfileStreamProcessor, logger);
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Psc Link and a Psc exists, so the Psc link is updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscExistsAndNoPscLinkSoUpdatePscLinks() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileMessageWithValidResourceUri();

        CompanyProfile companyProfile = testData.createCompanyProfileFromJson();
        assertNull(companyProfile.getData().getLinks().getPersonsWithSignificantControl());
        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        PscList pscList = testData.createPscList();
        assertTrue(pscList.getTotalResults() > 0);
        final ApiResponse<PscList> pscApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, pscList);

        final ApiResponse<CompanyProfile> updatedCompanyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, null);

        ArgumentCaptor<CompanyProfile> argument = ArgumentCaptor.forClass(CompanyProfile.class);
        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), (argument.capture()))).thenAnswer(
                invocationOnMock -> updatedCompanyProfileApiResponse);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);
        when(pscService.getPscList(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(pscApiResponse);

        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any(CompanyProfile.class));
        // Check that the profile has been correctly updated for the call
        assertEquals(1, argument.getAllValues().size()); // Should have only captured 1 argument
        assertNotNull(argument.getValue().getData().getLinks()); // We have links
        String pscLink = argument.getValue().getData().getLinks().getPersonsWithSignificantControl();
        assertEquals(String.format("/company/%s/persons-with-significant-control", MOCK_COMPANY_NUMBER), pscLink); // And the PSCs link is as expected
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Psc Link and a Psc does not exist, so the Psc link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereNoPscAndNoPscLinkSoNoLinkUpdate() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileMessageWithValidResourceUri();

        CompanyProfile companyProfile = testData.createCompanyProfileFromJson();
        assertNull(companyProfile.getData().getLinks().getPersonsWithSignificantControl());
        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        PscList pscList = new PscList();
        pscList.setTotalResults(0);
        final ApiResponse<PscList> pscApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, pscList);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);
        when(pscService.getPscList(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(pscApiResponse);

        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a Psc Link, so the Psc link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscLinkExistsSoNoLinkUpdate() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileMessageWithValidResourceUri();

        CompanyProfile companyProfile = testData.createCompanyProfileFromJson();
        companyProfile.getData().getLinks().setPersonsWithSignificantControl(
                String.format("/company/%s/persons-with-significant-control", MOCK_COMPANY_NUMBER));
        assertNotNull(companyProfile.getData().getLinks().getPersonsWithSignificantControl());
        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when CompanyProfile service is unavailable")
    void throwRetryableErrorExceptionWhenCompanyProfileServiceIsUnavailable() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileMessageWithValidResourceUri();

        CompanyProfile companyProfile = testData.createCompanyProfileFromJson();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.SERVICE_UNAVAILABLE.value(), null, companyProfile);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        assertThrows(RetryableErrorException.class, () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));

        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(companyProfileService, never()).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws NonRetryableErrorException when patch CompanyProfile returns Bad Request")
    void throwNonRetryableErrorExceptionWhenPatchCompanyProfileReturnsBadRequest() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileMessageWithValidResourceUri();

        CompanyProfile companyProfile = testData.createCompanyProfileFromJson();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        PscList pscList = testData.createPscList();
        assertTrue(pscList.getTotalResults() > 0);
        final ApiResponse<PscList> pscApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, pscList);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);
        when(companyProfileService.patchCompanyProfile(any(), any(), any()))
                .thenReturn(new ApiResponse<>(
                        HttpStatus.BAD_REQUEST.value(), null, null));
        when(pscService.getPscList(any(), any())).thenReturn(pscApiResponse);
        assertThrows(NonRetryableErrorException.class, () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));

        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Psc Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenPscDataAPIReturnsNon2XX() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileMessageWithValidResourceUri();

        CompanyProfile companyProfile = testData.createCompanyProfileFromJson();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);
        when(pscService.getPscList(any(), any()))
                .thenReturn(new ApiResponse<>(
                        HttpStatus.NOT_FOUND.value(), null, null));
        assertThrows(RetryableErrorException.class, () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verifyLoggingDataMap();
    }

    private void verifyLoggingDataMap() {
        Map<String, Object> dataMap = DataMapHolder.getLogMap();
        assertEquals(CONTEXT_ID, dataMap.get("request_id"));
        assertEquals(MOCK_COMPANY_NUMBER, dataMap.get("company_number"));
    }
}