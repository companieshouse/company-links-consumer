package uk.gov.companieshouse.company.links.processor;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.filinghistory.FilingHistoryList;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.api.psc.StatementList;
import uk.gov.companieshouse.company.links.consumer.CompanyProfileStreamConsumer;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.serialization.CompanyProfileDeserializer;
import uk.gov.companieshouse.company.links.service.*;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
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
    private Logger logger;
    @Mock
    private CompanyProfileDeserializer companyProfileDeserializer;
    @Mock
    ChargesService chargesService;
    @Mock
    PscListClient pscListClient;
    @Mock
    StatementsListClient statementsListClient;
    @Mock
    public AddChargesClient addChargesClient;
    @Mock
    public AddPscClient addPscClient;
    @Mock
    public AddFilingHistoryClient addFilingHistoryClient;
    @Mock
    FilingHistoryService filingHistoryService;
    @Mock
    public AddStatementsClient addStatementsClient;
    private TestData testData;

    @BeforeEach
    void setUp() {
        companyProfileStreamProcessor = spy(new CompanyProfileStreamProcessor(
                logger, companyProfileDeserializer,
                chargesService, addChargesClient,
                filingHistoryService, addFilingHistoryClient,
                pscListClient, addPscClient,
                statementsListClient, addStatementsClient));
        testData = new TestData();
        DataMapHolder.initialise(CONTEXT_ID);
        companyProfileStreamConsumer = new CompanyProfileStreamConsumer(companyProfileStreamProcessor, logger);
    }

    //CHARGES TESTS
    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Charges Link and Charges exist, so the Charges link is updated")
    void successfullyProcessCompanyProfileResourceChangedWhereChargesExistAndNoChargesLinkSoUpdateLink() throws IOException {

        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setCharges(null);
        assertNull(companyProfile.getLinks().getCharges());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        ApiResponse<ChargesApi> chargesResponse = new ApiResponse<> (200, null, testData.createCharges());
        assertFalse(chargesResponse.getData().getItems().isEmpty());
        when(chargesService.getCharges(any(), any())).thenReturn(chargesResponse);


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");


        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(addChargesClient).patchLink(argument.capture());
        assertEquals(argument.getValue().getCompanyNumber(), MOCK_COMPANY_NUMBER);
        assertEquals(argument.getValue().getRequestId(), CONTEXT_ID);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Charges Link and Charges do not exist, so the Charges link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereNoChargesAndNoChargesLinkSoNoLinkUpdate() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setCharges(null);
        assertNull(companyProfile.getLinks().getCharges());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        when(chargesService.getCharges(any(), any())).thenReturn(new ApiResponse<> (200, null, new ChargesApi()) );


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");


        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addChargesClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a Charges Link, so the Charges link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereChargesLinkExistsSoNoLinkUpdate() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        assertNotNull(companyProfile.getLinks().getCharges());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");


        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addChargesClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Charges Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenChargesDataAPIReturnsNon2XX() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setCharges(null);
        assertNull(companyProfile.getLinks().getCharges());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.SERVICE_UNAVAILABLE.value(),
                HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                new HttpHeaders()).build();
        when(chargesService.getCharges(any(), any()))
                .thenThrow(
                        new RetryableErrorException("endpoint not found",
                                ApiErrorResponseException.fromHttpResponseException(httpResponseException)));
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verifyLoggingDataMap();
    }

    // FILING HISTORY TESTS
    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a filing history Link and filing history exists, so the filing history link is updated")
    void successfullyProcessCompanyProfileResourceChangedWhereFilingHistoryExistsAndNoFilingHistoryLinkSoUpdateFilingHistoryLinks() throws IOException, URIValidationException {

        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setFilingHistory(null);
        assertNull(companyProfile.getLinks().getFilingHistory());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        FilingHistoryList filingHistoryListList = testData.createFilingHistoryList();
        assertFalse(filingHistoryListList.getItems().isEmpty());
        when(filingHistoryService.getFilingHistory(any(),any())).thenReturn(filingHistoryListList);


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");


        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(addFilingHistoryClient).patchLink(argument.capture());
        assertEquals(argument.getValue().getCompanyNumber(), MOCK_COMPANY_NUMBER);
        assertEquals(argument.getValue().getRequestId(), CONTEXT_ID);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a filing history Link and filing history does not exist, so the filing history link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereNoFilingHistoryAndNoFilingHistoryLinkSoNoLinkUpdate() throws IOException, URIValidationException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setFilingHistory(null);
        assertNull(companyProfile.getLinks().getFilingHistory());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        when(filingHistoryService.getFilingHistory(any(),any())).thenReturn(new FilingHistoryList());


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");


        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addFilingHistoryClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a filing history Link, so the filing history link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereFilingHistoryLinkExistsSoNoLinkUpdate() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        assertNotNull(companyProfile.getLinks().getFilingHistory());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");


        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addFilingHistoryClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Filing History Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenFilingHistoryDataAPIReturnsNon2XX() throws IOException, URIValidationException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setFilingHistory(null);
        assertNull(companyProfile.getLinks().getFilingHistory());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.SERVICE_UNAVAILABLE.value(),
                HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                new HttpHeaders()).build();
        when(filingHistoryService.getFilingHistory(any(),any()))
                .thenThrow(
                        new RetryableErrorException("endpoint not found",
                                ApiErrorResponseException.fromHttpResponseException(httpResponseException)));
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verifyLoggingDataMap();
    }

    //PSCS TESTS
    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Psc Link and a Psc exists, so the Psc link is updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscExistsAndNoPscLinkSoUpdatePscLinks() throws IOException {

        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setPersonsWithSignificantControl(null);
        assertNull(companyProfile.getLinks().getPersonsWithSignificantControl());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        PscList pscList = testData.createPscList();
        assertFalse(pscList.getItems().isEmpty());
        when(pscListClient.getPscs(any())).thenReturn(pscList);


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");


        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(addPscClient).patchLink(argument.capture());
        assertEquals(argument.getValue().getCompanyNumber(), MOCK_COMPANY_NUMBER);
        assertEquals(argument.getValue().getRequestId(), CONTEXT_ID);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Psc Link and a Psc does not exist, so the Psc link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereNoPscAndNoPscLinkSoNoLinkUpdate() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setPersonsWithSignificantControl(null);
        assertNull(companyProfile.getLinks().getPersonsWithSignificantControl());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        when(pscListClient.getPscs(any())).thenReturn(new PscList());


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");


        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addPscClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a Psc Link, so the Psc link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscLinkExistsSoNoLinkUpdate() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        assertNotNull(companyProfile.getLinks().getPersonsWithSignificantControl());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");


        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addPscClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Psc Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenPscDataAPIReturnsNon2XX() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setPersonsWithSignificantControl(null);
        assertNull(companyProfile.getLinks().getPersonsWithSignificantControl());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.SERVICE_UNAVAILABLE.value(),
                HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                new HttpHeaders()).build();
        when(pscListClient.getPscs(any()))
                .thenThrow(
                        new RetryableErrorException("endpoint not found",
                                ApiErrorResponseException.fromHttpResponseException(httpResponseException)));
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verifyLoggingDataMap();
    }

    //PSC Statements TESTS
    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Psc Statements Link and Psc Statements exist, so the Psc Statements link is updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscStatementsExistAndNoPscStatementsLinkSoUpdateLink() throws IOException {

        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setPersonsWithSignificantControlStatements(null);
        assertNull(companyProfile.getLinks().getPersonsWithSignificantControlStatements());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        StatementList statementList = testData.createStatementList();
        assertFalse(statementList.getItems().isEmpty());
        when(statementsListClient.getStatementsList(any(), any())).thenReturn(statementList);

        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(addStatementsClient).patchLink(argument.capture());
        assertEquals(argument.getValue().getCompanyNumber(), MOCK_COMPANY_NUMBER);
        assertEquals(argument.getValue().getRequestId(), CONTEXT_ID);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Psc Statements Link and Psc Statements do not exist, so the Psc Statements link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereNoPscStatementsAndNoPscStatementsLinkSoNoLinkUpdate() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setPersonsWithSignificantControlStatements(null);
        assertNull(companyProfile.getLinks().getPersonsWithSignificantControlStatements());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addStatementsClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a Psc Statements Link, so the Charges link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscStatementsLinkExistsSoNoLinkUpdate() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        assertNotNull(companyProfile.getLinks().getPersonsWithSignificantControlStatements());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");


        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addStatementsClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Psc Statements Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenPscStatementsDataAPIReturnsNon2XX() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setPersonsWithSignificantControlStatements(null);
        assertNull(companyProfile.getLinks().getPersonsWithSignificantControlStatements());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.SERVICE_UNAVAILABLE.value(),
                HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                new HttpHeaders()).build();
        when(statementsListClient.getStatementsList(any(), any()))
                .thenThrow(
                        new RetryableErrorException("endpoint not found",
                                ApiErrorResponseException.fromHttpResponseException(httpResponseException)));
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verifyLoggingDataMap();
    }

    private void verifyLoggingDataMap() {
        Map<String, Object> dataMap = DataMapHolder.getLogMap();
        assertEquals(CONTEXT_ID, dataMap.get("request_id"));
        assertEquals(MOCK_COMPANY_NUMBER, dataMap.get("company_number"));
    }
}