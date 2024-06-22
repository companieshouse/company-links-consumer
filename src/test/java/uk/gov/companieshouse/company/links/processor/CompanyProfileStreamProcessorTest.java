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
import org.springframework.web.client.HttpClientErrorException;
import uk.gov.companieshouse.api.appointment.OfficerList;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.exemptions.CompanyExemptions;
import uk.gov.companieshouse.api.exemptions.Exemptions;
import uk.gov.companieshouse.api.exemptions.PscExemptAsSharesAdmittedOnMarketItem;
import uk.gov.companieshouse.api.exemptions.PscExemptAsTradingOnRegulatedMarketItem;
import uk.gov.companieshouse.api.exemptions.PscExemptAsTradingOnUkRegulatedMarketItem;
import uk.gov.companieshouse.api.filinghistory.FilingHistoryList;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.insolvency.CompanyInsolvency;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.api.psc.StatementList;
import uk.gov.companieshouse.company.links.consumer.CompanyProfileStreamConsumer;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.serialization.CompanyProfileDeserializer;
import uk.gov.companieshouse.company.links.service.*;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static uk.gov.companieshouse.company.links.processor.TestData.CONTEXT_ID;
import static uk.gov.companieshouse.company.links.processor.TestData.MOCK_COMPANY_NUMBER;

@ExtendWith(MockitoExtension.class)
class CompanyProfileStreamProcessorTest {

    private CompanyProfileStreamProcessor companyProfileStreamProcessor;
    private CompanyProfileStreamConsumer companyProfileStreamConsumer;
    private TestData testData;

    @Mock
    private Logger logger;
    @Mock
    private CompanyProfileDeserializer companyProfileDeserializer;
    @Mock
    private ChargesService chargesService;
    @Mock
    private CompanyProfileService companyProfileService;
    @Mock
    private ExemptionsListClient exemptionsListClient;
    @Mock
    private AddExemptionsClient addExemptionsClient;
    @Mock
    private FilingHistoryService filingHistoryService;
    @Mock
    private AddFilingHistoryClient addFilingHistoryClient;
    @Mock
    private InsolvencyService insolvencyService;
    @Mock
    private OfficerListClient officerListClient;
    @Mock
    private AddOfficersClient addOfficersClient;
    @Mock
    private PscListClient pscListClient;
    @Mock
    private AddPscClient addPscClient;
    @Mock
    private StatementsListClient statementsListClient;
    @Mock
    private AddStatementsClient addStatementsClient;

    @BeforeEach
    void setUp() {
        companyProfileStreamProcessor = spy(new CompanyProfileStreamProcessor(
                logger, companyProfileDeserializer,
                chargesService, companyProfileService,
                exemptionsListClient, addExemptionsClient,
                filingHistoryService, addFilingHistoryClient,
                insolvencyService,
                officerListClient, addOfficersClient,
                pscListClient, addPscClient,
                statementsListClient, addStatementsClient));
        testData = new TestData();
        DataMapHolder.initialise(CONTEXT_ID);
        companyProfileStreamConsumer = new CompanyProfileStreamConsumer(companyProfileStreamProcessor, logger);
    }

    private void verifyLoggingDataMap() {
        Map<String, Object> dataMap = DataMapHolder.getLogMap();
        assertEquals(CONTEXT_ID, dataMap.get("request_id"));
        assertEquals(MOCK_COMPANY_NUMBER, dataMap.get("company_number"));
    }

    // CHARGES TESTS
    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Charges Link and Charges exist, so the Charges link is updated")
    void successfullyProcessCompanyProfileResourceChangedWhereChargesExistAndNoChargesLinkSoUpdateLink() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setCharges(null);
        assertNull(companyProfile.getLinks().getCharges());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        ApiResponse<ChargesApi> chargesResponse = new ApiResponse<> (200, null, testData.createCharges());
        assertFalse(chargesResponse.getData().getItems().isEmpty());
        when(chargesService.getCharges(any(), any())).thenReturn(chargesResponse);

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any())).thenReturn(
                new ApiResponse<> (200, null));

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any());
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Charges Link and Charges do not exist, so the Charges link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereNoChargesAndNoChargesLinkSoNoLinkUpdate() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setCharges(null);
        assertNull(companyProfile.getLinks().getCharges());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        when(chargesService.getCharges(any(), any())).thenReturn(new ApiResponse<> (200, null, new ChargesApi()) );

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a Charges Link, so the Charges link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereChargesLinkExistsSoNoLinkUpdate() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        assertNotNull(companyProfile.getLinks().getCharges());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Charges Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenChargesDataAPIReturnsNon2XX() throws IOException {
        // given
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
                .thenThrow(new RetryableErrorException("endpoint not found",
                                ApiErrorResponseException.fromHttpResponseException(httpResponseException)));

        // when, then
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws a Non RetryableErrorException when Charges Data API returns non successful response")
    void throwANonRetryableErrorExceptionWhenChargesDataAPIReturnsNon2XX() throws IOException, URIValidationException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setCharges(null);
        assertNull(companyProfile.getLinks().getCharges());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        ApiResponse<ChargesApi> chargesResponse = new ApiResponse<> (409, null, testData.createCharges());
        assertFalse(chargesResponse.getData().getItems().isEmpty());
        when(chargesService.getCharges(any(), any())).thenReturn(chargesResponse);

        HttpClientErrorException conflictException = HttpClientErrorException.create(
                HttpStatus.CONFLICT,
                "Conflict",
                new org.springframework.http.HttpHeaders(),
                null,
                StandardCharsets.UTF_8
        );

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any())).
                thenThrow(conflictException);

        // when, then
        assertThrows(NonRetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any());
        verifyLoggingDataMap();

    }

    //Exemptions TESTS
    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have an Exemptions Link and a PscExemptAsTradingOnRegulatedMarket" +
            " Exemption exist, so the Exemptions link is updated")
    void successfullyProcessCompanyProfileResourceChangedWhereARegulatedMarketExemptionExistAndNoExemptionsLinkSoUpdateLink() throws IOException, URIValidationException {
        // given
        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setExemptions(null);
        assertNull(companyProfile.getLinks().getExemptions());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        CompanyExemptions companyExemptions = testData.createExemptions();
        var exemptions = new Exemptions();
        exemptions.setPscExemptAsTradingOnRegulatedMarket(new PscExemptAsTradingOnRegulatedMarketItem());
        companyExemptions.setExemptions(exemptions);
        assertNotNull(companyExemptions.getExemptions().getPscExemptAsTradingOnRegulatedMarket());
        when(exemptionsListClient.getExemptionsList(any(), any())).thenReturn(companyExemptions);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(addExemptionsClient).patchLink(argument.capture());
        assertEquals(argument.getValue().getCompanyNumber(), MOCK_COMPANY_NUMBER);
        assertEquals(argument.getValue().getRequestId(), CONTEXT_ID);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have an Exemptions Link and a PscExemptAsSharesAdmittedOnMarket" +
            " Exemption exist, so the Exemptions link is updated")
    void successfullyProcessCompanyProfileResourceChangedWhereASharesExemptionExistAndNoExemptionsLinkSoUpdateLink() throws IOException, URIValidationException {
        // given
        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setExemptions(null);
        assertNull(companyProfile.getLinks().getExemptions());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        CompanyExemptions companyExemptions = testData.createExemptions();
        var exemptions = new Exemptions();
        exemptions.setPscExemptAsSharesAdmittedOnMarket(new PscExemptAsSharesAdmittedOnMarketItem());
        companyExemptions.setExemptions(exemptions);
        assertNotNull(companyExemptions.getExemptions().getPscExemptAsSharesAdmittedOnMarket());
        when(exemptionsListClient.getExemptionsList(any(), any())).thenReturn(companyExemptions);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(addExemptionsClient).patchLink(argument.capture());
        assertEquals(argument.getValue().getCompanyNumber(), MOCK_COMPANY_NUMBER);
        assertEquals(argument.getValue().getRequestId(), CONTEXT_ID);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have an Exemptions Link and a PscExemptAsTradingOnUkRegulatedMarket" +
            " Exemption exist, so the Exemptions link is updated")
    void successfullyProcessCompanyProfileResourceChangedWhereAUkMarketExemptionExistAndNoExemptionsLinkSoUpdateLink() throws IOException, URIValidationException {
        // given
        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setExemptions(null);
        assertNull(companyProfile.getLinks().getExemptions());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        CompanyExemptions companyExemptions = testData.createExemptions();
        var exemptions = new Exemptions();
        exemptions.setPscExemptAsTradingOnUkRegulatedMarket(new PscExemptAsTradingOnUkRegulatedMarketItem());
        companyExemptions.setExemptions(exemptions);
        assertNotNull(companyExemptions.getExemptions().getPscExemptAsTradingOnUkRegulatedMarket());
        when(exemptionsListClient.getExemptionsList(any(), any())).thenReturn(companyExemptions);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(addExemptionsClient).patchLink(argument.capture());
        assertEquals(argument.getValue().getCompanyNumber(), MOCK_COMPANY_NUMBER);
        assertEquals(argument.getValue().getRequestId(), CONTEXT_ID);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have an Exemptions Link and a PscExemptAsTradingOnEuRegulatedMarket" +
            " Exemption exist, so the Exemptions link is updated")
    void successfullyProcessCompanyProfileResourceChangedWhereAEuMarketExemptionExistAndNoExemptionsLinkSoUpdateLink() throws IOException, URIValidationException {
        // given
        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setExemptions(null);
        assertNull(companyProfile.getLinks().getExemptions());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        CompanyExemptions companyExemptions = testData.createExemptions();
        companyExemptions.getExemptions().setDisclosureTransparencyRulesChapterFiveApplies(null);
        assertNotNull(companyExemptions.getExemptions().getPscExemptAsTradingOnEuRegulatedMarket());
        when(exemptionsListClient.getExemptionsList(any(), any())).thenReturn(companyExemptions);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(addExemptionsClient).patchLink(argument.capture());
        assertEquals(argument.getValue().getCompanyNumber(), MOCK_COMPANY_NUMBER);
        assertEquals(argument.getValue().getRequestId(), CONTEXT_ID);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have an Exemptions Link and a DisclosureTransparencyRulesChapterFiveApplies" +
            " Exemption exist, so the Exemptions link is updated")
    void successfullyProcessCompanyProfileResourceChangedWhereAChapterFiveExemptionExistAndNoExemptionsLinkSoUpdateLink() throws IOException, URIValidationException {
        // given
        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setExemptions(null);
        assertNull(companyProfile.getLinks().getExemptions());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        CompanyExemptions companyExemptions = testData.createExemptions();
        companyExemptions.getExemptions().setPscExemptAsTradingOnEuRegulatedMarket(null);
        assertNotNull(companyExemptions.getExemptions().getDisclosureTransparencyRulesChapterFiveApplies());
        when(exemptionsListClient.getExemptionsList(any(), any())).thenReturn(companyExemptions);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(addExemptionsClient).patchLink(argument.capture());
        assertEquals(argument.getValue().getCompanyNumber(), MOCK_COMPANY_NUMBER);
        assertEquals(argument.getValue().getRequestId(), CONTEXT_ID);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have an Exemptions Link and Exemptions do not exist, so the Exemptions link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereNoExemptionsAndNoExemptionsLinkSoNoLinkUpdate() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setExemptions(null);
        assertNull(companyProfile.getLinks().getExemptions());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addExemptionsClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have an Exemptions Link, so the Exemptions link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereExemptionsLinkExistsSoNoLinkUpdate() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        assertNotNull(companyProfile.getLinks().getExemptions());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addExemptionsClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Company Exemptions Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenCompanyExemptionsDataAPIReturnsNon2XX() throws IOException, URIValidationException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setExemptions(null);
        assertNull(companyProfile.getLinks().getExemptions());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.SERVICE_UNAVAILABLE.value(),
                HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                new HttpHeaders()).build();
        when(exemptionsListClient.getExemptionsList(any(), any()))
                .thenThrow(new RetryableErrorException("endpoint not found",
                                ApiErrorResponseException.fromHttpResponseException(httpResponseException)));

        // when, then
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Charges Data API returns non successful response but still processes and updates Exemptions link")
    void throwRetryableErrorExceptionWhenChargesDataAPIReturnsNon2XXAndProcessesExemptionsLink() throws IOException, URIValidationException {
        // given
        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setCharges(null);
        companyProfile.getLinks().setExemptions(null);
        assertNull(companyProfile.getLinks().getCharges());
        assertNull(companyProfile.getLinks().getExemptions());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.SERVICE_UNAVAILABLE.value(),
                HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                new HttpHeaders()).build();
        when(chargesService.getCharges(any(), any()))
                .thenThrow(new RetryableErrorException("endpoint not found",
                        ApiErrorResponseException.fromHttpResponseException(httpResponseException)));

        CompanyExemptions companyExemptions = testData.createExemptions();
        var exemptions = new Exemptions();
        exemptions.setPscExemptAsTradingOnRegulatedMarket(new PscExemptAsTradingOnRegulatedMarketItem());
        companyExemptions.setExemptions(exemptions);
        assertNotNull(companyExemptions.getExemptions().getPscExemptAsTradingOnRegulatedMarket());
        when(exemptionsListClient.getExemptionsList(any(), any())).thenReturn(companyExemptions);

        // when, then
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));

        verify(addExemptionsClient).patchLink(argument.capture());
        verifyNoInteractions(companyProfileService);

        verifyLoggingDataMap();
    }

    // FILING HISTORY TESTS
    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a filing history Link and filing history exists, so the filing history link is updated")
    void successfullyProcessCompanyProfileResourceChangedWhereFilingHistoryExistsAndNoFilingHistoryLinkSoUpdateFilingHistoryLinks() throws IOException, URIValidationException {
        // given
        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setFilingHistory(null);
        assertNull(companyProfile.getLinks().getFilingHistory());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        FilingHistoryList filingHistoryListList = testData.createFilingHistoryList();
        assertFalse(filingHistoryListList.getItems().isEmpty());
        when(filingHistoryService.getFilingHistory(any(),any())).thenReturn(filingHistoryListList);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
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
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setFilingHistory(null);
        assertNull(companyProfile.getLinks().getFilingHistory());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        when(filingHistoryService.getFilingHistory(any(),any())).thenReturn(new FilingHistoryList());

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addFilingHistoryClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a filing history Link, so the filing history link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereFilingHistoryLinkExistsSoNoLinkUpdate() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        assertNotNull(companyProfile.getLinks().getFilingHistory());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addFilingHistoryClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Filing History Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenFilingHistoryDataAPIReturnsNon2XX() throws IOException, URIValidationException {
        // given
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
                .thenThrow(new RetryableErrorException("endpoint not found",
                                ApiErrorResponseException.fromHttpResponseException(httpResponseException)));

        // when, then
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verifyLoggingDataMap();
    }

    // INSOLVENCY TESTS
    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Insolvency Link and Insolvencies exist, so the Insolvency link is updated")
    void successfullyProcessCompanyProfileResourceChangedWhereInsolvenciesExistAndNoInsolvencyLinkSoUpdateLink() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setInsolvency(null);
        assertNull(companyProfile.getLinks().getInsolvency());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        ApiResponse<CompanyInsolvency> insolvencyResponse = new ApiResponse<> (200, null, testData.createInsolvency());
        assertFalse(insolvencyResponse.getData().getCases().isEmpty());
        when(insolvencyService.getInsolvency(any(), any())).thenReturn(insolvencyResponse);

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any())).thenReturn(
                new ApiResponse<> (200, null));

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any());
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws a Non RetryableErrorException when Company Insolvency returns non successful response")
    void throwANonRetryableErrorExceptionWhenInsolvencyDataAPIReturnsNon2XX() throws IOException, URIValidationException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setInsolvency(null);
        assertNull(companyProfile.getLinks().getInsolvency());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        ApiResponse<CompanyInsolvency> insolvencyResponse = new ApiResponse<> (409, null, testData.createInsolvency());
        assertFalse(insolvencyResponse.getData().getCases().isEmpty());
        when(insolvencyService.getInsolvency(any(), any())).thenReturn(insolvencyResponse);

        HttpClientErrorException conflictException = HttpClientErrorException.create(
                HttpStatus.CONFLICT,
                "Conflict",
                new org.springframework.http.HttpHeaders(),
                null,
                StandardCharsets.UTF_8
        );

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any())).
                thenThrow(conflictException);

        // when, then
        assertThrows(NonRetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any());
        verifyLoggingDataMap();

    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Insolvency Link and Insolvencies do not exist, so the Insolvency link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereNoInsolvencyAndNoInsolvencyLinkSoNoLinkUpdate() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setInsolvency(null);
        assertNull(companyProfile.getLinks().getInsolvency());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        when(insolvencyService.getInsolvency(any(), any())).thenReturn(
                new ApiResponse<>(200, null, new CompanyInsolvency()));

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a Insolvency Link, so the Insolvency link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereInsolvencyLinkExistsSoNoLinkUpdate() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        assertNotNull(companyProfile.getLinks().getInsolvency());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Insolvency Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenInsolvencyDataAPIReturnsNon2XX() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setInsolvency(null);
        assertNull(companyProfile.getLinks().getInsolvency());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.SERVICE_UNAVAILABLE.value(),
                HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                new HttpHeaders()).build();
        when(insolvencyService.getInsolvency(any(), any()))
                .thenThrow(new RetryableErrorException("endpoint not found",
                                ApiErrorResponseException.fromHttpResponseException(httpResponseException)));

        // when, then
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verifyLoggingDataMap();
    }

    // OFFICERS TESTS
    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Officers Link and Officers exist, so the Officers link is updated")
    void successfullyProcessCompanyProfileResourceChangedWhereOfficersExistAndNoOfficersLinkSoUpdateLink() throws IOException, URIValidationException {
        // given
        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setOfficers(null);
        assertNull(companyProfile.getLinks().getOfficers());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        OfficerList officerList = testData.createOfficers();
        assertFalse(officerList.getItems().isEmpty());
        when(officerListClient.getOfficers(any())).thenReturn(officerList);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(addOfficersClient).patchLink(argument.capture());
        assertEquals(argument.getValue().getCompanyNumber(), MOCK_COMPANY_NUMBER);
        assertEquals(argument.getValue().getRequestId(), CONTEXT_ID);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Officer Link and a Officer does not exist, so the Officer link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereNoOfficerAndNoOfficerLinkSoNoLinkUpdate() throws IOException, URIValidationException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setOfficers(null);
        assertNull(companyProfile.getLinks().getOfficers());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        when(officerListClient.getOfficers(any())).thenReturn(new OfficerList());

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addOfficersClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a Officer Link, so the Officer link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereOfficerLinkExistsSoNoLinkUpdate() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        assertNotNull(companyProfile.getLinks().getOfficers());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addOfficersClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Officer Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenOfficerAPIReturnsNon2XX() throws IOException, URIValidationException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setOfficers(null);
        assertNull(companyProfile.getLinks().getOfficers());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.SERVICE_UNAVAILABLE.value(),
                HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                new HttpHeaders()).build();
        when(officerListClient.getOfficers(any()))
                .thenThrow(new RetryableErrorException("endpoint not found",
                                ApiErrorResponseException.fromHttpResponseException(httpResponseException)));

        // when, then
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verifyLoggingDataMap();
    }

    // PSCS TESTS
    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Psc Link and a Psc exists, so the Psc link is updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscExistsAndNoPscLinkSoUpdatePscLinks() throws IOException {
        // given
        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setPersonsWithSignificantControl(null);
        assertNull(companyProfile.getLinks().getPersonsWithSignificantControl());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        PscList pscList = testData.createPscList();
        assertFalse(pscList.getItems().isEmpty());
        when(pscListClient.getPscs(any())).thenReturn(pscList);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
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
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setPersonsWithSignificantControl(null);
        assertNull(companyProfile.getLinks().getPersonsWithSignificantControl());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        when(pscListClient.getPscs(any())).thenReturn(new PscList());

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addPscClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a Psc Link, so the Psc link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscLinkExistsSoNoLinkUpdate() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        assertNotNull(companyProfile.getLinks().getPersonsWithSignificantControl());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addPscClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Psc Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenPscDataAPIReturnsNon2XX() throws IOException {
        // given
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
                .thenThrow(new RetryableErrorException("endpoint not found",
                                ApiErrorResponseException.fromHttpResponseException(httpResponseException)));

        // when, then
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verifyLoggingDataMap();
    }

    // PSC STATEMENTS TESTS
    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Psc Statements Link and Psc Statements exist, so the Psc Statements link is updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscStatementsExistAndNoPscStatementsLinkSoUpdateLink() throws IOException {
        // given
        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setPersonsWithSignificantControlStatements(null);
        assertNull(companyProfile.getLinks().getPersonsWithSignificantControlStatements());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        StatementList statementList = testData.createStatementList();
        assertFalse(statementList.getItems().isEmpty());
        when(statementsListClient.getStatementsList(any(), any())).thenReturn(statementList);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
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
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        companyProfile.getLinks().setPersonsWithSignificantControlStatements(null);
        assertNull(companyProfile.getLinks().getPersonsWithSignificantControlStatements());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addStatementsClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a Psc Statements Link, so the Psc Statements link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscStatementsLinkExistsSoNoLinkUpdate() throws IOException {
        // given
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileWithLinksMessageWithValidResourceUri();
        Data companyProfile = testData.createCompanyProfileWithLinksFromJson();
        assertNotNull(companyProfile.getLinks().getPersonsWithSignificantControlStatements());
        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);

        // when
        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        // then
        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoInteractions(addStatementsClient);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Psc Statements Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenPscStatementsDataAPIReturnsNon2XX() throws IOException {
        // given
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
                .thenThrow(new RetryableErrorException("endpoint not found",
                                ApiErrorResponseException.fromHttpResponseException(httpResponseException)));

        // when, then
        assertThrows(RetryableErrorException.class,
                () -> companyProfileStreamProcessor.processDelta(mockResourceChangedMessage));
        verifyLoggingDataMap();
    }

}