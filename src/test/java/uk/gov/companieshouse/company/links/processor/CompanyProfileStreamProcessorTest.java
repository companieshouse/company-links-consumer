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
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.company.links.consumer.CompanyProfileStreamConsumer;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.serialization.CompanyProfileDeserializer;
import uk.gov.companieshouse.company.links.service.AddPscClient;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.company.links.service.PscListClient;
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
    private CompanyProfileService companyProfileService;
    @Mock
    PscListClient pscListClient;
    @Mock
    private Logger logger;
    @Mock
    public AddPscClient pscClient;
    @Mock
    private CompanyProfileDeserializer companyProfileDeserializer;
    private TestData testData;

    @BeforeEach
    void setUp() {
        companyProfileStreamProcessor = spy(new CompanyProfileStreamProcessor(
                companyProfileService,
                pscListClient,
                logger,
                pscClient,
                companyProfileDeserializer));
        testData = new TestData();
        DataMapHolder.initialise(CONTEXT_ID);
        companyProfileStreamConsumer = new CompanyProfileStreamConsumer(companyProfileStreamProcessor, logger);
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Psc Link and a Psc exists, so the Psc link is updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscExistsAndNoPscLinkSoUpdatePscLinks() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileMessageWithValidResourceUri();

        Data companyProfile = testData.createCompanyProfileFromJson();
        assertNull(companyProfile.getLinks().getPersonsWithSignificantControl());

        PscList pscList = testData.createPscList();
        assertTrue(pscList.getTotalResults() > 0);
        final ApiResponse<PscList> pscApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, pscList);

        when(pscListClient.getPscs(any()))
                .thenReturn(pscList);


        ArgumentCaptor<PatchLinkRequest> argument = ArgumentCaptor.forClass(PatchLinkRequest.class);

        when(companyProfileDeserializer.deserialiseCompanyData(mockResourceChangedMessage.getPayload().getData())).thenReturn(companyProfile);


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");


        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);

        verify(pscClient).patchLink(argument.capture());
        assertEquals(argument.getValue().getCompanyNumber(), MOCK_COMPANY_NUMBER);
        assertEquals(argument.getValue().getRequestId(), CONTEXT_ID);
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does not have a Psc Link and a Psc does not exist, so the Psc link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWhereNoPscAndNoPscLinkSoNoLinkUpdate() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileMessageWithValidResourceUri();

        PscList pscList = new PscList();
        pscList.setTotalResults(0);
        final ApiResponse<PscList> pscApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, pscList);


        when(pscListClient.getPscs(any()))
                .thenReturn(new PscList()
                        .totalResults(0));

        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a Company Profile ResourceChanged payload, " +
            "where the Company Profile does have a Psc Link, so the Psc link is not updated")
    void successfullyProcessCompanyProfileResourceChangedWherePscLinkExistsSoNoLinkUpdate() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileMessageWithValidResourceUri();

        Data data = new Data();
        Links links = new Links();
        links.setPersonsWithSignificantControl(String.format("/company/%s/persons-with-significant-control", MOCK_COMPANY_NUMBER));;
        data.setLinks(links);
        when(companyProfileDeserializer.deserialiseCompanyData(any())).thenReturn(data);


        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(companyProfileStreamProcessor).processDelta(mockResourceChangedMessage);
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Psc Data API returns non successful response !2XX")
    void throwRetryableErrorExceptionWhenPscDataAPIReturnsNon2XX() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileMessageWithValidResourceUri();

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

    private void verifyLoggingDataMap() {
        Map<String, Object> dataMap = DataMapHolder.getLogMap();
        assertEquals(CONTEXT_ID, dataMap.get("request_id"));
        assertEquals(MOCK_COMPANY_NUMBER, dataMap.get("company_number"));
    }
}