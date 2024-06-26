package uk.gov.companieshouse.company.links.processor;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import uk.gov.companieshouse.api.charges.ChargeApi;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.consumer.ChargesStreamConsumer;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.service.ChargesService;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.companieshouse.company.links.processor.TestData.CONTEXT_ID;
import static uk.gov.companieshouse.company.links.processor.TestData.MOCK_COMPANY_NUMBER;

/**
 * Originally this tested the Charges Stream Processor which was fine whe it had only one method.
 * Now it has 2 so we need the make sure to consumer calls the correct method.
 */
@ExtendWith(MockitoExtension.class)
class ChargesStreamConsumerTest {

    private ChargesStreamProcessor chargesStreamProcessor;

    private ChargesStreamConsumer chargesStreamConsumer;

    @Mock
    private CompanyProfileService companyProfileService;

    @Mock
    ChargesService chargesService;

    @Mock
    private Logger logger;

    private TestData testData;

    @BeforeEach
    void setUp() {
        chargesStreamProcessor = spy(new ChargesStreamProcessor(
                companyProfileService,
                chargesService,
                logger));
        testData = new TestData();
        DataMapHolder.initialise(CONTEXT_ID);
        chargesStreamConsumer = new ChargesStreamConsumer(chargesStreamProcessor, logger);
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData delete payload, profile without charges links")
    void successfullyProcessResourceChangedDataChargesDelete() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithDelete();

        CompanyProfile companyProfile = testData.createCompanyProfile();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        chargesStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(chargesStreamProcessor).processDelete(mockResourceChangedMessage);
        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(companyProfileService, never()).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData delete payload, profile with charges links, no current charges")
    void successfullyProcessResourceChangedDataChargesDeleteWithLinks() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithDelete();

        CompanyProfile companyProfile = testData.createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
            HttpStatus.OK.value(), null, companyProfile);
        final ApiResponse<CompanyProfile> updatedCompanyProfileApiResponse = new ApiResponse<>(
            HttpStatus.OK.value(), null, null);

        final ChargesApi noCharges = new ChargesApi();
        noCharges.setTotalCount(0);
        final ApiResponse<ChargesApi> chargesApiResponseNoCharges = new ApiResponse<>(
            HttpStatus.OK.value(), null, noCharges);

        ArgumentCaptor<CompanyProfile> argument = ArgumentCaptor.forClass(CompanyProfile.class);
        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), (argument.capture()))).thenAnswer(
            invocationOnMock -> updatedCompanyProfileApiResponse);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
            .thenReturn(companyProfileApiResponse);

        when(chargesService.getCharges(CONTEXT_ID, MOCK_COMPANY_NUMBER)).thenReturn(chargesApiResponseNoCharges);

        chargesStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(chargesStreamProcessor).processDelete(mockResourceChangedMessage);
        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        // dont need to know that this method is called with a specific value just that it was called
        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
            any(CompanyProfile.class));
        // here we should check that the profile has been correctly updated for the call
        assertEquals(1, argument.getAllValues().size()); // should have only captured 1 argument
        Data data = argument.getValue().getData();
        assertNotNull(data.getLinks()); // we have links
        assertNull(data.getLinks().getCharges()); // and the charges link is as expected
        assertFalse(data.getHasCharges());
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData delete payload, profile with charges links and current charges")
    void successfullyProcessResourceChangedDataChargesDeleteWithLinksAndCharges() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithDelete();

        CompanyProfile companyProfile = testData.createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
            HttpStatus.OK.value(), null, companyProfile);

        final ChargesApi hasCharges = new ChargesApi();
        hasCharges.setTotalCount(1);
        ChargeApi chargeApi = new ChargeApi();
        chargeApi.setId("123");
        hasCharges.getItems().add(chargeApi);
        final ApiResponse<ChargesApi> chargesApiResponseNoCharges = new ApiResponse<>(
            HttpStatus.OK.value(), null, hasCharges);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
            .thenReturn(companyProfileApiResponse);

        when(chargesService.getCharges(CONTEXT_ID, MOCK_COMPANY_NUMBER)).thenReturn(chargesApiResponseNoCharges);

        assertThrows(RetryableErrorException.class, () ->chargesStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset"));

        verify(chargesStreamProcessor).processDelete(mockResourceChangedMessage);
        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        // dont need to know that this method is called with a specific value just that it was called
        verify(companyProfileService, never()).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
            any(CompanyProfile.class));
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    /*
     * Original ChargesStreamProcessor test update to verify that consumer is calling correct method
     * Did not update al just a couple to prove that it works.
     */

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData payload, updating charges links")
    void successfullyProcessResourceChangedDataChargesLinksGetsUpdated() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithValidResourceUri();

        CompanyProfile companyProfile = testData.createCompanyProfile();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);
        final ApiResponse<CompanyProfile> updatedCompanyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, null);

        ArgumentCaptor<CompanyProfile> argument = ArgumentCaptor.forClass(CompanyProfile.class);
        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), (argument.capture()))).thenAnswer(
                invocationOnMock -> updatedCompanyProfileApiResponse);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);
        when(chargesService.getACharge(any(), any()))
                .thenReturn(new ApiResponse<>(
                        HttpStatus.OK.value(), null, null));

        chargesStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(chargesStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        // dont need to know that this method is called with a specific value just that it was called
        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
        // here we should check that the profile has been correctly updated for the call
        assertEquals(1, argument.getAllValues().size()); // should have only captured 1 argument
        assertNotNull(argument.getValue().getData().getLinks()); // we have links
        String links = argument.getValue().getData().getLinks().getCharges();
        assertEquals(TestData.ALL_COMPANY_CHARGES_LINK, links); // and the charges link is as expected
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Checks if company profile has links and should return false if there are no charges inside links")
    void doesCompanyProfileHaveCharges_should_return_false() {
        CompanyProfile companyProfile = testData.createCompanyProfile();

        assertFalse(chargesStreamProcessor.doesCompanyProfileHaveCharges(CONTEXT_ID, MOCK_COMPANY_NUMBER,
            companyProfile.getData().getLinks()));
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData payload, links doesn't need updating")
    void successfullyProcessResourceChangedDataChargesDoesntGetUpdated() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithValidResourceUri();

        CompanyProfile companyProfileWithLinks = testData.createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfileWithLinks);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        chargesStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(chargesStreamProcessor).processDelta(mockResourceChangedMessage);
        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(companyProfileService, never()).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));

        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("Checks if company profile has links and should return true if there are charges inside links")
    void doesCompanyProfileHaveCharges_should_return_true() {

        CompanyProfile companyProfileWithLinks = testData.createCompanyProfileWithChargesLinks();

        assertTrue(chargesStreamProcessor.doesCompanyProfileHaveCharges(CONTEXT_ID, MOCK_COMPANY_NUMBER,
                companyProfileWithLinks.getData().getLinks()));
    }

    @Test
    @DisplayName("process CompanyProfile Updates where charges inside links are updated successfully")
    void processCompanyProfileUpdates_SuccessfullyChargesLinksGetsUpdated() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithValidResourceUri();

        CompanyProfile companyProfile = testData.createCompanyProfile();

        final ApiResponse<CompanyProfile> companyProfileApiGetResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenAnswer(invocation -> companyProfileApiGetResponse);

        final ApiResponse<Void> updatedCompanyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, null);
        ArgumentCaptor<CompanyProfile> argument = ArgumentCaptor.forClass(CompanyProfile.class);

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                        (argument.capture())))
                .thenAnswer(invocation -> updatedCompanyProfileApiResponse);
        when(chargesService.getACharge(any(), any()))
                .thenReturn(new ApiResponse<>(
                        HttpStatus.OK.value(), null, null));

        chargesStreamProcessor.processDelta(mockResourceChangedMessage);

        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));

        assertEquals(1, argument.getAllValues().size());
        Links links = argument.getValue().getData().getLinks();
        assertNotNull(links);
        String charges = links.getCharges();
        assertEquals(String.format(TestData.ALL_COMPANY_CHARGES_LINK, MOCK_COMPANY_NUMBER), charges);
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("process CompanyProfile Updates where charges inside links are not updated successfully")
    void processCompanyProfileUpdates_ChargesLinksNotUpdated() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithValidResourceUri();

        CompanyProfile companyProfileWithLinks = testData.createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiGetResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfileWithLinks);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenAnswer(invocation -> companyProfileApiGetResponse);

        chargesStreamProcessor.processDelta(mockResourceChangedMessage);

        verify(companyProfileService, never()).
                patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("update CompanyProfile With Charges")
    void updateCompanyProfileWithCharges() {

        CompanyProfile companyProfile = testData.createCompanyProfile();

        final ApiResponse<Void> updatedCompanyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, null);

        ArgumentCaptor<CompanyProfile> argument = ArgumentCaptor.forClass(CompanyProfile.class);

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                (argument.capture())))
                .thenAnswer(invocation -> updatedCompanyProfileApiResponse);

        chargesStreamProcessor.addCompanyChargesLink(CONTEXT_ID, MOCK_COMPANY_NUMBER,
                companyProfile.getData());

        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
        assertEquals(1, argument.getAllValues().size());

        Data data = argument.getValue().getData();
        assertTrue(data.getHasCharges());
        Links links = data.getLinks();
        assertNotNull(links);
        assertEquals(String.format(TestData.ALL_COMPANY_CHARGES_LINK, MOCK_COMPANY_NUMBER), links.getCharges());
        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    static Stream<Arguments> testExtractCompanyNumberFromResourceUri() {
        return Stream.of(
                Arguments.of("/company/OC330796/charges/--GWGxXZanPgGtcE5dTZcrLlk3k", "OC330796"),
                Arguments.of("/companyabc/OC330796/charges/--GWGxXZanPgGtcE5dTZcrLlk3k", null),
                Arguments.of("/company/12345678/aabccharges/--GWGxXZanPgGtcE5dTZcrLlk3k", null),
                Arguments.of("/companyabc/12345678/aabccharges/--GWGxXZanPgGtcE5dTZcrLlk3k", null),
                Arguments.of("/company//OC330796//charges/--GWGxXZanPgGtcE5dTZcrLlk3k", "/OC330796/")
        );
    }

    @ParameterizedTest(name = "{index} ==> {2}: is {0} valid? {1}")
    @MethodSource("testExtractCompanyNumberFromResourceUri")
    void urlPatternTest(String input, String expected) {
        Optional<String> companyNumberOptional =
                Optional.ofNullable(chargesStreamProcessor.extractCompanyNumber(input));
        companyNumberOptional.ifPresent(companyNumber -> assertEquals(expected, companyNumber));
        if (expected == null) {
            assertFalse(companyNumberOptional.isPresent());
        } else {
            assertEquals(expected, companyNumberOptional.get());
        }
    }

    @Test
    @DisplayName("No Company Number Extracted")
    void noCompanyNumberExtracted() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData
                .createResourceChangedMessageWithInValidResourceUri();
        assertThrows(NonRetryableErrorException.class, () -> chargesStreamProcessor.processDelta(mockResourceChangedMessage));

        verify(companyProfileService, never()).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);

        verify(companyProfileService, never()).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
    }

    @Test
    @DisplayName("throws RetryableErrorException when CompanyProfile service is unavailable")
    void throwRetryableErrorExceptionWhenCompanyProfileServiceIsUnavailable() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithValidResourceUri();

        CompanyProfile companyProfileWithLinks = testData.createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.SERVICE_UNAVAILABLE.value(), null, companyProfileWithLinks);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        assertThrows(RetryableErrorException.class, () -> chargesStreamProcessor.processDelta(mockResourceChangedMessage));

        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(companyProfileService, never()).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));

        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws NonRetryableErrorException when patch CompanyProfile returns Bad Request")
    void throwNonRetryableErrorExceptionWhenPatchCompanyProfileReturnsBadRequest() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithValidResourceUri();

        CompanyProfile companyProfileWithLinks = testData.createCompanyProfile();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfileWithLinks);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);
        when(companyProfileService.patchCompanyProfile(any(), any(), any()))
                .thenReturn(new ApiResponse<>(
                        HttpStatus.BAD_REQUEST.value(), null, null));
        when(chargesService.getACharge(any(), any()))
                .thenReturn(new ApiResponse<>(
                        HttpStatus.OK.value(), null, null));
        assertThrows(NonRetryableErrorException.class, () -> chargesStreamProcessor.processDelta(mockResourceChangedMessage));

        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));

        verifyNoMoreInteractions(companyProfileService);
        verifyLoggingDataMap();
    }

    @Test
    @DisplayName("throws RetryableErrorException when Charges Data API returns non successfull response !2XX")
    void throwRetryableErrorExceptionWhenChargeDataAPIReturnsNon2XX() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithValidResourceUri();

        CompanyProfile companyProfileWithLinks = testData.createCompanyProfile();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfileWithLinks);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);
        when(chargesService.getACharge(any(), any()))
                .thenReturn(new ApiResponse<>(
                        HttpStatus.NOT_FOUND.value(), null, null));
        assertThrows(RetryableErrorException.class, () -> chargesStreamProcessor.processDelta(mockResourceChangedMessage));
        verify(companyProfileService).getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER);
        verifyLoggingDataMap();
    }

    private void verifyLoggingDataMap() {
        Map<String, Object> dataMap = DataMapHolder.getLogMap();
        assertEquals(CONTEXT_ID, dataMap.get("request_id"));
        assertEquals(MOCK_COMPANY_NUMBER, dataMap.get("company_number"));
    }
}