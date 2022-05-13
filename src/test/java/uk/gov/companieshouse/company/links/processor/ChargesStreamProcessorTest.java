package uk.gov.companieshouse.company.links.processor;

import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
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

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.companieshouse.company.links.consumer.TestData.CONTEXT_ID;
import static uk.gov.companieshouse.company.links.processor.TestData.COMPANY_CHARGES_LINK;
import static uk.gov.companieshouse.company.links.processor.TestData.MOCK_COMPANY_NUMBER;

@ExtendWith(MockitoExtension.class)
class ChargesStreamProcessorTest {

    private ChargesStreamProcessor chargesStreamProcessor;

    @Mock
    private CompanyProfileService companyProfileService;

    @Mock
    private Logger logger;

    private TestData testData;

    @BeforeEach
    void setUp() {
        chargesStreamProcessor = new ChargesStreamProcessor(
                companyProfileService,
                logger);
        testData = new TestData();
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData with deleted event payload, charges links removed")
    void successfullyProcessResourceChangedDataWithDeletedEventPayloadChargesLinksRemoved() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessageWithDeletedChargesEvent();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
            HttpStatus.OK.value(), null, createCompanyProfileWithoutChargesLinks());

        when(companyProfileService.getCompanyProfile("context_id", MOCK_COMPANY_NUMBER))
            .thenReturn(companyProfileApiResponse);

        chargesStreamProcessor.processDelete(mockResourceChangedMessage);

        verify(companyProfileService, atLeastOnce()).getCompanyProfile("context_id", MOCK_COMPANY_NUMBER);
        verify(companyProfileService, never()).patchCompanyProfile(eq("context_id"), eq(MOCK_COMPANY_NUMBER), any(CompanyProfile.class));
    }

    @SuppressWarnings("rawtypes")
    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData with deleted event payload, charges links removed")
    void successfullyProcessResourceChangedDataWithDeletedEventPayloadNoChargesLinksToRemoved() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessageWithDeletedChargesEvent();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
            HttpStatus.OK.value(), null, createCompanyProfileWithChargesLinks());

        final ApiResponse<CompanyProfile> patchApiResponse = new ApiResponse<>(
            HttpStatus.OK.value(), null, null);

        when(companyProfileService.getCompanyProfile("context_id", MOCK_COMPANY_NUMBER))
            .thenReturn(companyProfileApiResponse);

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), any(CompanyProfile.class))).thenAnswer(
            (Answer<ApiResponse>) invocationOnMock -> patchApiResponse);

        chargesStreamProcessor.processDelete(mockResourceChangedMessage);

        verify(companyProfileService, atLeastOnce()).getCompanyProfile("context_id", MOCK_COMPANY_NUMBER);
        verify(companyProfileService).patchCompanyProfile(eq("context_id"), eq(MOCK_COMPANY_NUMBER), any(CompanyProfile.class));
    }


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
                (Answer<ApiResponse<CompanyProfile>>) invocationOnMock -> updatedCompanyProfileApiResponse);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        chargesStreamProcessor.process(mockResourceChangedMessage);

        verify(companyProfileService, times(1)).getCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER));
        // dont need to know that this method is called with a specific value just that it was called
        verify(companyProfileService, times(1)).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
        // here we should check that the profile has been correctly updated for the call
        assertEquals(1, argument.getAllValues().size()); // should have only captured 1 argument
        assertNotNull(argument.getValue().getData().getLinks()); // we have links
        String links = argument.getValue().getData().getLinks().getCharges();
        assertEquals(String.format(COMPANY_CHARGES_LINK, MOCK_COMPANY_NUMBER), links); // and the charges link is as expected
        verifyNoMoreInteractions(companyProfileService);
    }

    @Test
    @DisplayName("Checks if company profile has links and should return false if there are no charges inside links")
    void doesCompanyProfileHaveCharges_should_return_false() throws IOException {
        CompanyProfile companyProfile = testData.createCompanyProfile();

        assertFalse(chargesStreamProcessor.doesCompanyProfileHaveCharges(MOCK_COMPANY_NUMBER,
            companyProfile.getData()));

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

        chargesStreamProcessor.process(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER));
        verify(companyProfileService, times(0)).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));

        verifyNoMoreInteractions(companyProfileService);
    }

    @Test
    @DisplayName("Checks if company profile has links and should return true if there are charges inside links")
    void doesCompanyProfileHaveCharges_should_return_true() throws IOException {

        CompanyProfile companyProfileWithLinks = testData.createCompanyProfileWithChargesLinks();

        assertTrue(chargesStreamProcessor.doesCompanyProfileHaveCharges(MOCK_COMPANY_NUMBER,
                companyProfileWithLinks.getData()));

    }

    @Test
    @DisplayName("process CompanyProfile Updates where charges inside links are updated successfully")
    void processCompanyProfileUpdates_SuccessfullyChargesLinksGetsUpdated() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithValidResourceUri();

        CompanyProfile companyProfile = testData.createCompanyProfile();
        CompanyProfile companyProfileWithLinks = testData.createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        final ApiResponse<Void> updatedCompanyProfileApiResponse = new ApiResponse<Void>(
                HttpStatus.OK.value(), null, null);
        ArgumentCaptor<CompanyProfile> argument = ArgumentCaptor.forClass(CompanyProfile.class);

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                        (argument.capture())))
                .thenAnswer((Answer) invocation -> updatedCompanyProfileApiResponse);

        chargesStreamProcessor.processCompanyProfileUpdates(
                CONTEXT_ID, MOCK_COMPANY_NUMBER, companyProfileApiResponse,
                mockResourceChangedMessage.getPayload(), mockResourceChangedMessage.getHeaders());

        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));

        assertEquals(1, argument.getAllValues().size());
        Links links = argument.getValue().getData().getLinks();
        assertNotNull(links);
        String charges = links.getCharges();
        assertEquals(String.format(COMPANY_CHARGES_LINK, MOCK_COMPANY_NUMBER), charges);
        verifyNoMoreInteractions(companyProfileService);
    }

    @Test
    @DisplayName("process CompanyProfile Updates where charges inside links are not updated successfully")
    void processCompanyProfileUpdates_ChargesLinksNotUpdated() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithValidResourceUri();

        CompanyProfile companyProfileWithLinks = testData.createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfileWithLinks);

        chargesStreamProcessor.processCompanyProfileUpdates(
                CONTEXT_ID, MOCK_COMPANY_NUMBER, companyProfileApiResponse,
                mockResourceChangedMessage.getPayload(), mockResourceChangedMessage.getHeaders());

        verify(companyProfileService, times(0)).
                patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
        verifyNoMoreInteractions(companyProfileService);
    }

    @Test
    @DisplayName("update CompanyProfile With Charges")
    void updateCompanyProfileWithCharges() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithValidResourceUri();

        CompanyProfile companyProfile = testData.createCompanyProfile();
        CompanyProfile companyProfileWithLinks = testData.createCompanyProfileWithChargesLinks();

        final ApiResponse<Void> updatedCompanyProfileApiResponse = new ApiResponse<Void>(
                HttpStatus.OK.value(), null, null);

        ArgumentCaptor<CompanyProfile> argument = ArgumentCaptor.forClass(CompanyProfile.class);

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                (argument.capture())))
                .thenAnswer((Answer) invocation -> updatedCompanyProfileApiResponse);

        chargesStreamProcessor.updateCompanyProfileWithCharges(CONTEXT_ID, MOCK_COMPANY_NUMBER,
                companyProfile.getData(), mockResourceChangedMessage.getPayload(),
                mockResourceChangedMessage.getHeaders());

        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
        assertEquals(1, argument.getAllValues().size());
        Links links = argument.getValue().getData().getLinks();
        assertNotNull(links);
        assertEquals(String.format(COMPANY_CHARGES_LINK, MOCK_COMPANY_NUMBER), links.getCharges());
        verifyNoMoreInteractions(companyProfileService);
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
    public void urlPatternTest(String input, String expected) {
        Optional<String> companyNumberOptional = chargesStreamProcessor.extractCompanyNumber(input);
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
        assertThrows(NonRetryableErrorException.class, () -> chargesStreamProcessor.process(mockResourceChangedMessage));

        verify(companyProfileService, times(0)).getCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER));

        verify(companyProfileService, times(0)).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
    }

    private Message<ResourceChangedData> createResourceChangedMessageWithDeletedChargesEvent() throws IOException {
        InputStreamReader exampleInsolvencyJsonPayload = new InputStreamReader(
            Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                .getResourceAsStream("charges-record.json")));
        String insolvencyRecord = FileCopyUtils.copyToString(exampleInsolvencyJsonPayload);
        EventRecord deletedEventRecord = new EventRecord();
        deletedEventRecord.setType("deleted");

        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
            .setContextId("context_id")
            .setResourceId(MOCK_COMPANY_NUMBER)
            .setResourceKind("company-charges")
            .setResourceUri(String.format("/company/%s/charges", MOCK_COMPANY_NUMBER))
            .setEvent(deletedEventRecord)
            .setData(insolvencyRecord)
            .build();

        return MessageBuilder
            .withPayload(resourceChangedData)
            .setHeader(KafkaHeaders.RECEIVED_TOPIC, "test")
            .build();
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

        assertThrows(RetryableErrorException.class, () -> chargesStreamProcessor.process(mockResourceChangedMessage));

        verify(companyProfileService).getCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER));
        verify(companyProfileService, times(0)).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                Mockito.any(CompanyProfile.class));

        verifyNoMoreInteractions(companyProfileService);
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

        assertThrows(NonRetryableErrorException.class, () -> chargesStreamProcessor.process(mockResourceChangedMessage));

        verify(companyProfileService).getCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER));
        verify(companyProfileService, times(1)).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                Mockito.any(CompanyProfile.class));

        verifyNoMoreInteractions(companyProfileService);
    }


    private CompanyProfile createCompanyProfileWithoutChargesLinks() {
        Data companyProfileData = new Data();
        companyProfileData.setCompanyNumber(MOCK_COMPANY_NUMBER);
        Links links = new Links();
        companyProfileData.setLinks(links);

        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(companyProfileData);
        return companyProfile;
    }

    private CompanyProfile createCompanyProfileWithChargesLinks() {
        Data companyProfileData = new Data();
        companyProfileData.setCompanyNumber(MOCK_COMPANY_NUMBER);
        Links links = new Links();
        links.setCharges("/company/"+MOCK_COMPANY_NUMBER+"/charges");
        companyProfileData.setLinks(links);

        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(companyProfileData);
        return companyProfile;
    }

}