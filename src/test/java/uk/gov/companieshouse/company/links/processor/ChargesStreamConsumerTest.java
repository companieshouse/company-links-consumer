package uk.gov.companieshouse.company.links.processor;

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
import org.mockito.stubbing.Answer;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;

import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.consumer.ChargesStreamConsumer;
import uk.gov.companieshouse.company.links.service.ChargesService;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Originally this testedthe Charges Stream Processor which was fine whe it had only one method.
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
        chargesStreamConsumer = new ChargesStreamConsumer(chargesStreamProcessor);
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData delete payload, profile without charges links")
    void successfullyProcessResourceChangedDataChargesDelete() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithDelete();

        CompanyProfile companyProfile = testData.createCompanyProfile();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
            HttpStatus.OK.value(), null, companyProfile);
        final ApiResponse<CompanyProfile> updatedCompanyProfileApiResponse = new ApiResponse<>(
            HttpStatus.OK.value(), null, null);

        when(companyProfileService.getCompanyProfile(TestData.CONTEXT_ID, TestData.MOCK_COMPANY_NUMBER))
            .thenReturn(companyProfileApiResponse);

        chargesStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(chargesStreamProcessor).processDelete(eq(mockResourceChangedMessage));
        verify(companyProfileService, times(1)).getCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER));
        verify(companyProfileService, never()).patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER),
            any(CompanyProfile.class));
        verifyNoMoreInteractions(companyProfileService);
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
        when(companyProfileService.patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER), (argument.capture()))).thenAnswer(
            (Answer) invocationOnMock -> updatedCompanyProfileApiResponse);

        when(companyProfileService.getCompanyProfile(TestData.CONTEXT_ID, TestData.MOCK_COMPANY_NUMBER))
            .thenReturn(companyProfileApiResponse);

        when(chargesService.getCharges(TestData.CONTEXT_ID, TestData.MOCK_COMPANY_NUMBER)).thenReturn(chargesApiResponseNoCharges);

        chargesStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(chargesStreamProcessor).processDelete(eq(mockResourceChangedMessage));
        verify(companyProfileService, times(1)).getCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER));
        // dont need to know that this method is called with a specific value just that it was called
        verify(companyProfileService, times(1)).patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER),
            any(CompanyProfile.class));
        // here we should check that the profile has been correctly updated for the call
        assertEquals(1, argument.getAllValues().size()); // should have only captured 1 argument
        assertNotNull(argument.getValue().getData().getLinks()); // we have links
        assertNull(argument.getValue().getData().getLinks().getCharges()); // and the charges link is as expected
        verifyNoMoreInteractions(companyProfileService);
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData delete payload, profile with charges links and current charges")
    void successfullyProcessResourceChangedDataChargesDeleteWithLinksAndCharges() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithDelete();

        CompanyProfile companyProfile = testData.createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
            HttpStatus.OK.value(), null, companyProfile);
        final ApiResponse<CompanyProfile> updatedCompanyProfileApiResponse = new ApiResponse<>(
            HttpStatus.OK.value(), null, null);

        final ChargesApi hasCharges = new ChargesApi();
        hasCharges.setTotalCount(1);
        final ApiResponse<ChargesApi> chargesApiResponseNoCharges = new ApiResponse<>(
            HttpStatus.OK.value(), null, hasCharges);

        when(companyProfileService.getCompanyProfile(TestData.CONTEXT_ID, TestData.MOCK_COMPANY_NUMBER))
            .thenReturn(companyProfileApiResponse);

        when(chargesService.getCharges(TestData.CONTEXT_ID, TestData.MOCK_COMPANY_NUMBER)).thenReturn(chargesApiResponseNoCharges);

        chargesStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(chargesStreamProcessor).processDelete(eq(mockResourceChangedMessage));
        verify(companyProfileService, times(1)).getCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER));
        // dont need to know that this method is called with a specific value just that it was called
        verify(companyProfileService, never()).patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER),
            any(CompanyProfile.class));
        verifyNoMoreInteractions(companyProfileService);
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
        when(companyProfileService.patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER), (argument.capture()))).thenAnswer(
                (Answer) invocationOnMock -> updatedCompanyProfileApiResponse);

        when(companyProfileService.getCompanyProfile(TestData.CONTEXT_ID, TestData.MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        chargesStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(chargesStreamProcessor).process(eq(mockResourceChangedMessage));
        verify(companyProfileService, times(1)).getCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER));
        // dont need to know that this method is called with a specific value just that it was called
        verify(companyProfileService, times(1)).patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
        // here we should check that the profile has been correctly updated for the call
        assertEquals(1, argument.getAllValues().size()); // should have only captured 1 argument
        assertNotNull(argument.getValue().getData().getLinks()); // we have links
        String links = argument.getValue().getData().getLinks().getCharges();
        assertEquals(TestData.COMPANY_CHARGES_LINK, links); // and the charges link is as expected
        verifyNoMoreInteractions(companyProfileService);
    }

    @Test
    @DisplayName("Checks if company profile has links and should return false if there are no charges inside links")
    void doesCompanyProfileHaveCharges_should_return_false() throws IOException {
        CompanyProfile companyProfile = testData.createCompanyProfile();

        assertFalse(chargesStreamProcessor.doesCompanyProfileHaveCharges(TestData.MOCK_COMPANY_NUMBER,
            companyProfile.getData()));

    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ResourceChangedData payload, links doesn't need updating")
    void successfullyProcessResourceChangedDataChargesDoesntGetUpdated() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createResourceChangedMessageWithValidResourceUri();

        CompanyProfile companyProfileWithLinks = testData.createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfileWithLinks);

        when(companyProfileService.getCompanyProfile(TestData.CONTEXT_ID, TestData.MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        chargesStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verify(chargesStreamProcessor).process(eq(mockResourceChangedMessage));
        verify(companyProfileService).getCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER));
        verify(companyProfileService, times(0)).patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));

        verifyNoMoreInteractions(companyProfileService);
    }

    @Test
    @DisplayName("Checks if company profile has links and should return true if there are charges inside links")
    void doesCompanyProfileHaveCharges_should_return_true() throws IOException {

        CompanyProfile companyProfileWithLinks = testData.createCompanyProfileWithChargesLinks();

        assertTrue(chargesStreamProcessor.doesCompanyProfileHaveCharges(TestData.MOCK_COMPANY_NUMBER,
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

        when(companyProfileService.patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER),
                        (argument.capture())))
                .thenAnswer((Answer) invocation -> updatedCompanyProfileApiResponse);

        chargesStreamProcessor.processCompanyProfileUpdates(
                TestData.CONTEXT_ID, TestData.MOCK_COMPANY_NUMBER, companyProfileApiResponse,
                mockResourceChangedMessage.getPayload(), mockResourceChangedMessage.getHeaders());

        verify(companyProfileService).patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));

        assertEquals(1, argument.getAllValues().size());
        Links links = argument.getValue().getData().getLinks();
        assertNotNull(links);
        String charges = links.getCharges();
        assertEquals(String.format(TestData.COMPANY_CHARGES_LINK, TestData.MOCK_COMPANY_NUMBER), charges);
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
                TestData.CONTEXT_ID, TestData.MOCK_COMPANY_NUMBER, companyProfileApiResponse,
                mockResourceChangedMessage.getPayload(), mockResourceChangedMessage.getHeaders());

        verify(companyProfileService, times(0)).
                patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER),
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

        when(companyProfileService.patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER),
                (argument.capture())))
                .thenAnswer((Answer) invocation -> updatedCompanyProfileApiResponse);

        chargesStreamProcessor.updateCompanyProfileWithCharges(TestData.CONTEXT_ID, TestData.MOCK_COMPANY_NUMBER,
                companyProfile.getData(), mockResourceChangedMessage.getPayload(),
                mockResourceChangedMessage.getHeaders());

        verify(companyProfileService).patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
        assertEquals(1, argument.getAllValues().size());
        Links links = argument.getValue().getData().getLinks();
        assertNotNull(links);
        assertEquals(String.format(TestData.COMPANY_CHARGES_LINK, TestData.MOCK_COMPANY_NUMBER), links.getCharges());
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
        String companyNumber = chargesStreamProcessor.extractCompanyNumber(input);
        assertEquals(expected, companyNumber);
    }

    @Test
    @DisplayName("No Company Number Extracted")
    void noCompanyNumberExtracted() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = testData
                .createResourceChangedMessageWithInValidResourceUri();

        chargesStreamProcessor.process(mockResourceChangedMessage);

        verify(companyProfileService, times(0))
            .getCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER));

        verify(companyProfileService, times(0))
            .patchCompanyProfile(eq(TestData.CONTEXT_ID), eq(TestData.MOCK_COMPANY_NUMBER),
                any(CompanyProfile.class));
    }
}