package uk.gov.companieshouse.company.links.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChargesStreamProcessorTest {
    private static final String MOCK_COMPANY_NUMBER = "03105860";
    private static final String CONTEXT_ID = "context_id";
    public static final String RESOURCE_KIND = "company-charges";
    public static final String TOPIC = "test";
    public static final String PARTITION = "partition_1";
    public static final String OFFSET = "offset_1";
    public static final String COMPANY_CHARGES_LINK = "/company/%s/charges";
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

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);
        final ApiResponse<CompanyProfile> updatedCompanyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, null);

        ArgumentCaptor<CompanyProfile> argument = ArgumentCaptor.forClass(CompanyProfile.class);
        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER), (argument.capture()))).thenAnswer(
                (Answer) invocationOnMock -> updatedCompanyProfileApiResponse);

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        chargesStreamProcessor.process(mockResourceChangedMessage);

        verify(companyProfileService, times(1)).getCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER));
        // dont need to know that this method is called with a specific value just that it was called
        verify(companyProfileService, times(1)).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                Mockito.any(CompanyProfile.class));
        // here we should check that the profile has been correctly updated for the call
        assertEquals(1, argument.getAllValues().size()); // should have only captured 1 argument
        assertNotNull(argument.getValue().getData().getLinks()); // we have links
        String links = argument.getValue().getData().getLinks().getCharges();
        assertTrue(String.format(COMPANY_CHARGES_LINK, MOCK_COMPANY_NUMBER).equals(links)); // and the charges link is as expected
        verifyNoMoreInteractions(companyProfileService);
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

        when(companyProfileService.getCompanyProfile(CONTEXT_ID, MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        chargesStreamProcessor.process(mockResourceChangedMessage);

        verify(companyProfileService).getCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER));
        verify(companyProfileService, times(0)).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                Mockito.any(CompanyProfile.class));

        verifyNoMoreInteractions(companyProfileService);
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
        ArgumentCaptor<CompanyProfile> argument = ArgumentCaptor.forClass(CompanyProfile.class);

        when(companyProfileService.patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                        (argument.capture())))
                .thenAnswer((Answer) invocation -> updatedCompanyProfileApiResponse);

        chargesStreamProcessor.processCompanyProfileUpdates(
                CONTEXT_ID, MOCK_COMPANY_NUMBER, companyProfileApiResponse,
                mockResourceChangedMessage.getPayload(), mockResourceChangedMessage.getHeaders());

        verify(companyProfileService).patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                Mockito.any(CompanyProfile.class));

        assertEquals(1, argument.getAllValues().size());
        Links links = argument.getValue().getData().getLinks();
        assertNotNull(links);
        String charges = links.getCharges();
        assertTrue(String.format(COMPANY_CHARGES_LINK, MOCK_COMPANY_NUMBER).equals(charges));
        verifyNoMoreInteractions(companyProfileService);
    }

    @Test
    @DisplayName("process CompanyProfile Updates where charges inside links are not updated successfully")
    void processCompanyProfileUpdates_ChargesLinksNotUpdated() throws IOException {

        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        CompanyProfile companyProfileWithLinks = createCompanyProfileWithChargesLinks();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfileWithLinks);

        chargesStreamProcessor.processCompanyProfileUpdates(
                CONTEXT_ID, MOCK_COMPANY_NUMBER, companyProfileApiResponse,
                mockResourceChangedMessage.getPayload(), mockResourceChangedMessage.getHeaders());

        verify(companyProfileService, times(0)).
                patchCompanyProfile(eq(CONTEXT_ID), eq(MOCK_COMPANY_NUMBER),
                Mockito.any(CompanyProfile.class));
        verifyNoMoreInteractions(companyProfileService);
    }

    @Test
    @DisplayName("update CompanyProfile With Charges")
    void updateCompanyProfileWithCharges() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = createResourceChangedMessage();

        CompanyProfile companyProfile = createCompanyProfile();
        CompanyProfile companyProfileWithLinks = createCompanyProfileWithChargesLinks();

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
                Mockito.any(CompanyProfile.class));
        assertEquals(1, argument.getAllValues().size());
        Links links = argument.getValue().getData().getLinks();
        assertNotNull(links);
        assertTrue(String.format(COMPANY_CHARGES_LINK, MOCK_COMPANY_NUMBER).equals(links.getCharges()));
        verifyNoMoreInteractions(companyProfileService);
    }

    private Message<ResourceChangedData> createResourceChangedMessage() throws IOException {
        InputStreamReader exampleChargesJsonPayload = new InputStreamReader(
                Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                        .getResourceAsStream("charges-record.json")));
        String chargesRecord = FileCopyUtils.copyToString(exampleChargesJsonPayload);

        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
                .setContextId(CONTEXT_ID)
                .setResourceId(MOCK_COMPANY_NUMBER)
                .setResourceKind(RESOURCE_KIND)
                .setResourceUri(String.format(COMPANY_CHARGES_LINK, MOCK_COMPANY_NUMBER))
                .setEvent(new EventRecord())
                .setData(chargesRecord)
                .build();

        return MessageBuilder
                .withPayload(resourceChangedData)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, TOPIC)
                .setHeader(KafkaHeaders.RECEIVED_PARTITION_ID, PARTITION)
                .setHeader(KafkaHeaders.OFFSET, OFFSET)
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
        links.setCharges(String.format(COMPANY_CHARGES_LINK, MOCK_COMPANY_NUMBER));
        companyProfile.getData().setLinks(links);
    }

}