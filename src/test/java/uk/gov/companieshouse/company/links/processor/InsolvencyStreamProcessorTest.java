package uk.gov.companieshouse.company.links.processor;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.producer.InsolvencyStreamProducer;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.delta.ChsDelta;
import uk.gov.companieshouse.logging.Logger;

@ExtendWith(MockitoExtension.class)
class InsolvencyStreamProcessorTest {
    private static final String MOCK_COMPANY_NUMBER = "02588581";
    private InsolvencyStreamProcessor insolvencyProcessor;

    @Mock
    private InsolvencyStreamProducer insolvencyProducer;

    @Mock
    private CompanyProfileService companyProfileService;

    @Mock
    private Logger logger;

    @BeforeEach
    void setUp() {
        insolvencyProcessor = new InsolvencyStreamProcessor(
                insolvencyProducer,
                companyProfileService,
                logger);
    }

    @Test
    @DisplayName("Successfully processes a kafka message containing a ChsDelta payload")
    void successfullyProcessInsolvencyDelta() throws IOException {
        Message<ChsDelta> mockChsDeltaMessage = createChsDeltaMessage();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, createCompanyProfile());

        when(companyProfileService.getCompanyProfile("context_id", MOCK_COMPANY_NUMBER))
                .thenReturn(companyProfileApiResponse);

        insolvencyProcessor.process(mockChsDeltaMessage);

        verify(companyProfileService).getCompanyProfile("context_id", MOCK_COMPANY_NUMBER);
        verify(logger, times(2)).trace(anyString());
        verify(logger, atLeastOnce()).trace(contains(
                "InsolvencyDelta extracted from Kafka message"));
        verify(logger, atLeastOnce()).trace((
                String.format("Retrieved company profile for company number %s: %s",
                        MOCK_COMPANY_NUMBER, companyProfileApiResponse.getData())));
    }

    private Message<ChsDelta> createChsDeltaMessage() throws IOException {
        InputStreamReader exampleInsolvencyJsonPayload = new InputStreamReader(
                Objects.requireNonNull(
                        ClassLoader.getSystemClassLoader().getResourceAsStream("insolvency-delta" +
                                ".json")));
        String insolvencyData = FileCopyUtils.copyToString(exampleInsolvencyJsonPayload);

        ChsDelta mockChsDelta = ChsDelta.newBuilder()
                .setData(insolvencyData)
                .setContextId("context_id")
                .setAttempt(1)
                .build();

        return MessageBuilder
                .withPayload(mockChsDelta)
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

}