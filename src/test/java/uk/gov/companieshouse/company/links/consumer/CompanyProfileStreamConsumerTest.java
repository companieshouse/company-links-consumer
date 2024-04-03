package uk.gov.companieshouse.company.links.consumer;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.processor.CompanyProfileStreamProcessor;
import uk.gov.companieshouse.company.links.processor.TestData;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import static uk.gov.companieshouse.company.links.processor.TestData.CONTEXT_ID;
import static uk.gov.companieshouse.company.links.processor.TestData.MOCK_COMPANY_NUMBER;
import static uk.gov.companieshouse.company.links.processor.TestData.COMPANY_PROFILE_LINK;


@ExtendWith(MockitoExtension.class)
public class CompanyProfileStreamConsumerTest {

    private CompanyProfileStreamConsumer companyProfileStreamConsumer;
    private CompanyProfileStreamProcessor companyProfileStreamProcessor;
    @Mock
    private CompanyProfileService companyProfileService;
    @Mock
    private Logger logger;
    private TestData testData;

    @BeforeEach
    void setUp() {
        companyProfileStreamProcessor =  spy(new CompanyProfileStreamProcessor(
                companyProfileService,
                logger));
        testData = new TestData();
        DataMapHolder.initialise(CONTEXT_ID);
        companyProfileStreamConsumer = new CompanyProfileStreamConsumer(companyProfileStreamProcessor, logger);
    }

    @Test
    @DisplayName("Successfully receive company profile resource changed message with no errors")
    void successfullyReceiveCompanyProfileMessage() throws IOException {
        Message<ResourceChangedData> mockResourceChangedMessage = testData.createCompanyProfileMessageWithValidResourceUri();

        CompanyProfile companyProfile = testData.createCompanyProfile();

        final ApiResponse<CompanyProfile> companyProfileApiResponse = new ApiResponse<>(
                HttpStatus.OK.value(), null, companyProfile);

        companyProfileStreamConsumer.receive(mockResourceChangedMessage, "topic", "partition", "offset");

        verifyNoInteractions(companyProfileService);
    }
}
