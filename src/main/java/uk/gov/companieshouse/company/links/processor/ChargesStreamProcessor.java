package uk.gov.companieshouse.company.links.processor;

import static uk.gov.companieshouse.company.links.processor.ResponseHandler.handleResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryErrorException;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class ChargesStreamProcessor {

    private final Logger logger;
    private final CompanyProfileService companyProfileService;

    /**
     * Construct an Charges stream processor.
     */
    @Autowired
    public ChargesStreamProcessor(CompanyProfileService companyProfileService,
                                  Logger logger) {
        this.companyProfileService = companyProfileService;
        this.logger = logger;
    }

    /**
     * Process a ResourceChangedData message.
     */
    public void process(Message<ResourceChangedData> resourceChangedMessage) {
        try {
            MessageHeaders headers = resourceChangedMessage.getHeaders();
            final String receivedTopic =
                    Objects.requireNonNull(headers.get(KafkaHeaders.RECEIVED_TOPIC)).toString();
            //TODO need to check where we set this property.
            //TODO We need to create a new one for this processor
            final boolean isRetry = headers.containsKey("CHARGES_DELTA_RETRY_COUNT");
            final ResourceChangedData payload = resourceChangedMessage.getPayload();
            final String logContext = payload.getContextId();
            final Map<String, Object> logMap = new HashMap<>();

            // the resource_id field returned represents the charges record's company number
            final String companyNumber = payload.getResourceId();
            logger.trace(String.format("Resource changed message of kind %s "
                    + "for company number %s retrieved", payload.getResourceKind(), companyNumber));

            final ApiResponse<CompanyProfile> response =
                    getCompanyProfileApi(logContext, logMap, companyNumber);

            processCompanyProfileUpdates(logContext, logMap, companyNumber, response);

        } catch (RetryErrorException ex) {
            logger.error(String.format("Exception occurred due to %s", ex));
            retry(resourceChangedMessage);
        } catch (Exception ex) {
            logger.error(String.format("Unexpected Exception occurred due to %s", ex));
            handleErrors(resourceChangedMessage);
            // send to error topic
        }
    }

    private void processCompanyProfileUpdates(String logContext, Map<String, Object> logMap,
                                              String companyNumber,
                                              ApiResponse<CompanyProfile> response) {
        var data = response.getData().getData();
        var links = data.getLinks();

        if (doesCompanyProfileHaveCharges(companyNumber, links)) {
            return;
        }

        updateCompanyProfileWithCharges(logContext, logMap, companyNumber, data, links);

    }

    private void updateCompanyProfileWithCharges(String logContext, Map<String, Object> logMap,
                                                 String companyNumber, Data data, Links links) {
        logger.trace(String.format("Current company profile with company number %s,"
                        + " does not contain charges link, attaching charges link",
                companyNumber));

        if (links == null) {
            links = new Links();
        }

        links.setCharges(String.format("/company/%s/charges", companyNumber));
        data.setLinks(links);
        var companyProfile = new CompanyProfile();
        companyProfile.setData(data);

        final ApiResponse<Void> patchResponse =
                companyProfileService.patchCompanyProfile(
                        logContext, companyNumber, companyProfile
                );

        logger.trace(String.format("Performing a PATCH with new company profile %s",
                companyProfile));
        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "Response from PATCH call to company profile api", logMap, logger);
    }

    private boolean doesCompanyProfileHaveCharges(String companyNumber, Links links) {
        if (links != null && links.getCharges() != null) {
            logger.trace(String.format("Company profile with company number %s,"
                            + " already contains charges links, will not perform patch",
                    companyNumber));
            return true;
        }
        return false;
    }

    private ApiResponse<CompanyProfile> getCompanyProfileApi(String logContext,
                                                             Map<String, Object> logMap,
                                                             String companyNumber) {
        final ApiResponse<CompanyProfile> response =
                companyProfileService.getCompanyProfile(logContext, companyNumber);
        logger.trace(String.format("Retrieved company profile for company number %s: %s",
                companyNumber, response.getData()));
        handleResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                "Response from GET call to company profile api", logMap, logger);
        return response;
    }

    public void retry(Message<ResourceChangedData> resourceChangedMessage) {
        // Retry functionality added in a future ticket
    }

    private void handleErrors(Message<ResourceChangedData> resourceChangedMessage) {
        // Error functionality added in a future ticket
    }

}
