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
import uk.gov.companieshouse.company.links.producer.InsolvencyStreamProducer;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class InsolvencyStreamProcessor {

    private final Logger logger;
    private final InsolvencyStreamProducer insolvencyStreamProducer;
    private final CompanyProfileService companyProfileService;

    /**
     * Construct an insolvency stream processor.
     */
    @Autowired
    public InsolvencyStreamProcessor(InsolvencyStreamProducer insolvencyStreamProducer,
                                     CompanyProfileService companyProfileService,
                                     Logger logger) {
        this.insolvencyStreamProducer = insolvencyStreamProducer;
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
            final boolean isRetry = headers.containsKey("INSOLVENCY_DELTA_RETRY_COUNT");
            final ResourceChangedData payload = resourceChangedMessage.getPayload();
            final String logContext = payload.getContextId();
            final Map<String, Object> logMap = new HashMap<>();

            // the resource_id field returned represents the insolvency record's company number
            final String companyNumber = payload.getResourceId();
            logger.trace(String.format("Resource changed message of kind %s "
                    + "for company number %s retrieved", payload.getResourceKind(), companyNumber));

            final ApiResponse<CompanyProfile> response =
                    companyProfileService.getCompanyProfile(logContext, companyNumber);
            logger.trace(String.format("Retrieved company profile for company number %s: %s",
                    companyNumber, response.getData()));
            handleResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                    "Response from GET call to company profile api", logMap, logger);

            Links responseLinks = response.getData().getData().getLinks();
            if (responseLinks.getInsolvency() != null) {
                logger.trace(String.format("Company profile with company number %s,"
                        + " already contains insolvency links, will not perform patch"));
                return;
            }

            logger.trace(String.format("Current company profile with company number %s,"
                    + " does not contain an insolvency link, attaching an insolvency link",
                    companyNumber));

            Data data = response.getData().getData();
            Links links = data.getLinks();
            links.setInsolvency(String.format("/company/%s/insolvency", companyNumber));
            data.setLinks(links);
            CompanyProfile companyProfile = new CompanyProfile();
            companyProfile.setData(data);

            logger.trace(String.format("Performing a PATCH with new company profile %s",
                    companyProfile));

            // TODO DSND-603: PATCH company profile
        } catch (RetryErrorException ex) {
            retry(resourceChangedMessage);
        } catch (Exception ex) {
            handleErrors(resourceChangedMessage);
            // send to error topic
        }
    }

    public void retry(Message<ResourceChangedData> resourceChangedMessage) {

    }

    private void handleErrors(Message<ResourceChangedData> resourceChangedMessage) {

    }
}
