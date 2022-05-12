package uk.gov.companieshouse.company.links.processor;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class InsolvencyStreamProcessor extends StreamResponseProcessor {

    private final CompanyProfileService companyProfileService;

    /**
     * Construct an insolvency stream processor.
     */
    @Autowired
    public InsolvencyStreamProcessor(
                                     CompanyProfileService companyProfileService,
                                     Logger logger) {
        super(logger);
        this.companyProfileService = companyProfileService;
    }

    /**
     * Process a ResourceChangedData deleted message.
     */
    public void processDelete(Message<ResourceChangedData> resourceChangedMessage) {
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();
        final Map<String, Object> logMap = new HashMap<>();

        // the resource_id field returned represents the insolvency record's company number
        final String companyNumber = payload.getResourceId();
        if (StringUtils.isEmpty(companyNumber)) {
            logger.error("Company number is empty or null");
            throw new NonRetryableErrorException("Company number is empty or null");
        }
        logger.trace(String.format("Resource changed message for deleted event of kind %s "
                + "for company number %s retrieved", payload.getResourceKind(), companyNumber));

        final ApiResponse<CompanyProfile> response =
                companyProfileService.getCompanyProfile(logContext, companyNumber);
        logger.trace(String.format("Retrieved company profile for company number %s: %s",
                companyNumber, response.getData()));
        handleResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                "Response from GET call to company profile api", logMap);
        var data = response.getData().getData();
        var links = data.getLinks();

        if (links != null && links.getInsolvency() == null) {
            logger.trace(String.format("Company profile with company number %s,"
                            + " does not contain insolvency links, will not perform patch",
                    companyNumber));
            return;
        }

        links.setInsolvency(null);
        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(data);

        final ApiResponse<Void> patchResponse =
                companyProfileService.patchCompanyProfile(
                        logContext, companyNumber, companyProfile
                );

        logger.trace(String.format("Performing a PATCH with new company profile %s",
                companyProfile));
        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "Response from PATCH call to company profile api", logMap);
    }

    /**
     * Process a ResourceChangedData message.
     */
    public void processDelta(Message<ResourceChangedData> resourceChangedMessage) {
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();
        final Map<String, Object> logMap = new HashMap<>();

        // the resource_id field returned represents the insolvency record's company number
        final String companyNumber = payload.getResourceId();
        if (StringUtils.isEmpty(companyNumber)) {
            logger.error("Company number is empty or null");
            throw new NonRetryableErrorException("Company number is empty or null");
        }
        logger.trace(String.format("Resource changed message of kind %s "
                + "for company number %s retrieved", payload.getResourceKind(), companyNumber));

        final ApiResponse<CompanyProfile> response =
                companyProfileService.getCompanyProfile(logContext, companyNumber);
        logger.trace(String.format("Retrieved company profile for company number %s: %s",
                companyNumber, response.getData()));
        handleResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                "Response from GET call to company profile api", logMap);
        var data = response.getData().getData();
        var links = data.getLinks();

        if (links != null && links.getInsolvency() != null) {
            logger.trace(String.format("Company profile with company number %s,"
                    + " already contains insolvency links, will not perform patch",
                    companyNumber));
            return;
        }

        logger.trace(String.format("Current company profile with company number %s,"
                + " does not contain an insolvency link, attaching an insolvency link",
                companyNumber));

        if (links == null) {
            links = new Links();
        }

        links.setInsolvency(String.format("/company/%s/insolvency", companyNumber));
        data.setLinks(links);
        data.setHasInsolvencyHistory(true);
        var companyProfile = new CompanyProfile();
        companyProfile.setData(data);

        final ApiResponse<Void> patchResponse =
                companyProfileService.patchCompanyProfile(
                        logContext, companyNumber, companyProfile
                );

        logger.trace(String.format("Performing a PATCH with new company profile %s",
                companyProfile));
        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "Response from PATCH call to company profile api", logMap);

    }



}
