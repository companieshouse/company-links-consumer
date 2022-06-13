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
    public InsolvencyStreamProcessor(CompanyProfileService companyProfileService,
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
            throw new NonRetryableErrorException(String.format(
                    "Company number is empty or null in message with contextId %s", logContext));
        }

        logger.trace(String.format("Resource changed message with contextId %s for deleted event "
                        + "of kind %s for company number %s retrieved",
                logContext, payload.getResourceKind(), companyNumber));

        final ApiResponse<CompanyProfile> response =
                companyProfileService.getCompanyProfile(logContext, companyNumber);
        handleResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                "GET company-profile-api", companyNumber, logMap);

        var data = response.getData().getData();
        var links = data.getLinks();

        if (links != null && links.getInsolvency() == null) {
            logger.trace(String.format("Company profile with company number %s,"
                    + " does not contain insolvency links, will not perform patch"
                    + " for contextId %s", companyNumber, logContext));
            return;
        }

        links.setInsolvency(null);
        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(data);

        logger.trace(String.format("Performing a PATCH with "
                + "company number %s for contextId %s", companyNumber, logContext));
        final ApiResponse<Void> patchResponse =
                companyProfileService.patchCompanyProfile(
                        logContext, companyNumber, companyProfile
                );
        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "PATCH company-profile-api", companyNumber, logMap);
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
            throw new NonRetryableErrorException(String.format(
                    "Company number is empty or null in message with contextId %s", logContext));
        }

        logger.trace(String.format("Resource changed message with contextId %s of kind %s "
                        + "for company number %s retrieved",
                logContext, payload.getResourceKind(), companyNumber));

        final ApiResponse<CompanyProfile> response =
                companyProfileService.getCompanyProfile(logContext, companyNumber);
        handleResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                "GET company-profile-api", companyNumber, logMap);

        var data = response.getData().getData();
        var links = data.getLinks();

        if (links != null && links.getInsolvency() != null) {
            logger.trace(String.format("Company profile with company number %s,"
                    + " contains insolvency links, will not perform PATCH"
                    + " for contextId %s", companyNumber, logContext));
            return;
        }

        logger.trace(String.format("Performing a PATCH with "
                + "company number %s for contextId %s", companyNumber, logContext));

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
        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "PATCH company-profile-api", companyNumber, logMap);
    }



}
