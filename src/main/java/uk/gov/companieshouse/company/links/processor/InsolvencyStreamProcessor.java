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
import uk.gov.companieshouse.api.insolvency.CompanyInsolvency;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.service.CompanyInsolvencyService;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.company.links.type.ApiType;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class InsolvencyStreamProcessor extends StreamResponseProcessor {

    private final CompanyProfileService companyProfileService;
    private final CompanyInsolvencyService companyInsolvencyService;

    /**
     * Construct an insolvency stream processor.
     */
    @Autowired
    public InsolvencyStreamProcessor(
            CompanyProfileService companyProfileService,
            Logger logger, CompanyInsolvencyService companyInsolvencyService) {
        super(logger);
        this.companyProfileService = companyProfileService;
        this.companyInsolvencyService = companyInsolvencyService;
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
        handleCompanyProfileResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                "GET", ApiType.COMPANY_PROFILE, companyNumber, logMap);

        var data = response.getData().getData();
        var links = data.getLinks();

        if (links == null || links.getInsolvency() == null) {
            logger.trace(String.format("Company profile with company number %s,"
                    + " does not contain insolvency links, will not perform patch"
                    + " for contextId %s", companyNumber, logContext));
            return;
        }

        CompanyProfile companyProfile = new CompanyProfile();

        final ApiResponse<CompanyInsolvency> companyInsolvencyResponse = companyInsolvencyService
                .getCompanyInsolvency(logContext, companyNumber);

        if (companyInsolvencyResponse.getStatusCode() == HttpStatus.GONE.value()) {
            links.setInsolvency(null);
            data.hasInsolvencyHistory(false);
            data.setLinks(links);
            companyProfile.setData(data);
        } else {
            String message = "Response from insolvency-data-api service, main delta is not "
                    + "yet deleted, throwing retry-able exception to check again";
            logger.errorContext(logContext, message, null, logMap);
            throw new RetryableErrorException(message);
        }

        final ApiResponse<Void> patchResponse = companyProfileService.patchCompanyProfile(
                        logContext, companyNumber, companyProfile);
        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "PATCH", ApiType.COMPANY_PROFILE, companyNumber, logMap);
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
        handleCompanyProfileResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                "GET", ApiType.COMPANY_PROFILE, companyNumber, logMap);

        var data = response.getData().getData();
        var links = data.getLinks();

        if (links != null && !StringUtils.isBlank(links.getInsolvency())) {
            logger.trace(String.format("Company profile with company number %s,"
                    + " contains insolvency links, will not perform PATCH"
                    + " for contextId %s", companyNumber, logContext));
            return;
        }

        if (links == null) {
            links = new Links();
        }

        final ApiResponse<CompanyInsolvency> companyInsolvencyResponse =
                companyInsolvencyService.getCompanyInsolvency(logContext, companyNumber);

        HttpStatus statusCode = HttpStatus.valueOf(companyInsolvencyResponse.getStatusCode());

        if (statusCode.is2xxSuccessful()) {
            links.setInsolvency(String.format("/company/%s/insolvency", companyNumber));
        } else {
            String message = "Response from companyInsolvencyService, main delta update not"
                    + " yet completed, Re-Trying";
            logger.errorContext(logContext, message, null, logMap);
            throw new RetryableErrorException(message);
        }

        data.setLinks(links);
        data.setHasInsolvencyHistory(true);

        var companyProfile = new CompanyProfile();
        companyProfile.setData(data);

        final ApiResponse<Void> patchResponse = companyProfileService.patchCompanyProfile(
                        logContext, companyNumber, companyProfile);
        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "PATCH", ApiType.COMPANY_PROFILE, companyNumber, logMap);
    }



}
