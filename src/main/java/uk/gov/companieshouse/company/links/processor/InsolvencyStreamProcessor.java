package uk.gov.companieshouse.company.links.processor;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.insolvency.CompanyInsolvency;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.company.links.service.InsolvencyService;
import uk.gov.companieshouse.company.links.type.ApiType;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class InsolvencyStreamProcessor extends StreamResponseProcessor {

    private final CompanyProfileService companyProfileService;
    private final InsolvencyService insolvencyService;

    /**
     * Construct an insolvency stream processor.
     */
    @Autowired
    public InsolvencyStreamProcessor(
            CompanyProfileService companyProfileService,
            Logger logger, InsolvencyService insolvencyService) {
        super(logger);
        this.companyProfileService = companyProfileService;
        this.insolvencyService = insolvencyService;
    }

    /**
     * Process a ResourceChangedData deleted message.
     */
    public void processDelete(Message<ResourceChangedData> resourceChangedMessage) {
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();

        // the resource_id field returned represents the insolvency record's company number
        final String companyNumber = payload.getResourceId();
        DataMapHolder.get()
                .companyNumber(companyNumber);
        final ApiResponse<CompanyProfile> response = getCompanyProfile(payload, logContext,
                companyNumber);

        var data = response.getData().getData();
        var links = data.getLinks();

        if (links == null || links.getInsolvency() == null) {
            logger.trace(String.format("Company profile with company number %s,"
                    + " does not contain insolvency links, will not perform patch"
                    + " for contextId %s", companyNumber, logContext), DataMapHolder.getLogMap());
            return;
        }

        final ApiResponse<CompanyInsolvency> insolvencyResponse = insolvencyService
                .getInsolvency(logContext, companyNumber);

        if (insolvencyResponse.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
            links.setInsolvency(null);
            data.hasInsolvencyHistory(false);
            data.setLinks(links);

            patchCompanyProfile(data, logContext, companyNumber);
        } else {
            String message = "Response from insolvency-data-api service, main delta is not "
                    + "yet deleted, throwing retry-able exception to check again";
            logger.errorContext(logContext, message, null, DataMapHolder.getLogMap());
            throw new RetryableErrorException(message);
        }
    }

    /**
     * Process a ResourceChangedData message.
     */
    public void processDelta(Message<ResourceChangedData> resourceChangedMessage) {
        final var payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();

        // the resource_id field returned represents the insolvency record's company number
        final String companyNumber = payload.getResourceId();
        DataMapHolder.get().companyNumber(companyNumber);
        final ApiResponse<CompanyProfile> response =
                getCompanyProfile(payload, logContext, companyNumber);

        var data = response.getData().getData();
        var links = data.getLinks();

        if (links != null && !StringUtils.isBlank(links.getInsolvency())) {
            logger.trace(String.format("Company profile with company number %s,"
                    + " contains insolvency links, will not perform PATCH"
                    + " for contextId %s", companyNumber, logContext), DataMapHolder.getLogMap());
            return;
        }

        if (links == null) {
            links = new Links();
        }

        final ApiResponse<CompanyInsolvency> companyInsolvencyResponse =
                insolvencyService.getInsolvency(logContext, companyNumber);

        HttpStatus statusCode = HttpStatus.valueOf(companyInsolvencyResponse.getStatusCode());

        if (statusCode.is2xxSuccessful()) {
            links.setInsolvency(String.format("/company/%s/insolvency", companyNumber));
            data.setLinks(links);
            data.setHasInsolvencyHistory(true);

            patchCompanyProfile(data, logContext, companyNumber);
        } else {
            String message = "Response from companyInsolvencyService, main delta update not"
                    + " yet completed, Re-Trying";
            logger.errorContext(logContext, message, null, DataMapHolder.getLogMap());
            throw new RetryableErrorException(message);
        }
    }

    private void patchCompanyProfile(Data data, String logContext, String companyNumber) {
        var companyProfile = new CompanyProfile();
        companyProfile.setData(data);

        final ApiResponse<Void> patchResponse = companyProfileService.patchCompanyProfile(
                logContext, companyNumber, companyProfile);
        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "PATCH", ApiType.COMPANY_PROFILE, companyNumber);
    }

    private ApiResponse<CompanyProfile> getCompanyProfile(ResourceChangedData payload,
            String logContext,
            String companyNumber) {
        if (StringUtils.isEmpty(companyNumber)) {
            throw new NonRetryableErrorException(String.format(
                    "Company number is empty or null in message with contextId %s", logContext));
        }

        logger.trace(String.format("Resource changed message with contextId %s of kind %s "
                        + "for company number %s retrieved",
                logContext, payload.getResourceKind(), companyNumber), DataMapHolder.getLogMap());

        final ApiResponse<CompanyProfile> response =
                companyProfileService.getCompanyProfile(logContext, companyNumber);
        handleCompanyProfileResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                "GET", ApiType.COMPANY_PROFILE, companyNumber);
        return response;
    }
}
