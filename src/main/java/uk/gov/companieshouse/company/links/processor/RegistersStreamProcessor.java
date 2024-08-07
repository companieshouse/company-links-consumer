package uk.gov.companieshouse.company.links.processor;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.model.registers.CompanyRegistersApi;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.company.links.service.RegistersService;
import uk.gov.companieshouse.company.links.type.ApiType;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class RegistersStreamProcessor extends StreamResponseProcessor {

    private final CompanyProfileService companyProfileService;
    private final RegistersService registersService;

    /**
     * Construct a registers stream processor.
     */
    @Autowired
    public RegistersStreamProcessor(
            CompanyProfileService companyProfileService,
            Logger logger, RegistersService registersService) {
        super(logger);
        this.companyProfileService = companyProfileService;
        this.registersService = registersService;
    }

    /**
     * Process a ResourceChangedData deleted message.
     */
    public void processDelete(Message<ResourceChangedData> resourceChangedMessage) {
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();

        // the resource_id field returned represents the registers record's company number
        final String companyNumber = payload.getResourceId();
        DataMapHolder.get()
                .companyNumber(companyNumber);
        final ApiResponse<CompanyProfile> response = getCompanyProfile(payload, logContext,
                companyNumber);

        var data = response.getData().getData();
        var links = data.getLinks();

        if (links == null || links.getRegisters() == null) {
            logger.trace(String.format("Company profile with company number %s,"
                    + " does not contain registers links, will not perform patch"
                    + " for contextId %s", companyNumber, logContext), DataMapHolder.getLogMap());
            return;
        }

        final ApiResponse<CompanyRegistersApi> registersResponse = registersService
                .getRegisters(logContext, companyNumber);

        if (registersResponse.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
            links.setRegisters(null);
            data.setLinks(links);

            patchCompanyProfile(data, logContext, companyNumber);
        } else {
            String message = "Response from registers-data-api service, main delta is not "
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

        // the resource_id field returned represents the registers record's company number
        final String companyNumber = payload.getResourceId();
        DataMapHolder.get().companyNumber(companyNumber);
        final ApiResponse<CompanyProfile> response =
                getCompanyProfile(payload, logContext, companyNumber);

        var data = response.getData().getData();
        var links = data.getLinks();

        if (links != null && !StringUtils.isBlank(links.getRegisters())) {
            logger.trace(String.format("Company profile with company number %s,"
                    + " contains registers links, will not perform PATCH"
                    + " for contextId %s", companyNumber, logContext), DataMapHolder.getLogMap());
            return;
        }

        if (links == null) {
            links = new Links();
        }

        final ApiResponse<CompanyRegistersApi> companyRegistersResponse =
                registersService.getRegisters(logContext, companyNumber);

        HttpStatus statusCode = HttpStatus.valueOf(companyRegistersResponse.getStatusCode());

        if (statusCode.is2xxSuccessful()) {
            links.setRegisters(String.format("/company/%s/registers", companyNumber));
            data.setLinks(links);

            patchCompanyProfile(data, logContext, companyNumber);
        } else {
            String message = "Response from companyRegistersService, main delta update not"
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
