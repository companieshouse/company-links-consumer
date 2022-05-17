package uk.gov.companieshouse.company.links.processor;

import static uk.gov.companieshouse.company.links.processor.ResponseHandler.handleResponse;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.service.ChargesService;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class ChargesStreamProcessor {

    public static final String EXTRACT_COMPANY_NUMBER_PATTERN = "(?<=company/)(.*?)(?=/charges)";
    private final Logger logger;
    private final CompanyProfileService companyProfileService;
    private final ChargesService chargesService;

    /**
     * Construct an Charges stream processor.
     */
    @Autowired
    public ChargesStreamProcessor(CompanyProfileService companyProfileService,
                                  ChargesService chargesService,
                                  Logger logger) {
        this.companyProfileService = companyProfileService;
        this.chargesService = chargesService;
        this.logger = logger;
    }

    /**
     * Process a ResourceChangedData message for delete.
     */
    public void processDelete(Message<ResourceChangedData> resourceChangedMessage)
        throws JsonProcessingException {
        MessageHeaders headers = resourceChangedMessage.getHeaders();
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();
        final Map<String, Object> logMap = new HashMap<>();

        // the resource_id field returned represents the charges record's company number
        final String companyNumber = extractCompanyNumber(payload.getResourceUri());
        if (StringUtils.isEmpty(companyNumber)) {
            logger.error("Company number is empty or null");
            throw new NonRetryableErrorException("Company number is empty or null");
        }

        logger.trace(String.format("Resource changed message for delete event of kind %s "
                + "for company number %s retrieved", payload.getResourceKind(), companyNumber));



        final ApiResponse<CompanyProfile> response =
                getCompanyProfileApi(logContext, logMap, companyNumber);

        var data = response.getData().getData();
        var links = data.getLinks();

        // Do we have links and is there a charges link? If the link does not exist
        // then there is nothing to delete.
        if (links == null || links.getCharges() == null) {
            logger.trace(String.format("Company profile with company number %s,"
                    + " does not contain charges links, will not perform delete",
                    companyNumber));
            return;
        }

        // invoke charges-data-api GET all charges endpoint to fetch remaining
        // number of charges for a given company number
        ApiResponse<ChargesApi> charges = chargesService.getCharges(
                logContext, companyNumber);

        // if remaining number of charges is 0 in above GET call,
        // remove the link from the company profile object and invoke existing
        // PATCH (company/<number>/links) endpoint on company_profile_api to update the entity
        if (charges.getData().getTotalCount() == 0) {
            links.setCharges(null);
            CompanyProfile companyProfile = new CompanyProfile();
            companyProfile.setData(data);

            final ApiResponse<Void> patchResponse = companyProfileService
                    .patchCompanyProfile(logContext, companyNumber, companyProfile);

            logger.trace(String.format(
                    "Performing a PATCH on company profile %s to remove charges link",
                    companyProfile));

            handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                    "Response from PATCH call to company profile api", logMap, logger);
        } else {
            //if remaining number of charges is 1 or greater than 1 in above GET call,
            // do nothing, end of processing. i.e. No PATCH invocation
            logger.trace(String.format(
                    "Nothing to PATCH on company number %s, charges link not removed",
                    companyNumber));
        }
    }

    /**
     * Process a ResourceChangedData message.
     */
    public void process(Message<ResourceChangedData> resourceChangedMessage)
            throws JsonProcessingException {
        MessageHeaders headers = resourceChangedMessage.getHeaders();

        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();
        final Map<String, Object> logMap = new HashMap<>();

        // the resource_id field returned represents the charges record's company number
        final String companyNumber = extractCompanyNumber(payload.getResourceUri());
        logger.trace(String.format("Resource changed message of kind %s "
                + "for company number %s retrieved", payload.getResourceKind(), companyNumber));
        if (!StringUtils.isEmpty(companyNumber)) {
            final ApiResponse<CompanyProfile> response =
                    getCompanyProfileApi(logContext, logMap, companyNumber);

            ApiResponse<Void> patchResponse = processCompanyProfileUpdates(logContext,
                    companyNumber, response,
                    payload, headers);
            if (patchResponse != null) {
                handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                        "Response from PATCH call to company profile api", logMap, logger);
            }
        }
    }

    ApiResponse<Void> processCompanyProfileUpdates(String logContext,
                                      String companyNumber,
                                      ApiResponse<CompanyProfile> response,
                                      ResourceChangedData payload,
                                      MessageHeaders headers)
            throws JsonProcessingException {
        var data = response.getData().getData();
        var links = data.getLinks();

        if (doesCompanyProfileHaveCharges(companyNumber, data)) {
            return null;
        }

        return updateCompanyProfileWithCharges(logContext, companyNumber, data,
                payload, headers);

    }

    ApiResponse<Void> updateCompanyProfileWithCharges(String logContext,
                                         String companyNumber, Data data,
                                         ResourceChangedData payload,
                                         MessageHeaders headers)
            throws JsonProcessingException {
        logger.trace(String.format("Current company profile with company number %s,"
                        + " does not contain charges link, attaching charges link",
                companyNumber));

        Links links = data.getLinks() == null ? new Links() : data.getLinks();

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
        return patchResponse;
    }

    boolean doesCompanyProfileHaveCharges(String companyNumber, Data data) {

        Links links = data.getLinks();
        if (links != null && links.getCharges() != null) {
            logger.trace(String.format("Company profile with company number %s,"
                            + " already contains charges links, will not perform patch",
                    companyNumber));
            return true;
        }
        return false;
    }

    ApiResponse<CompanyProfile> getCompanyProfileApi(String logContext,
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

    String extractCompanyNumber(String resourceUri) {

        if (StringUtils.isNotBlank(resourceUri)) {
            //matches all characters between company/ and /
            Pattern companyNo = Pattern.compile(EXTRACT_COMPANY_NUMBER_PATTERN);
            Matcher matcher = companyNo.matcher(resourceUri);
            if (matcher.find()) {
                return matcher.group(0).length() > 1 ? matcher.group(0) : null;
            }
        }
        logger.trace(String.format("Could not extract company number from uri "
                + "%s ", resourceUri));
        return null;
    }
}
