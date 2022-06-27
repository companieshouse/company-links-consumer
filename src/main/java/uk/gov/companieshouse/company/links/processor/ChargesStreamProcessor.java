package uk.gov.companieshouse.company.links.processor;

import static org.apache.commons.lang3.StringUtils.substringAfterLast;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.charges.ChargeApi;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.service.ChargesService;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.company.links.type.ApiType;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class ChargesStreamProcessor extends StreamResponseProcessor {

    public static final String EXTRACT_COMPANY_NUMBER_PATTERN = "(?<=company/)(.*?)(?=/charges)";
    private final CompanyProfileService companyProfileService;
    private final ChargesService chargesService;

    /**
     * Construct an Charges stream processor.
     */
    @Autowired
    public ChargesStreamProcessor(CompanyProfileService companyProfileService,
                                  ChargesService chargesService,
                                  Logger logger) {
        super(logger);
        this.companyProfileService = companyProfileService;
        this.chargesService = chargesService;
    }

    /**
     * Process a ResourceChangedData message for delete.
     */
    public void processDelete(Message<ResourceChangedData> resourceChangedMessage) {
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();
        final String resourceUri = payload.getResourceUri();
        final Map<String, Object> logMap = new HashMap<>();

        final Optional<String> companyNumberOptional = extractCompanyNumber(resourceUri);
        var companyNumber = companyNumberOptional.orElseThrow(
                () -> new NonRetryableErrorException(String.format(
                        "Unable to extract company number due to "
                                + "invalid resource uri %s in message with contextId %s",
                        resourceUri, logContext)));

        logger.trace(String.format("Resource changed message for deleted event of kind %s "
                        + "for company number %s with contextId %s retrieved",
                payload.getResourceKind(), companyNumber, logContext));

        Data data = fetchCompanyProfile(logContext, companyNumber, logMap);
        var links = data.getLinks();

        if (links == null || links.getCharges() == null) {
            logger.trace(String.format("Company profile with company number %s,"
                    + " does not contain charges links, will not perform DELETE"
                    + " for contextId %s", companyNumber, logContext));
            return;
        }

        ApiResponse<ChargesApi> chargesResponse = chargesService.getCharges(
                logContext, companyNumber);

        handleResponse(HttpStatus.valueOf(chargesResponse.getStatusCode()), logContext,
                "GET", ApiType.CHARGES, companyNumber, logMap);

        ChargesApi chargesData = chargesResponse.getData();

        if (chargesData.getTotalCount() == 0) {
            removeCompanyChargesLink(logContext, logMap, companyNumber, data, links);
        } else {
            String incomingChargeId = substringAfterLast(resourceUri, "/");
            if (chargesData.getItems().stream().anyMatch(x ->
                    incomingChargeId.equals(x.getId()))) {
                throw new RetryableErrorException(String.format("Charge with id: %s is still not "
                        + "deleted", incomingChargeId));
            }

            logger.trace(String.format(
                    "Nothing to PATCH with company number %s for contextId %s,"
                            + " charges link not removed", companyNumber, logContext));
        }
    }

    /**
     * Process a ResourceChangedData message.
     */
    public void processDelta(Message<ResourceChangedData> resourceChangedMessage) {
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();
        final String resourceUri = payload.getResourceUri();
        final Map<String, Object> logMap = new HashMap<>();

        final Optional<String> companyNumberOptional = extractCompanyNumber(resourceUri);
        var companyNumber = companyNumberOptional.orElseThrow(
                () -> new NonRetryableErrorException(String.format(
                        "Unable to extract company number due to "
                                + "invalid resource uri %s in message with contextId %s",
                        resourceUri, logContext)));

        logger.trace(String.format("Resource changed message for event of kind %s "
                        + "for company number %s with contextId %s retrieved",
                payload.getResourceKind(), companyNumber, logContext));

        Data data = fetchCompanyProfile(logContext, companyNumber, logMap);

        // if no charges then update company profile
        if (!doesCompanyProfileHaveCharges(logContext, companyNumber, data.getLinks())) {

            ApiResponse<ChargeApi> chargeApiResponse =
                    chargesService.getACharge(logContext, resourceUri);
            HttpStatus httpStatus =
                    HttpStatus.resolve(chargeApiResponse.getStatusCode());
            if (httpStatus == null || !httpStatus.is2xxSuccessful()) {
                throw new RetryableErrorException(String.format(
                        "Resource not found in company charges for the "
                                + "resource uri %s with contextId %s",
                        resourceUri, logContext));
            }
            addCompanyChargesLink(logContext, logMap, companyNumber, data);
        }
    }

    private Data fetchCompanyProfile(String logContext, String companyNumber,
                                     Map<String, Object> logMap) {
        final ApiResponse<CompanyProfile> response =
                companyProfileService.getCompanyProfile(logContext, companyNumber);
        handleResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                "GET", ApiType.COMPANY_PROFILE, companyNumber, logMap);

        return response.getData().getData();
    }

    void addCompanyChargesLink(String logContext, Map<String, Object> logMap,
                                       String companyNumber, Data data) {
        logger.trace(String.format("Message with contextId %s and company number %s -"
                        + "company profile does not contain charges link, attaching charges link",
                logContext, companyNumber));

        Links links = data.getLinks() == null ? new Links() : data.getLinks();

        links.setCharges(String.format("/company/%s/charges", companyNumber));
        data.setLinks(links);
        data.setHasCharges(true);
        var companyProfile = new CompanyProfile();
        companyProfile.setData(data);

        final ApiResponse<Void> patchResponse = companyProfileService.patchCompanyProfile(
                logContext, companyNumber, companyProfile);
        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "PATCH", ApiType.COMPANY_PROFILE, companyNumber, logMap);
    }

    void removeCompanyChargesLink(String logContext, Map<String, Object> logMap,
                                          String companyNumber, Data data, Links links) {
        links.setCharges(null);
        data.setHasCharges(false);
        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(data);

        logger.trace(String.format("Performing a PATCH with "
                + "company number %s for contextId %s", companyNumber, logContext));
        final ApiResponse<Void> patchResponse = companyProfileService.patchCompanyProfile(
                logContext, companyNumber, companyProfile);

        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "PATCH", ApiType.COMPANY_PROFILE, companyNumber, logMap);
    }

    boolean doesCompanyProfileHaveCharges(String logContext, String companyNumber, Links links) {
        boolean hasCharges = (links != null && links.getCharges() != null);
        if (hasCharges) {
            logger.trace(String.format("Message with contextId %s and company number %s -"
                            + "company profile contains charges links, will not perform patch",
                    logContext, companyNumber));
        }
        return hasCharges;
    }

    Optional<String> extractCompanyNumber(String resourceUri) {
        if (StringUtils.isNotBlank(resourceUri)) {
            //matches all characters between company/ and /
            Pattern companyNo = Pattern.compile(EXTRACT_COMPANY_NUMBER_PATTERN);
            Matcher matcher = companyNo.matcher(resourceUri);
            if (matcher.find()) {
                return Optional.ofNullable(matcher.group());
            }
        }
        logger.trace(String.format("Could not extract company number from uri "
                + "%s ", resourceUri));
        return Optional.empty();
    }
}
