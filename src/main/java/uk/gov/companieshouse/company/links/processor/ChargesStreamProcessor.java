package uk.gov.companieshouse.company.links.processor;

import static org.apache.commons.lang3.StringUtils.substringAfterLast;

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
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.service.ChargesService;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.company.links.type.ApiType;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class ChargesStreamProcessor extends StreamResponseProcessor {

    public static final String EXTRACT_COMPANY_NUMBER_PATTERN = "(?<=company/)(.*)(?=/charges)";
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
        final String companyNumber = extractCompanyNumber(resourceUri);
        DataMapHolder.get()
                .companyNumber(companyNumber);

        final ApiResponse<CompanyProfile> response =
                getCompanyProfile(payload, companyNumber);
        var data = response.getData().getData();
        var links = data.getLinks();

        if (links == null || links.getCharges() == null) {
            logger.trace(String.format("Company profile with company number %s,"
                            + " does not contain charges links, will not perform DELETE",
                            companyNumber), DataMapHolder.getLogMap());
            return;
        }

        ApiResponse<ChargesApi> chargesResponse = chargesService.getCharges(
                logContext, companyNumber);

        handleResponse(HttpStatus.valueOf(chargesResponse.getStatusCode()), logContext,
                "GET", ApiType.CHARGES, companyNumber);

        ChargesApi chargesData = chargesResponse.getData();

        if (chargesData.getTotalCount() == 0) {
            removeCompanyChargesLink(logContext, companyNumber, data, links);
        } else {
            String incomingChargeId = substringAfterLast(resourceUri, "/");
            if (chargesData.getItems().stream().anyMatch(x ->
                    incomingChargeId.equals(x.getId()))) {
                throw new RetryableErrorException(String.format("Charge with id: %s is still not "
                        + "deleted", incomingChargeId));
            }

            logger.trace(String.format("Nothing to PATCH with company number %s, charges link not"
                    + " removed", companyNumber), DataMapHolder.getLogMap());
        }
    }

    /**
     * Process a ResourceChangedData message.
     */
    public void processDelta(Message<ResourceChangedData> resourceChangedMessage) {
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();
        final String resourceUri = payload.getResourceUri();
        final String companyNumber = extractCompanyNumber(resourceUri);
        final ApiResponse<CompanyProfile> response =
                getCompanyProfile(payload, companyNumber);
        var data = response.getData().getData();
        DataMapHolder.get()
                .companyNumber(companyNumber);
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
            addCompanyChargesLink(logContext, companyNumber, data);
        }
    }

    private ApiResponse<CompanyProfile> getCompanyProfile(ResourceChangedData payload,
            String companyNumber) {
        final String logContext = payload.getContextId();
        if (StringUtils.isEmpty(companyNumber)) {
            throw new NonRetryableErrorException("Company number is empty or null in message");
        }

        logger.trace(String.format("Resource changed message of kind %s "
                        + "for company number %s retrieved",
                payload.getResourceKind(), companyNumber), DataMapHolder.getLogMap());

        final ApiResponse<CompanyProfile> response =
                companyProfileService.getCompanyProfile(logContext, companyNumber);
        handleCompanyProfileResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                "GET", ApiType.COMPANY_PROFILE, companyNumber);
        return response;
    }

    void addCompanyChargesLink(String logContext, String companyNumber, Data data) {
        logger.trace(String.format("Message with contextId %s and company number %s -"
                        + "company profile does not contain charges link, attaching charges link",
                logContext, companyNumber), DataMapHolder.getLogMap());

        Links links = data.getLinks() == null ? new Links() : data.getLinks();

        links.setCharges(String.format("/company/%s/charges", companyNumber));
        data.setLinks(links);
        data.setHasCharges(true);
        var companyProfile = new CompanyProfile();
        companyProfile.setData(data);

        final ApiResponse<Void> patchResponse = companyProfileService.patchCompanyProfile(
                logContext, companyNumber, companyProfile);
        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "PATCH", ApiType.COMPANY_PROFILE, companyNumber);
    }

    void removeCompanyChargesLink(String logContext, String companyNumber, Data data, Links links) {
        links.setCharges(null);
        data.setHasCharges(false);
        patchCompanyProfile(logContext, companyNumber, data);
    }

    private void patchCompanyProfile(String logContext, String companyNumber, Data data) {
        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(data);

        final ApiResponse<Void> patchResponse = companyProfileService.patchCompanyProfile(
                logContext, companyNumber, companyProfile);

        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "PATCH", ApiType.COMPANY_PROFILE, companyNumber);
    }

    boolean doesCompanyProfileHaveCharges(String logContext, String companyNumber, Links links) {
        boolean hasCharges = (links != null && links.getCharges() != null);
        if (hasCharges) {
            logger.trace(String.format("Message with contextId %s and company number %s -"
                            + "company profile contains charges links, will not perform patch",
                    logContext, companyNumber), DataMapHolder.getLogMap());
        }
        return hasCharges;
    }

    String extractCompanyNumber(String resourceUri) {
        if (StringUtils.isNotBlank(resourceUri)) {
            //matches all characters between company/ and /
            Pattern companyNo = Pattern.compile(EXTRACT_COMPANY_NUMBER_PATTERN);
            Matcher matcher = companyNo.matcher(resourceUri);
            if (matcher.find()) {
                String companyNumber = matcher.group();
                DataMapHolder.get()
                        .companyNumber(companyNumber);

                return companyNumber;
            }
        }
        logger.trace(String.format("Could not extract company number from uri "
                + "%s ", resourceUri), DataMapHolder.getLogMap());
        return null;
    }
}
