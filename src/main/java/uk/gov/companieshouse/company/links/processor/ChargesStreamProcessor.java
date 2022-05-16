package uk.gov.companieshouse.company.links.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class ChargesStreamProcessor extends StreamResponseProcessor {

    public static final String EXTRACT_COMPANY_NUMBER_PATTERN = "(?<=company/)(.*?)(?=/charges)";
    private final CompanyProfileService companyProfileService;

    /**
     * Construct an Charges stream processor.
     */
    @Autowired
    public ChargesStreamProcessor(CompanyProfileService companyProfileService,
                                  Logger logger) {
        super(logger);
        this.companyProfileService = companyProfileService;
    }

    /**
     * Process a ResourceChangedData message for delete.
     */
    public void processDelete(Message<ResourceChangedData> resourceChangedMessage) {
        MessageHeaders headers = resourceChangedMessage.getHeaders();
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();
        final Map<String, Object> logMap = new HashMap<>();

        // the resource_id field returned represents the charges record's company number
        final Optional<String> companyNumberOptional =
                extractCompanyNumber(payload.getResourceUri());
        companyNumberOptional.orElseThrow(
                () -> new NonRetryableErrorException("Unable to extract company number due to "
                        + "invalid resource uri in the message")
        );
        companyNumberOptional.ifPresent(companyNumber -> {
            logger.trace(String.format("Resource changed message for delete event of kind %s "
                    + "for company number %s retrieved", payload.getResourceKind(), companyNumber));

            final ApiResponse<CompanyProfile> response =
                    getCompanyProfileApi(logContext, logMap, companyNumber);

            var data = response.getData().getData();
            var links = data.getLinks();

            if (links != null && links.getCharges() == null) {
                logger.trace(String.format("Company profile with company number %s,"
                                + " does not contain chagres links, will not perform patch",
                        companyNumber));
                return;
            }

            links.setCharges(null);
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
        });
    }

    /**
     * Process a ResourceChangedData message.
     */
    public void process(Message<ResourceChangedData> resourceChangedMessage) {
        MessageHeaders headers = resourceChangedMessage.getHeaders();

        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();
        final Map<String, Object> logMap = new HashMap<>();

        // the resource_id field returned represents the charges record's company number
        final Optional<String> companyNumberOptional =
                extractCompanyNumber(payload.getResourceUri());
        companyNumberOptional.orElseThrow(
                () -> new NonRetryableErrorException("Unable to extract company number due to "
                        + "invalid resource uri in the message")
        );

        companyNumberOptional.ifPresent(companyNumber -> {
            logger.trace(String.format("Resource changed message of kind %s "
                    + "for company number %s retrieved", payload.getResourceKind(), companyNumber));

            final ApiResponse<CompanyProfile> response =
                    getCompanyProfileApi(logContext, logMap, companyNumber);

            ApiResponse<Void> patchResponse = processCompanyProfileUpdates(logContext,
                    companyNumber, response,
                    payload, headers);
            if (patchResponse != null) {
                handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                        "Response from PATCH call to company profile api", logMap);
            }
        });
    }

    ApiResponse<Void> processCompanyProfileUpdates(String logContext,
                                                   String companyNumber,
                                                   ApiResponse<CompanyProfile> response,
                                                   ResourceChangedData payload,
                                                   MessageHeaders headers) {
        var data = response.getData().getData();

        if (doesCompanyProfileHaveCharges(companyNumber, data)) {
            //do nothing
            return null;
        }

        return updateCompanyProfileWithCharges(logContext, companyNumber, data,
                payload, headers);

    }

    ApiResponse<Void> updateCompanyProfileWithCharges(String logContext,
                                                      String companyNumber, Data data,
                                                      ResourceChangedData payload,
                                                      MessageHeaders headers) {
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
                "Response from GET call to company profile api", logMap);
        return response;
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

