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
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.company.links.service.PscService;
import uk.gov.companieshouse.company.links.type.ApiType;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class CompanyProfileStreamProcessor extends StreamResponseProcessor {

    private final CompanyProfileService companyProfileService;
    private final PscService pscService;

    /**
     * Construct a Company Profile stream processor.
     */
    @Autowired
    public CompanyProfileStreamProcessor(CompanyProfileService companyProfileService,
                                         PscService pscService, Logger logger) {
        super(logger);
        this.companyProfileService = companyProfileService;
        this.pscService = pscService;
    }

    /**
     * Process a ResourceChangedData message.
     */
    public void processDelta(Message<ResourceChangedData> resourceChangedMessage) {
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String contextId = payload.getContextId();
        final String resourceUri = payload.getResourceUri();
        final String companyNumber = payload.getResourceId();
        final ApiResponse<CompanyProfile> response =
                getCompanyProfile(payload, companyNumber);
        var data = response.getData().getData();
        DataMapHolder.get()
                .companyNumber(companyNumber);

        if (data.getLinks() != null) {
            // If no PSCs in the resource changed and PSCs exist then update company profile
            if (data.getLinks().getPersonsWithSignificantControl() == null) {
                ApiResponse<PscList> pscApiResponse = pscService
                        .getPscList(contextId, companyNumber);
                HttpStatus httpStatus = HttpStatus.resolve(pscApiResponse.getStatusCode());

                if (httpStatus == null
                        || (httpStatus != HttpStatus.NOT_FOUND && !httpStatus.is2xxSuccessful())) {
                    throw new RetryableErrorException(String.format(
                            "Resource not found for PSCs List for the resource uri %s"
                                    + "with contextId %s", resourceUri, contextId));
                } else if (httpStatus.is2xxSuccessful()
                        && pscApiResponse.getData().getTotalResults() > 0) {
                    addCompanyPscsLink(contextId, companyNumber, data);
                }
            }
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

    void addCompanyPscsLink(String logContext, String companyNumber, Data data) {
        logger.trace(String.format("Message with contextId %s and company number %s -"
                        + "company profile does not contain charges link, attaching charges link",
                logContext, companyNumber), DataMapHolder.getLogMap());

        Links links = data.getLinks() == null ? new Links() : data.getLinks();

        links.setPersonsWithSignificantControl(
                String.format("/company/%s/persons-with-significant-control", companyNumber));
        data.setLinks(links);
        var companyProfile = new CompanyProfile();
        companyProfile.setData(data);

        final ApiResponse<Void> patchResponse = companyProfileService.patchCompanyProfile(
                logContext, companyNumber, companyProfile);
        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                "PATCH", ApiType.COMPANY_PROFILE, companyNumber);
    }
}
