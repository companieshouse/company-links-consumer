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
        final String companyNumber = payload.getResourceId();
        DataMapHolder.get()
                .companyNumber(companyNumber);
        final ApiResponse<CompanyProfile> response = getCompanyProfile(payload, companyNumber);
        var data = response.getData().getData();
        processPscLink(contextId, companyNumber, data);
    }

    /**
     * Process the PSCs link for a Company Profile ResourceChanged message.
     * If there is no PSCs link in the ResourceChanged and PSCs exist then add the link
     */
    private void processPscLink(String contextId, String companyNumber, Data data) {
        if (data.getLinks().getPersonsWithSignificantControl() == null) {
            ApiResponse<PscList> pscApiResponse = pscService
                    .getPscList(contextId, companyNumber);
            HttpStatus httpStatus = HttpStatus.resolve(pscApiResponse.getStatusCode());

            if (httpStatus == null || !httpStatus.is2xxSuccessful()) {
                throw new RetryableErrorException(String.format(
                        "Resource not found for PSCs List for company number %s"
                                + "and contextId %s", companyNumber, contextId));
            }
            if (pscApiResponse.getData() != null
                    && pscApiResponse.getData().getTotalResults() != null
                    && pscApiResponse.getData().getTotalResults() > 0) {
                addCompanyPscsLink(contextId, companyNumber, data);
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

    private void addCompanyPscsLink(String logContext, String companyNumber, Data data) {
        logger.trace(String.format("Message with contextId %s and company number %s -"
                        + "company profile does not contain PSC link, attaching PSC link",
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
