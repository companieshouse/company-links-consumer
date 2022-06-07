package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.ApiType;
import uk.gov.companieshouse.logging.Logger;

@Service
public class CompanyProfileService extends BaseApiClientService {

    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Value("${api.api-key}")
    private String companyProfileApiKey;

    @Value("${api.api-url}")
    private String companyProfileApiUrl;

    /**
     * Construct a company profile service - used to retrieve a company profile record.
     * @param logger the CH logger
     */
    @Autowired
    public CompanyProfileService(Logger logger,
                                 Supplier<InternalApiClient> internalApiClientSupplier) {
        super(logger);
        this.internalApiClientSupplier = internalApiClientSupplier;
    }

    /**
     * Retrieve a company profile given a company number from company-profile-api.
     *
     * @param companyNumber the company's company number
     * @return an ApiResponse containing the CompanyProfileApi data model
     */
    public ApiResponse<CompanyProfile> getCompanyProfile(String contextId, String companyNumber)
            throws RetryableErrorException {
        logger.trace(String.format("Call to GET company profile with contextId %s "
                        + "and company number %s", contextId, companyNumber));

        String uri = String.format("/company/%s/links", companyNumber);

        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);

        return executeOp(contextId, "GET", ApiType.COMPANY_PROFILE, uri,
                internalApiClient
                        .privateCompanyResourceHandler()
                        .getCompanyProfile(uri));
    }

    /**
     * Update a company profile given a company number using PATCH from company-profile-api.
     *
     * @param companyNumber the company's company number
     * @param companyProfile the company profile
     * @return an ApiResponse
     */
    public ApiResponse<Void> patchCompanyProfile(String contextId, String companyNumber,
                                                 CompanyProfile companyProfile) {
        logger.trace(String.format("Call to PATCH company profile with contextId %s "
                + "and company number %s", contextId, companyNumber));

        String uri = String.format("/company/%s/links", companyNumber);

        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);

        return executeOp(contextId, "PATCH", ApiType.COMPANY_PROFILE, uri,
                internalApiClient
                        .privateCompanyResourceHandler()
                        .patchCompanyProfile(uri, companyProfile));
    }
}
