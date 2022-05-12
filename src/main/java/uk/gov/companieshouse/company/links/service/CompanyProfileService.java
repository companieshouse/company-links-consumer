package uk.gov.companieshouse.company.links.service;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.handler.Executor;
import uk.gov.companieshouse.api.handler.company.PrivateCompanyResourceHandler;
import uk.gov.companieshouse.api.handler.company.request.PrivateCompanyProfileGet;
import uk.gov.companieshouse.api.handler.company.request.PrivateCompanyProfilePatch;
import uk.gov.companieshouse.api.http.ApiKeyHttpClient;
import uk.gov.companieshouse.api.http.HttpClient;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

//import static uk.gov.companieshouse.company.links.processor.ResponseHandler.handleResponse;

@Service
public class CompanyProfileService extends BaseApiClientService {

    @Value("${api.company-profile-api-key}")
    private String companyProfileApiKey;

    @Value("${api.endpoint}")
    private String companyProfileApiUrl;

    /**
     * Construct a company profile service - used to retrieve a company profile record.
     * @param logger the CH logger
     */
    @Autowired
    public CompanyProfileService(Logger logger) {
        super(logger);
    }

    /**
     * CompanyProfileApiInvoker
     */
    @Autowired
    public CompanyProfileApiInvoker companyProfileApiInvoker;

    /**
     * Retrieve a company profile given a company number from company-profile-api.
     *
     * @param companyNumber the company's company number
     * @return an ApiResponse containing the CompanyProfileApi data model
     */
    public ApiResponse<CompanyProfile> getCompanyProfile(String contextId, String companyNumber)
            throws RetryableErrorException {
        String uri = String.format("/company/%s/links", companyNumber);

        Map<String, Object> logMap = createLogMap(companyNumber, "GET", uri);
        logger.infoContext(contextId, String.format("GET %s", uri), logMap);

        PrivateCompanyProfileGet companyProfileApiResponse = companyProfileApiInvoker.getApiClient(contextId)
                .privateCompanyResourceHandler()
                .getCompanyProfile(uri);

        final ApiResponse<CompanyProfile> getResponse = executeOp(contextId, "getCompanyProfileApi", uri,
                companyProfileApiResponse);

        logger.trace(String.format("Retrieved company profile for company number %s: %s",
                companyNumber, getResponse.getData()));

        handleResponse(HttpStatus.valueOf(getResponse.getStatusCode()), contextId,
                "Response from GET call to company profile api", logMap, logger);

        return getResponse;
    }

//    /**
//     * Get an internal api client instance.
//     */
//    public InternalApiClient getApiClient(String contextId) {
//        InternalApiClient apiClient = new InternalApiClient(getHttpClient(contextId));
//        apiClient.setBasePath(companyProfileApiUrl);
//        return apiClient;
//    }

//    private HttpClient getHttpClient(String contextId) {
//        ApiKeyHttpClient httpClient = new ApiKeyHttpClient(companyProfileApiKey);
//        httpClient.setRequestId(contextId);
//        return httpClient;
//    }

    /**
     * Update a company profile given a company number using PATCH from company-profile-api.
     *
     * @param companyNumber the company's company number
     * @param companyProfile the company profile
     * @return an ApiResponse
     */
    public ApiResponse<Void> patchCompanyProfile(String contextId, String companyNumber,
                                                 CompanyProfile companyProfile) {
        String uri = String.format("/company/%s/links", companyNumber);

        Map<String, Object> logMap = createLogMap(companyNumber, "PATCH", uri);
        logger.infoContext(contextId, String.format("PATCH %s", uri), logMap);

        PrivateCompanyProfilePatch companyProfileApiResponse =
                companyProfileApiInvoker.getApiClient(contextId)
                .privateCompanyResourceHandler()
                .patchCompanyProfile(uri, companyProfile);

        final ApiResponse<Void> patchResponse = executeOp(contextId, "patchCompanyProfileApi", uri,
                companyProfileApiResponse);

        logger.trace(String.format("Performing a PATCH with new company profile %s",
                companyProfile));

        handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), contextId,
                "Response from PATCH call to company profile api", logMap, logger);

        return patchResponse;
    }

    private Map<String, Object> createLogMap(String companyNumber, String method, String path) {
        final Map<String, Object> logMap = new HashMap<>();
        logMap.put("company_number", companyNumber);
        logMap.put("method", method);
        logMap.put("path", path);
        return logMap;
    }
}
