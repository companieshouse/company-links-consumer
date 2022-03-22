package uk.gov.companieshouse.company.links.service;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.http.ApiKeyHttpClient;
import uk.gov.companieshouse.api.http.HttpClient;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.service.api.BaseApiClientServiceImpl;
import uk.gov.companieshouse.logging.Logger;

@Service
public class CompanyProfileService extends BaseApiClientServiceImpl {

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
     * Retrieve a company profile given a company number from company-profile-api.
     *
     * @param companyNumber the company's company number
     * @return an ApiResponse containing the CompanyProfileApi data model
     */
    public ApiResponse<CompanyProfile> getCompanyProfile(String contextId, String companyNumber) {
        var uri = String.format("/company/%s", companyNumber);

        Map<String, Object> logMap = createLogMap(companyNumber, "GET", uri);
        logger.infoContext(contextId, String.format("GET %s", uri), logMap);

        return executeOp(contextId, "getCompanyProfileApi", uri,
                getApiClient(contextId)
                        .privateCompanyResourceHandler()
                        .getCompanyProfile(uri));
    }

    /**
     * Get an internal api client instance.
     */
    public InternalApiClient getApiClient(String contextId) {
        var apiClient = new InternalApiClient(getHttpClient(contextId));
        apiClient.setBasePath(companyProfileApiUrl);
        return apiClient;
    }

    private HttpClient getHttpClient(String contextId) {
        var httpClient = new ApiKeyHttpClient(companyProfileApiKey);
        httpClient.setRequestId(contextId);
        return httpClient;
    }

    private Map<String, Object> createLogMap(String companyNumber, String method, String path) {
        final Map<String, Object> logMap = new HashMap<>();
        logMap.put("company_number", companyNumber);
        logMap.put("method", method);
        logMap.put("path", path);
        return logMap;
    }
}
