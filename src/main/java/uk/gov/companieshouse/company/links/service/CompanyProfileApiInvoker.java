package uk.gov.companieshouse.company.links.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.http.ApiKeyHttpClient;
import uk.gov.companieshouse.api.http.HttpClient;

@Component
public class CompanyProfileApiInvoker {

    @Value("${api.company-profile-api-key}")
    private String companyProfileApiKey;

    @Value("${api.endpoint}")
    private String companyProfileApiUrl;

    /**
     * Get an internal api client instance.
     */
    public InternalApiClient getApiClient(String contextId) {
        InternalApiClient apiClient = new InternalApiClient(getHttpClient(contextId));
        apiClient.setBasePath(companyProfileApiUrl);
        return apiClient;
    }

    private HttpClient getHttpClient(String contextId) {
        ApiKeyHttpClient httpClient = new ApiKeyHttpClient(companyProfileApiKey);
        httpClient.setRequestId(contextId);
        return httpClient;
    }
}
