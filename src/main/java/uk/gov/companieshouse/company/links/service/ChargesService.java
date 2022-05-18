package uk.gov.companieshouse.company.links.service;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.http.ApiKeyHttpClient;
import uk.gov.companieshouse.api.http.HttpClient;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@Service
public class ChargesService extends BaseApiClientService {

    @Value("${api.charges-data-api-key}")
    private String chargesApiKey;

    @Value("${api.charges-data-api-url}")
    private String chargesApiUrl;

    /**
     * Construct a company profile service - used to retrieve a company profile record.
     * @param logger the CH logger
     */
    @Autowired
    public ChargesService(Logger logger) {
        super(logger);
    }

    /**
     * Retrieve company charges given a company number from charges-data-api.
     *
     * @param companyNumber the company's company number
     * @return an ApiResponse containing the charges data model
     */
    public ApiResponse<ChargesApi> getCharges(String contextId, String companyNumber)
        throws RetryableErrorException {
        String uri = String.format("/company/%s/charges", companyNumber);

        Map<String, Object> logMap = createLogMap(companyNumber, "GET", uri);
        logger.infoContext(contextId, String.format("GET %s", uri), logMap);
        return executeOp(contextId, "getCharges", uri,
            getApiClient(contextId)
                .privateDeltaChargeResourceHandler()
                .getCharges(uri));
    }

    /**
     * Get an internal api client instance.
     */
    public InternalApiClient getApiClient(String contextId) {
        InternalApiClient apiClient = new InternalApiClient(getHttpClient(contextId));
        apiClient.setBasePath(chargesApiUrl);
        return apiClient;
    }

    private HttpClient getHttpClient(String contextId) {
        ApiKeyHttpClient httpClient = new ApiKeyHttpClient(chargesApiKey);
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
