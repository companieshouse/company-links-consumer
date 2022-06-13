package uk.gov.companieshouse.company.links.service;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.ApiType;
import uk.gov.companieshouse.logging.Logger;

@Service
public class ChargesService extends BaseApiClientService {

    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Value("${api.api-key}")
    private String chargesApiKey;

    @Value("${api.api-url}")
    private String chargesApiUrl;

    /**
     * Construct a company profile service - used to retrieve a company profile record.
     * @param logger the CH logger
     */
    @Autowired
    public ChargesService(Logger logger,
                          Supplier<InternalApiClient> internalApiClientSupplier) {
        super(logger);
        this.internalApiClientSupplier = internalApiClientSupplier;
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

        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);

        return executeOp(contextId, "GET", ApiType.CHARGES, uri,
                internalApiClient
                .privateDeltaChargeResourceHandler()
                .getCharges(uri));
    }

    private Map<String, Object> createLogMap(String companyNumber, String method, String path) {
        final Map<String, Object> logMap = new HashMap<>();
        logMap.put("company_number", companyNumber);
        logMap.put("method", method);
        logMap.put("path", path);
        return logMap;
    }

}
