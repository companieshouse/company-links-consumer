package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.insolvency.CompanyInsolvency;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.ApiType;
import uk.gov.companieshouse.logging.Logger;

@Service
public class CompanyInsolvencyService extends BaseApiClientService {

    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Value("${api.api-key}")
    private String companyInsolvencyApiKey;

    @Value("${api.api-url}")
    private String companyInsolvencyApiUrl;

    /**
     * Construct a company insolvency service - used to retrieve a company insolvency record.
     * @param logger the CH logger
     */
    @Autowired
    public CompanyInsolvencyService(Logger logger,
                                    Supplier<InternalApiClient> internalApiClientSupplier) {
        super(logger);
        this.internalApiClientSupplier = internalApiClientSupplier;
    }

    /**
     * Retrieve a company insolvency given a company number from insolvency-data-api.
     * Note: Status 410 GONE for GET insolvency-data-api is thrown deliberately
     *     in the absence of a document and should not be considered an exception
     *     but be handled in the processor as a special case
     *
     * @param companyNumber the company's company number
     * @return an ApiResponse containing the CompanyInsolvencyApi data model
     */
    public ApiResponse<CompanyInsolvency> getCompanyInsolvency(String contextId,
                                                               String companyNumber)
            throws RetryableErrorException {
        logger.trace(String.format("Call to GET insolvency-data-api with contextId %s "
                        + "and company number %s", contextId, companyNumber));

        String uri = String.format("/company/%s/insolvency", companyNumber);

        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);

        return executeOp(contextId, "GET", ApiType.INSOLVENCY, uri,
                internalApiClient
                        .privateDeltaInsolvencyResourceHandler()
                        .getInsolvency(uri));
    }

}
