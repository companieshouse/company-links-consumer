package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.insolvency.CompanyInsolvency;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;

@Service
public class InsolvencyService extends BaseApiClientService {

    private final Supplier<InternalApiClient> internalApiClientSupplier;

    /**
     * Construct a company insolvency service - used to retrieve a company insolvency record.
     *
     * @param logger the CH logger
     */
    @Autowired
    public InsolvencyService(Logger logger,
                             Supplier<InternalApiClient> internalApiClientSupplier) {
        super(logger);
        this.internalApiClientSupplier = internalApiClientSupplier;
    }

    /**
     * Retrieve a company insolvency given a company number from insolvency-data-api. Note: Status
     * 410 GONE for GET insolvency-data-api is thrown deliberately in the absence of a document and
     * should not be considered an exception but be handled in the processor as a special case
     *
     * @param companyNumber the company's company number
     * @return an ApiResponse containing the CompanyInsolvencyApi data model
     */
    public ApiResponse<CompanyInsolvency> getInsolvency(String contextId,
            String companyNumber)
            throws RetryableErrorException {
        logger.trace(String.format("Call to GET insolvency-data-api with contextId %s "
                + "and company number %s", contextId, companyNumber), DataMapHolder.getLogMap());

        String uri = String.format("/company/%s/insolvency", companyNumber);

        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);

        return executeOp(contextId, internalApiClient
                .privateDeltaInsolvencyResourceHandler()
                .getInsolvency(uri));
    }

}
