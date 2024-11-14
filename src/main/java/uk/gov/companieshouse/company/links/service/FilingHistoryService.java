package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.filinghistory.FilingHistoryList;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;

@Service
public class FilingHistoryService extends BaseApiClientService {

    private final Supplier<InternalApiClient> internalApiClientSupplier;

    /**
     * Construct a company profile service - used to retrieve a company profile record.
     *
     * @param logger the CH logger
     */
    @Autowired
    public FilingHistoryService(Logger logger,
                                Supplier<InternalApiClient> internalApiClientSupplier) {
        super(logger);
        this.internalApiClientSupplier = internalApiClientSupplier;
    }

    /**
     * Retrieve company filing history given a company number from filing-history-data-api.
     *
     * @param companyNumber the company's company number
     * @return an ApiResponse containing the filing history data model
     */
    public FilingHistoryList getFilingHistory(String contextId, String companyNumber)
            throws RetryableErrorException, ApiErrorResponseException, URIValidationException {
        String uri = String.format("/company/%s/filing-history",
                companyNumber);

        logger.infoContext(contextId, String.format("GET %s", uri), DataMapHolder.getLogMap());

        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);

        return internalApiClient.privateDeltaResourceHandler()
                .getAllFilingHistory(uri).execute().getData();
    }
}