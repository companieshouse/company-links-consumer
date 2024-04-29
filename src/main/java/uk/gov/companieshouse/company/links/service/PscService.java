package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;

@Service
public class PscService extends BaseApiClientService {

    private final Supplier<InternalApiClient> internalApiClientSupplier;

    /**
     * Construct a psc service - used to retrieve a psc record.
     * @param logger the CH logger
     */
    @Autowired
    public PscService(Logger logger, Supplier<InternalApiClient> internalApiClientSupplier) {
        super(logger);
        this.internalApiClientSupplier = internalApiClientSupplier;
    }

    /**
     * Retrieve a psc given a company number from psc-data-api.
     *
     * @param companyNumber the company's company number
     * @return an ApiResponse containing the CompanyProfileApi data model
     */
    public ApiResponse<PscList> getPscList(String contextId, String companyNumber)
            throws RetryableErrorException {
        logger.trace(String.format("Call to GET list of PSCs with contextId %s "
                + "and company number %s", contextId, companyNumber), DataMapHolder.getLogMap());

        String uri = String.format("/company/%s/persons-with-significant-control", companyNumber);

        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);

        return executeOp(contextId, internalApiClient
                .privatePscFullRecordResourceHandler()
                .getPscs(uri));
    }

}
