package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.model.registers.CompanyRegistersApi;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;

@Service
public class RegistersService extends BaseApiClientService {

    private final Supplier<InternalApiClient> internalApiClientSupplier;

    /**
     * Construct a company registers service - used to retrieve a company registers record.
     *
     * @param logger the CH logger
     */
    @Autowired
    public RegistersService(Logger logger,
                            Supplier<InternalApiClient> internalApiClientSupplier) {
        super(logger);
        this.internalApiClientSupplier = internalApiClientSupplier;
    }

    /**
     * Retrieve a company registers resource given a company number from registers-data-api.
     *
     * @param companyNumber the company's company number
     * @return an ApiResponse containing the CompanyRegistersApi data model
     */
    public ApiResponse<CompanyRegistersApi> getRegisters(String contextId,
                                                          String companyNumber)
            throws RetryableErrorException {
        logger.trace(String.format("Call to GET registers-data-api with contextId %s "
                + "and company number %s", contextId, companyNumber), DataMapHolder.getLogMap());

        String uri = String.format("/company/%s/registers", companyNumber);

        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);

        return executeOp(contextId, internalApiClient.registers().list(uri));
    }

}
