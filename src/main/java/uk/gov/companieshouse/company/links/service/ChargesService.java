package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.charges.ChargeApi;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.handler.delta.charges.request.PrivateChargesGet;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;

@Service
public class ChargesService extends BaseApiClientService {

    private final Supplier<InternalApiClient> internalApiClientSupplier;

    /**
     * Construct a company profile service - used to retrieve a company profile record.
     *
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

        logger.infoContext(contextId, String.format("GET %s", uri), DataMapHolder.getLogMap());

        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);

        return executeOp(contextId, internalApiClient
                .privateDeltaChargeResourceHandler()
                .getCharges(uri));
    }

    /**
     * Retrieve company charge given a resource URI from charges-data-api.
     *
     * @param uri the company's charge id
     * @return an ApiResponse containing the charge data model
     */
    public ApiResponse<ChargeApi> getACharge(String contextId,
            String uri) {
        logger.infoContext(contextId, String.format("GET %s", uri), DataMapHolder.getLogMap());
        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);
        PrivateChargesGet privateChargesGet =
                internalApiClient.privateDeltaChargeResourceHandler()
                        .getACharge(uri);
        return executeOp(contextId, privateChargesGet);
    }
}
