package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.exemptions.CompanyExemptions;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.logging.Logger;

@Component
public class ExemptionsListClient {

    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientSupplier;

    public ExemptionsListClient(Logger logger,
                                Supplier<InternalApiClient> internalApiClientSupplier) {
        this.logger = logger;
        this.internalApiClientSupplier = internalApiClientSupplier;
    }

    /**
     * Retrieve company exemptions given a company number from company-exemptions-data-api.
     *
     * @param companyNumber the company's company number
     * @return an ApiResponse containing the exemptions data model
     */
    public CompanyExemptions getExemptionsList(String contextId, String companyNumber)
            throws ApiErrorResponseException, URIValidationException {
        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);

        String uri = String.format("/company/%s/exemptions", companyNumber);

        return internalApiClient.privateDeltaResourceHandler()
                .getCompanyExemptionsResource(uri)
                .execute()
                .getData();
    }
}
