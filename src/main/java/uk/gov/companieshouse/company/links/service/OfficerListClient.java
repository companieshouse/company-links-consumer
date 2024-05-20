package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.appointment.OfficerList;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;

@Component
public class OfficerListClient {

    private final Supplier<InternalApiClient> internalApiClientFactory;

    public OfficerListClient(Supplier<InternalApiClient> internalApiClientFactory) {
        this.internalApiClientFactory = internalApiClientFactory;
    }

    /**
     * Retrieves a list of officers for a given company number.
     *
     * @param linkRequest LinkRequest
     * @return OfficerList
     */
    public OfficerList getOfficers(PatchLinkRequest linkRequest) throws ApiErrorResponseException,
            URIValidationException {
        String uri = String.format("/company/%s/officers",
                linkRequest.getCompanyNumber());
        InternalApiClient internalApiClient = internalApiClientFactory.get();
        return internalApiClient.privateDeltaResourceHandler()
                .getOfficers(uri)
                .execute()
                .getData();
    }
}
