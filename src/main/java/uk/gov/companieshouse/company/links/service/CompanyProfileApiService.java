package uk.gov.companieshouse.company.links.service;

import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.model.ApiResponse;

//TODO Do we need Api service component for this consumer?
@Service
public class CompanyProfileApiService {

    /**
     * Invoke Company Profile API.
     */
    public ApiResponse<?> invokeCompanyProfileApi() {
        InternalApiClient internalApiClient = getInternalApiClient();
        internalApiClient.setBasePath("apiUrl");

        return null;
    }

    @Lookup
    public InternalApiClient getInternalApiClient() {
        return null;
    }
}
