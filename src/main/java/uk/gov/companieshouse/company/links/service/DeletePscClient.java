package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;



@Component
public class DeletePscClient implements LinkClient {

    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientFactory;

    public DeletePscClient(Logger logger,
                        Supplier<InternalApiClient> internalApiClientFactory) {
        this.logger = logger;
        this.internalApiClientFactory = internalApiClientFactory;
    }

    /**
     * Sends a patch request to the add PSCs link endpoint in
     * the company profile api and handles any error responses.
     */
    /**
     * Sends a patch request to the add PSCs link endpoint in
     * the company profile api and handles any error responses.
     */
    @Override
    public void patchLink(PatchLinkRequest linkRequest) {

    }
}