package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

@Component("removeOfficersLinkClient")
public class RemoveOfficersLinkClient implements LinkClient {

    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientFactory;

    public RemoveOfficersLinkClient(Logger logger,
            Supplier<InternalApiClient> internalApiClientFactory) {
        this.logger = logger;
        this.internalApiClientFactory = internalApiClientFactory;
    }

    /**
     * Sends a patch request to the remove officers link endpoint in the company profile api and
     * handles any error responses.
     *
     * @param linkRequest PatchLinkRequest
     */
    @Override
    public void patchLink(PatchLinkRequest linkRequest) {
        InternalApiClient client = internalApiClientFactory.get();
        client.getHttpClient().setRequestId(linkRequest.getRequestId());
        try {
            client.privateCompanyLinksResourceHandler()
                    .removeOfficersCompanyLink(
                            String.format("/company/%s/links/officers/delete",
                                    linkRequest.getCompanyNumber()))
                    .execute();
        } catch (ApiErrorResponseException ex) {
            if (ex.getStatusCode() / 100 == 5) {
                logger.error(String.format("Server error returned with status code: [%s] "
                                + "processing remove officers link request", ex.getStatusCode()),
                        DataMapHolder.getLogMap());
                throw new RetryableErrorException("Server error returned when processing "
                        + "remove officers link request", ex);
            } else if (ex.getStatusCode() == 409) {
                logger.info("HTTP 409 Conflict returned; "
                                + "company profile does not have an officers link already",
                        DataMapHolder.getLogMap());
            } else if (ex.getStatusCode() == 404) {
                logger.info("HTTP 404 Not Found returned; "
                        + "company profile does not exist", DataMapHolder.getLogMap());
            } else {
                logger.error(String.format("remove officers client error returned with "
                                + "status code: [%s] when processing remove officers link request",
                        ex.getStatusCode()), DataMapHolder.getLogMap());
                throw new NonRetryableErrorException("Client error returned when "
                        + "processing remove officers link request", ex);
            }
        } catch (IllegalArgumentException ex) {
            logger.error("Illegal argument exception caught when handling API response",
                    DataMapHolder.getLogMap());
            throw new RetryableErrorException("Server error returned when processing remove "
                    + "officers link request", ex);
        } catch (URIValidationException ex) {
            logger.error("Invalid companyNumber specified when handling API request",
                    DataMapHolder.getLogMap());
            throw new NonRetryableErrorException("Invalid companyNumber specified", ex);
        }
    }
}
