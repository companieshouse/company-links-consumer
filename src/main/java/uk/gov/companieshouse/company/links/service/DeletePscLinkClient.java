package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

@Component
public class DeletePscLinkClient implements LinkClient {

    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientFactory;

    public DeletePscLinkClient(Logger logger,
                               Supplier<InternalApiClient> internalApiClientFactory) {
        this.logger = logger;
        this.internalApiClientFactory = internalApiClientFactory;
    }

    /**
     * Sends a patch request to the remove psc link endpoint in the company profile api and
     * handles any error responses.
     *
     * @param linkRequest PatchLinkRequest
     * @return PscList
     */
    @Override
    public PscList patchLink(PatchLinkRequest linkRequest) {
        InternalApiClient internalApiClient = internalApiClientFactory.get();
        try {
            internalApiClient.privateCompanyLinksResourceHandler()
                    .deletePscCompanyLink(String.format(
                            "/company/%s/links/persons-with-significant-control/delete",
                            linkRequest.getCompanyNumber()))
                        .execute();
        } catch (ApiErrorResponseException ex) {
            if (ex.getStatusCode() / 100 == 5) {
                logger.error(String.format("Server error returned with status code: [%s] "
                        + "processing remove PSC link request", ex.getStatusCode()));
                throw new RetryableErrorException("Server error returned when processing "
                        + "remove PSC link request", ex);
            } else if (ex.getStatusCode() == 409) {
                logger.info("HTTP 409 Conflict returned; "
                        + "company profile does not have a PSC link already");
            } else if (ex.getStatusCode() == 404) {
                logger.info("HTTP 404 Not Found returned; "
                        + "company profile does not exist");
            } else {
                logger.error(String.format("remove PSC client error returned with "
                            + "status code: [%s] when processing remove PSC link request",
                        ex.getStatusCode()));
                throw new NonRetryableErrorException("Client error returned when "
                    + "processing remove PSC link request", ex);
            }
        } catch (IllegalArgumentException ex) {
            logger.error("Illegal argument exception caught when handling API response");
            throw new RetryableErrorException("Server error returned when processing remove "
                + "PSC link request", ex);
        } catch (URIValidationException ex) {
            logger.error("Invalid uri specified when handling API request");
            throw new NonRetryableErrorException("Invalid uri specified", ex);
        }
        return null;
    }
}
