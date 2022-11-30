package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@Component
public class AddExemptionsClient {
    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientFactory;

    public AddExemptionsClient(Logger logger,
            Supplier<InternalApiClient> internalApiClientFactory) {
        this.logger = logger;
        this.internalApiClientFactory = internalApiClientFactory;
    }

    /**
     * Sends a patch request to the add exemptions link endpoint in the
     * company profile api and handles any error responses.
     *
     * @param path The path of the patch request
     */
    public void addExemptionsLink(String path) {
        InternalApiClient client = internalApiClientFactory.get();
        try {
            client.privateCompanyLinksResourceHandler()
                    .addExemptionsCompanyLink(path)
                    .execute();
        } catch (ApiErrorResponseException ex) {
            if (ex.getStatusCode() / 100 == 5) {
                logger.error(String.format("Server error returned with status code: [%s] "
                        + "processing add exemptions link request", ex.getStatusCode()));
                throw new RetryableErrorException("Server error returned when processing "
                        + "add exemptions link request", ex);
            } else if (ex.getStatusCode() == 409) {
                logger.info("HTTP 409 Conflict returned; "
                        + "company profile already had exemptions link");
            } else if (ex.getStatusCode() == 404) {
                logger.info("HTTP 404 Not Found returned; "
                        + "company profile does not exist");
            } else {
                logger.error(String.format("Add exemptions client error returned with "
                        + "status code: [%s] when processing add exemptions link request",
                        ex.getStatusCode()));
                throw new NonRetryableErrorException("UpsertClient error returned when "
                        + "processing add exemptions link request", ex);
            }
        } catch (IllegalArgumentException ex) {
            logger.error("Illegal argument exception caught when handling API response");
            throw new RetryableErrorException("Server error returned when processing add "
                    + "exemptions link request", ex);
        } catch (URIValidationException ex) {
            logger.error("Invalid path specified when handling API request");
            throw new NonRetryableErrorException("Invalid path specified", ex);
        }
    }
}
