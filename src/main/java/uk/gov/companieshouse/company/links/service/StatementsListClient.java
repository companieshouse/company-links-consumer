package uk.gov.companieshouse.company.links.service;

import static org.springframework.http.HttpHeaders.CONTENT_LENGTH;

import java.util.function.Supplier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.psc.StatementList;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@Component
public class StatementsListClient {

    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientFactory;

    public StatementsListClient(Logger logger,
            Supplier<InternalApiClient> internalApiClientFactory) {
        this.logger = logger;
        this.internalApiClientFactory = internalApiClientFactory;
    }

    /**
     * Retrieves a list of statements for a given company number.
     *
     * @param companyNumber The companyNumber
     * @return StatementList
     */
    public StatementList getStatementsList(String companyNumber) {
        InternalApiClient client = internalApiClientFactory.get();
        try {
            return client.privateDeltaResourceHandler()
                    .getPscStatements(
                            String.format("/company/%s/persons-with-significant-control-statements",
                                    companyNumber))
                    .execute()
                    .getData();
        } catch (ApiErrorResponseException ex) {

            if (HttpStatus.valueOf(ex.getStatusCode()).is5xxServerError()) {
                String message = String.format("Server error status code: [%s] "
                                + "while fetching statements list for company %s",
                        ex.getStatusCode(),
                        companyNumber);
                logger.error(message);
                throw new RetryableErrorException(message, ex);
            } else if (ex.getStatusCode() == 404) {

                // *** HACK ALERT!!!
                // A 404 will be received from an API client under two conditions
                //   1. The requested resource was not present in the database, or
                //   2. The target service is not available
                // When the services are hosted on ECS the latter case will be fixed and a 500
                // response will be used to signal a service is down.
                // In the meantime, a zero length response body is seen when a resource is not
                // found (case 1), and a non-zero response body is seen when a service is
                // unavailable (case 2).
                if ((ex.getHeaders().containsKey(CONTENT_LENGTH)
                        && ex.getHeaders().getContentLength() > 0)
                        || (ex.getContent() != null && ex.getContent().length() > 0)) {
                    logger.error("psc-statements-data-api service is not available");
                    throw new RetryableErrorException(
                            "psc-statements-data-api service is not available", ex);
                }
                // *** End HACK ALERT!!!

                logger.debug(String.format("HTTP 404 Not Found returned for company number %s",
                        companyNumber));
                return new StatementList()
                        .totalResults(0);
            } else {
                String message = String.format("Client error status code: [%s] "
                        + "while fetching statements list", ex.getStatusCode());
                logger.error(message);
                throw new NonRetryableErrorException(message, ex);
            }
        } catch (IllegalArgumentException ex) {
            logger.error("Illegal argument exception caught when handling API response");
            throw new RetryableErrorException("Error returned when fetching statements", ex);
        } catch (URIValidationException ex) {
            String message = String.format("Invalid companyNumber [%s] when handling API request",
                    companyNumber);
            logger.error(message);
            throw new NonRetryableErrorException(message, ex);
        }
    }
}
