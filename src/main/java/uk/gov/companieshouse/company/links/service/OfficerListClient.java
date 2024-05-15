package uk.gov.companieshouse.company.links.service;

import org.apache.commons.lang.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.appointment.OfficerList;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import java.util.function.Supplier;
import uk.gov.companieshouse.logging.Logger;

@Component
public class OfficerListClient {

    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientFactory;

    public OfficerListClient(Logger logger, Supplier<InternalApiClient> internalApiClientFactory) {
        this.logger = logger;
        this.internalApiClientFactory = internalApiClientFactory;
    }

    /**
     * Retrieves a list of officers for a given company number.
     *
     * @param linkRequest LinkRequest
     * @return OfficerList
     */
    public OfficerList getOfficers(PatchLinkRequest linkRequest) {
        InternalApiClient internalApiClient = internalApiClientFactory.get();
        try {
            return internalApiClient.privateDeltaResourceHandler()
                    .getOfficers(String.format("/company/%s/officers",
                            linkRequest.getCompanyNumber()))
                    .execute()
                    .getData();
        } catch (ApiErrorResponseException ex) {
            if (ex.getStatusCode() / 100 == 5) {
                logger.error(String.format("Server error returned with status code: [%s] "
                        + "processing get Officers request", ex.getStatusCode()));
                throw new RetryableErrorException("Server error returned when processing "
                        + "get Officers request", ex);
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
                if (ex.getContent() != null
                        && !ex.getContent().contains("officers-not-found")
                        && ((ex.getHeaders().containsKey(HttpHeaders.CONTENT_LENGTH)
                        && ex.getHeaders().getContentLength() > 0)
                        || StringUtils.isEmpty(ex.getContent()))) {
                    logger.error("endpoint not found");
                    throw new RetryableErrorException("endpoint not found", ex);
                }
                // *** End HACK ALERT!!!

                logger.debug(String.format("HTTP 404 Not Found returned for company number %s",
                        linkRequest.getCompanyNumber()));
                return new OfficerList()
                        .totalResults(0);
            } else if (ex.getStatusCode() == 401) {
                logger.error(String.format("get PSCs client error returned with "
                        + "status code: [%s]", ex.getStatusCode()));
                throw new RetryableErrorException("Client error returned when "
                        + "processing get officers request", ex);
            } else {
                logger.error(String.format("get officers client error returned with "
                                + "status code: [%s]", ex.getStatusCode()));
                throw new NonRetryableErrorException("Client error returned when "
                        + "processing get officers request", ex);
            }
        } catch (IllegalArgumentException ex) {
            logger.error("Illegal argument exception caught when handling API response");
            throw new RetryableErrorException("Server error returned when processing get "
                    + "officers request", ex);
        } catch (URIValidationException ex) {
            logger.error("Invalid uri specified when handling API request");
            throw new NonRetryableErrorException("Invalid uri specified", ex);
        }
    }
}
