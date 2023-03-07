package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.appointment.OfficerList;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@Component
public class AppointmentsListClient {

    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientFactory;

    public AppointmentsListClient(Logger logger,
            Supplier<InternalApiClient> internalApiClientFactory) {
        this.logger = logger;
        this.internalApiClientFactory = internalApiClientFactory;
    }

    /**
     * Retrieves a list of officers appointed to a company.
     *
     * @param companyNumber The companyNumber
     * @return OfficerList
     */
    public OfficerList getAppointmentsList(String companyNumber) {
        InternalApiClient client = internalApiClientFactory.get();
        try {
            return client.privateCompanyAppointmentsListHandler()
                    .getCompanyAppointmentsList(
                            String.format("/company/%s/officers-test", companyNumber))
                    .execute()
                    .getData();
        } catch (ApiErrorResponseException ex) {

            if (HttpStatus.valueOf(ex.getStatusCode()).is5xxServerError()) {
                String message = String.format("Server error status code: [%s] "
                                + "while fetching appointments list for company %s",
                        ex.getStatusCode(),
                        companyNumber);
                logger.error(message);
                throw new RetryableErrorException(message, ex);
            } else if (ex.getStatusCode() == 404) {
                logger.debug(String.format("HTTP 404 Not Found returned for company number %s",
                        companyNumber));
                return new OfficerList()
                        .totalResults(0);
            } else {
                String message = String.format("Client error status code: [%s] "
                        + "while fetching appointments list", ex.getStatusCode());
                logger.error(message);
                throw new NonRetryableErrorException(message, ex);
            }
        } catch (IllegalArgumentException ex) {
            logger.error("Illegal argument exception caught when handling API response");
            throw new RetryableErrorException("Error returned when fetching appointments", ex);
        } catch (URIValidationException ex) {
            String message = String.format("Invalid companyNumber [%s] when handling API request",
                    companyNumber);
            logger.error(message);
            throw new NonRetryableErrorException(message, ex);
        }
    }
}
