package uk.gov.companieshouse.company.links.service;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.appointment.OfficerList;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@Component
public class RemoveOfficersClient implements LinkClient {
    private final Logger logger;
    private final AppointmentsListClient appointmentsListClient;
    private final RemoveOfficersLinkClient removeOfficersLinkClient;

    public RemoveOfficersClient(Logger logger,
                                AppointmentsListClient appointmentsListClient,
                                RemoveOfficersLinkClient removeOfficersLinkClient) {
        this.logger = logger;
        this.appointmentsListClient = appointmentsListClient;
        this.removeOfficersLinkClient = removeOfficersLinkClient;
    }

    /**
     * Sends a patch request to the remove officers link endpoint in the company profile api and
     * handles any error responses.
     *
     * @param linkRequest
     */
    @Override
    public void patchLink(PatchLinkRequest linkRequest) {
        try {

            OfficerList officerList = appointmentsListClient.getAppointmentsList(companyNumber);
            if (officerList.getTotalResults() == 0) {
                removeOfficersLinkClient.patchLink(companyNumber);
            } else {

            }
        } catch (ApiErrorResponseException ex) {
            if (HttpStatus.valueOf(ex.getStatusCode()).is5xxServerError()) {
                logger.error(String.format("Server error returned with status code: [%s] "
                        + "processing remove officers link request", ex.getStatusCode()));
                throw new RetryableErrorException("Server error returned when processing "
                        + "remove officers link request", ex);
            } else if (ex.getStatusCode() == 409) {
                logger.info("HTTP 409 Conflict returned; "
                        + "company profile does not have an officers link already");
            } else if (ex.getStatusCode() == 404) {
                logger.info("HTTP 404 Not Found returned; "
                        + "company profile does not exist");
            } else {
                logger.error(String.format("remove officers client error returned with "
                          + "status code: [%s] when processing remove officers link request",
                        ex.getStatusCode()));
                throw new NonRetryableErrorException("Client error returned when "
                        + "processing remove officers link request", ex);
            }
        } catch (IllegalArgumentException ex) {
            logger.error("Illegal argument exception caught when handling API response");
            throw new RetryableErrorException("Server error returned when processing remove "
                    + "officers link request", ex);
        } catch (URIValidationException ex) {
            logger.error("Invalid companyNumber specified when handling API request");
            throw new NonRetryableErrorException("Invalid companyNumber specified", ex);
        }
    }
}
