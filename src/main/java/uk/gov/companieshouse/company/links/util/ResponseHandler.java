package uk.gov.companieshouse.company.links.util;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;

@Component
public class ResponseHandler {

    private final Logger logger;

    private static final String SERVER_FAILED_MSG = "Server error returned with status code: [%s] "
            + "when processing add company %s link";
    private static final String CONFLICT_ERROR_MSG = "HTTP %s returned; "
            + "company profile already has a %s link";
    private static final String NOT_FOUND_ERROR_MSG = "HTTP %s returned; "
            + "company profile does not exist";
    private static final String CLIENT_ERROR_MSG = "Add %s client error returned with "
            + "status code: [%s] when processing link request";
    private static final String ILLEGAL_ARG_MSG = "Illegal argument exception caught when "
            + "handling API response";
    private static final String URI_VALIDATION_MSG = "Invalid companyNumber [%s] when "
            + "handling API request";

    public ResponseHandler(Logger logger) {
        this.logger = logger;
    }

    /**
     * Handler method for uri  validation exceptions.
     *
     * @param companyNumber String
     */
    public void handle(String companyNumber, URIValidationException ex) {
        logger.error(String.format(URI_VALIDATION_MSG, companyNumber), DataMapHolder.getLogMap());
        throw new NonRetryableErrorException(String.format(URI_VALIDATION_MSG, companyNumber), ex);
    }

    /**
     * Handler method for illegal argument exceptions.
     *
     * @param ex IllegalArgumentException
     */
    public void handle(IllegalArgumentException ex) {
        String causeMessage = ex.getCause() != null
                ? String.format("; %s", ex.getCause().getMessage()) : "";
        logger.error(ILLEGAL_ARG_MSG + causeMessage,
                DataMapHolder.getLogMap());
        throw new RetryableErrorException(ILLEGAL_ARG_MSG, ex);
    }

    /**
     * Handler method for api error response exceptions.
     *
     * @param ex ApiErrorResponseException
     */
    public void handle(int statusCode, String linkType, ApiErrorResponseException ex) {
        if (HttpStatus.valueOf(ex.getStatusCode()).is5xxServerError()) {
            logger.info(String.format(SERVER_FAILED_MSG, statusCode, linkType),
                    DataMapHolder.getLogMap());
            throw new RetryableErrorException(String.format(SERVER_FAILED_MSG,
                    statusCode, linkType), ex);
        } else if (HttpStatus.valueOf(ex.getStatusCode()).equals(HttpStatus.CONFLICT)) {
            logger.info(String.format(CONFLICT_ERROR_MSG, "409 Conflict", linkType),
                    DataMapHolder.getLogMap());
            throw new NonRetryableErrorException(String.format(CONFLICT_ERROR_MSG,
                    "409 Conflict", linkType), ex);
        } else if (HttpStatus.valueOf(ex.getStatusCode()).equals(HttpStatus.NOT_FOUND)) {
            logger.info(String.format(NOT_FOUND_ERROR_MSG, "404 Not Found"));
            throw new RetryableErrorException(String.format(NOT_FOUND_ERROR_MSG,
                    "404 Not Found"), ex);
        } else {
            logger.error(String.format(CLIENT_ERROR_MSG, linkType, ex.getStatusCode()),
                    DataMapHolder.getLogMap());
            throw new NonRetryableErrorException(String.format(CLIENT_ERROR_MSG, linkType,
                    ex.getStatusCode()), ex);
        }
    }
}
