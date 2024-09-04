package uk.gov.companieshouse.company.links.util;

import java.util.Arrays;
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
        logger.info(ILLEGAL_ARG_MSG + causeMessage,
                DataMapHolder.getLogMap());
        throw new RetryableErrorException(ILLEGAL_ARG_MSG, ex);
    }

    /**
     * Handler method for api error response exceptions.
     *
     * @param ex ApiErrorResponseException
     */
    public void handle(int statusCode, String linkType, ApiErrorResponseException ex) {
        HttpStatus httpsStatus = HttpStatus.valueOf(statusCode);
        final String reason = httpsStatus.getReasonPhrase();

        if (httpsStatus == HttpStatus.BAD_REQUEST) {
            final String msg = String.format("PATCH %s link returned %d %s [non-retryable]",
                    linkType, statusCode, reason);
            logger.error(msg, ex, DataMapHolder.getLogMap());
            throw new NonRetryableErrorException(msg, ex);
        } else if (httpsStatus == HttpStatus.CONFLICT) {
            final String msg =
                    String.format("%s link already present in target resource - continuing process",
                            linkType);
            logger.info(msg, DataMapHolder.getLogMap());
        } else {
            final String msg =
                    String.format("PATCH %s link returned %d %s [retryable]: %s",
                            linkType, statusCode, reason, Arrays.toString(ex.getStackTrace()));
            logger.info(msg, DataMapHolder.getLogMap());
            throw new RetryableErrorException(msg);
        }
    }
}
