package uk.gov.companieshouse.company.links.processor;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import uk.gov.companieshouse.company.links.exception.NonRetryErrorException;
import uk.gov.companieshouse.company.links.exception.RetryErrorException;
import uk.gov.companieshouse.logging.Logger;


public class ResponseHandler {

    /**
     * Common response handler.
     */
    public static void handleResponse(
            final ResponseStatusException ex,
            final HttpStatus httpStatus,
            final String logContext,
            final String msg,
            final Map<String, Object> logMap,
            final Logger logger)
            throws NonRetryErrorException, RetryErrorException {
        logMap.put("status", httpStatus.toString());
        if (HttpStatus.BAD_REQUEST == httpStatus) {
            // 400 BAD REQUEST status is not retryable
            logger.errorContext(logContext, msg, null, logMap);
            throw new NonRetryErrorException(msg);
        } else if (httpStatus.is4xxClientError() || httpStatus.is5xxServerError()) {
            // any other client or server status is retryable
            logger.errorContext(logContext, msg + ", retry", null, logMap);
            throw new RetryErrorException(msg);
        } else {
            logger.debugContext(logContext, msg, logMap);
        }
    }
}
