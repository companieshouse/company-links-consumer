package uk.gov.companieshouse.company.links.processor;

import java.util.Map;
import org.springframework.http.HttpStatus;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;


public final class ResponseHandler {

    private ResponseHandler() {
    }

    /**
     * Common response handler.
     */
    public static void handleResponse(
            final HttpStatus httpStatus,
            final String logContext,
            final String msg,
            final Map<String, Object> logMap,
            final Logger logger)
            throws NonRetryableErrorException, RetryableErrorException {
        logMap.put("status", httpStatus.toString());
        if (HttpStatus.BAD_REQUEST == httpStatus) {
            // 400 BAD REQUEST status is not retryable
            throw new NonRetryableErrorException(String
                    .format("Bad request %s", msg));
        } else if (!httpStatus.is2xxSuccessful()) {
            // any other client or server status is retryable
            logger.errorContext(logContext, msg + ", retry", null, logMap);
            throw new RetryableErrorException(String
                    .format("Unsuccessful %s", msg));
        } else {
            logger.trace(String.format("Successful %s", msg));
        }
    }
}
