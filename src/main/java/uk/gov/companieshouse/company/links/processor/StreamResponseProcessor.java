package uk.gov.companieshouse.company.links.processor;

import java.util.Map;
import org.springframework.http.HttpStatus;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

public class StreamResponseProcessor {

    final Logger logger;

    public StreamResponseProcessor(Logger logger) {
        this.logger = logger;
    }


    /**
     * Common response handler.
     */
    void handleResponse(
            final HttpStatus httpStatus,
            final String logContext,
            String message,
            final Map<String, Object> logMap)
            throws NonRetryableErrorException, RetryableErrorException {

        logMap.put("status", httpStatus.toString());

        if (HttpStatus.BAD_REQUEST == httpStatus) {
            // 400 BAD REQUEST status is not retryable
            logger.errorContext(logContext, message, null, logMap);
            throw new NonRetryableErrorException(message);
        } else if (!httpStatus.is2xxSuccessful()) {
            // any other client or server status is retryable
            logger.errorContext(logContext, message, null, logMap);
            throw new RetryableErrorException(message);
        } else {
            logger.trace("Got successful response");
        }
    }
}
