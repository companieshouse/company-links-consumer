package uk.gov.companieshouse.company.links.processor;

import java.util.List;
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
            String requestTypeAndService,
            String companyNumber,
            final Map<String, Object> logMap)
            throws NonRetryableErrorException, RetryableErrorException {
        String message = "Response from " + requestTypeAndService;
        logMap.put("status", httpStatus.toString());
        logMap.put("company_number", companyNumber);

        if (HttpStatus.BAD_REQUEST == httpStatus) {
            // 400 BAD REQUEST status is not retryable
            logger.errorContext(logContext, message, null, logMap);
            throw new NonRetryableErrorException(message);
        } else if (!httpStatus.is2xxSuccessful()) {
            // any other client or server status is retryable
            logger.errorContext(logContext, message, null, logMap);
            throw new RetryableErrorException(message);
        } else {
            logger.info(String.format("Successfully invoked %s endpoint"
                            + " for message with contextId %s and company number %s",
                    requestTypeAndService, logContext, companyNumber));
        }
    }
}
