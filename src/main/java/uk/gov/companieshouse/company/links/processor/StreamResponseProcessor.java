package uk.gov.companieshouse.company.links.processor;

import java.util.Map;
import org.springframework.http.HttpStatus;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.ApiType;
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
            String requestType,
            ApiType apiType,
            String companyNumber,
            final Map<String, Object> logMap)
            throws NonRetryableErrorException, RetryableErrorException {
        var message = String.format("Response from %s %s", requestType, apiType.toString());
        logMap.put("status", httpStatus.toString());
        logMap.put("company_number", companyNumber);

        if (HttpStatus.BAD_REQUEST == httpStatus) {
            logger.errorContext(logContext, message, null, logMap);
            throw new NonRetryableErrorException(message);
        } else if (httpStatus.is2xxSuccessful()) {
            logger.info(String.format("Successfully invoked %s %s endpoint"
                            + " for message with contextId %s and company number %s",
                    requestType, apiType, logContext, companyNumber));
        } else {
            logger.errorContext(logContext, message, null, logMap);
            throw new RetryableErrorException(message);
        }
    }
}
