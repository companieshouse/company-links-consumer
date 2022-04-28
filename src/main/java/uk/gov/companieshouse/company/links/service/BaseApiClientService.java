package uk.gov.companieshouse.company.links.service;

import java.util.HashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.Executor;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;


public abstract class BaseApiClientService {
    protected Logger logger;

    protected BaseApiClientService(final Logger logger) {
        this.logger = logger;
    }

    /**
     * General execution of an SDK endpoint.
     *
     * @param <T>           type of api response
     * @param logContext    context ID for logging
     * @param operationName name of operation
     * @param uri           uri of sdk being called
     * @param executor      executor to use
     * @return the response object
     */
    public <T> ApiResponse<T> executeOp(final String logContext,
                                        final String operationName,
                                        final String uri,
                                        final Executor<ApiResponse<T>> executor)
            throws RetryableErrorException {
        final Map<String, Object> logMap = new HashMap<>();
        logMap.put("operation_name", operationName);
        logMap.put("path", uri);

        try {
            return executor.execute();
        } catch (URIValidationException ex) {
            ex.printStackTrace();
            logger.errorContext(logContext, "SDK exception", ex, logMap);
            throw new RetryableErrorException("SDK Exception", ex);
        } catch (ApiErrorResponseException ex) {
            logMap.put("status", ex.getStatusCode());
            logger.errorContext(logContext, "SDK exception", ex, logMap);
            if (ex.getStatusCode() == HttpStatus.BAD_REQUEST.value()) {
                throw new NonRetryableErrorException("SDK Exception", ex);
            }
            throw new RetryableErrorException("SDK Exception", ex);
        }
    }
}