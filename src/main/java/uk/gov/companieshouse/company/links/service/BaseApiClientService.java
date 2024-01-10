package uk.gov.companieshouse.company.links.service;

import java.util.Collections;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.Executor;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;


public abstract class BaseApiClientService {

    protected Logger logger;

    protected BaseApiClientService(final Logger logger) {
        this.logger = logger;
    }

    /**
     * General execution of an SDK endpoint.
     *
     * @param <T>        type of api response
     * @param logContext context ID for logging
     * @param executor   executor to use
     * @return the response object
     */
    public <T> ApiResponse<T> executeOp(final String logContext,
            final Executor<ApiResponse<T>> executor)
            throws RetryableErrorException {
        try {
            return executor.execute();
        } catch (URIValidationException ex) {
            var errMsg = "URI Validation exception when executing SDK operation";
            logger.errorContext(logContext, errMsg, ex, DataMapHolder.getLogMap());
            throw new RetryableErrorException(errMsg, ex);
        } catch (ApiErrorResponseException ex) {
            logger.errorContext(logContext, "SDK exception - API Error Response", ex,
                    DataMapHolder.getLogMap());
            if (ex.getStatusCode() != 0) {
                return new ApiResponse<>(ex.getStatusCode(), Collections.emptyMap());
            }

            throw new RetryableErrorException("SDK Exception ", ex);
        }
    }
}