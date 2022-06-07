package uk.gov.companieshouse.company.links.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.Executor;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.ApiType;
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
     * @param requestType name of operation
     * @param uri           uri of sdk being called
     * @param executor      executor to use
     * @return the response object
     */
    public <T> ApiResponse<T> executeOp(final String logContext,
                                        final String requestType,
                                        final ApiType apiType,
                                        final String uri,
                                        final Executor<ApiResponse<T>> executor)
            throws RetryableErrorException {
        final Map<String, Object> logMap = new HashMap<>();
        logMap.put("operation_name", requestType);
        logMap.put("api_called", apiType);
        logMap.put("path", uri);

        try {
            return executor.execute();
        } catch (URIValidationException ex) {
            var errMsg = "URI Validation exception when executing SDK operation";
            logger.errorContext(logContext, errMsg, ex, logMap);
            throw new RetryableErrorException(errMsg, ex);
        } catch (ApiErrorResponseException ex) {
            logger.errorContext(logContext, "SDK exception - API Error Response", ex, logMap);
            if (ex.getStatusCode() != 0) {
                return new ApiResponse<>(ex.getStatusCode(), Collections.emptyMap());
            }

            throw new RetryableErrorException("SDK Exception ", ex);
        }
    }

    private boolean isOperationGet(String operationName, ApiType apiType) {
        return operationName.contains("GET")
                && (apiType == ApiType.COMPANY_PROFILE || apiType == ApiType.INSOLVENCY);
    }
}