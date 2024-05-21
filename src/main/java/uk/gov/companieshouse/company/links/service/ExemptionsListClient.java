package uk.gov.companieshouse.company.links.service;

import static org.springframework.http.HttpHeaders.CONTENT_LENGTH;

import java.util.function.Supplier;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.exemptions.CompanyExemptions;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;

@Component
public class ExemptionsListClient {

    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientSupplier;

    public ExemptionsListClient(Logger logger,
                                Supplier<InternalApiClient> internalApiClientSupplier) {
        this.logger = logger;
        this.internalApiClientSupplier = internalApiClientSupplier;
    }

    /**
     * Retrieve company exemptions given a company number from company-exemptions-data-api.
     *
     * @param companyNumber the company's company number
     * @return an ApiResponse containing the exemptions data model
     */
    public CompanyExemptions getExemptionsList(String contextId, String companyNumber) {
        InternalApiClient internalApiClient = internalApiClientSupplier.get();
        internalApiClient.getHttpClient().setRequestId(contextId);

        String uri = String.format("/company/%s/exemptions", companyNumber);

        try {
            return internalApiClient.privateDeltaResourceHandler()
                    .getCompanyExemptionsResource(uri)
                    .execute()
                    .getData();
        } catch (ApiErrorResponseException exception) {
            if (HttpStatus.valueOf(exception.getStatusCode()).is5xxServerError()) {
                String message = String.format("Server error status code: [%s] "
                        + "while fetching exemptions for company %s",
                        exception.getStatusCode(), companyNumber);
                logger.error(message, DataMapHolder.getLogMap());
                throw new RetryableErrorException(message, exception);
            } else if (exception.getStatusCode() == 404) {
                if ((exception.getHeaders().containsKey(CONTENT_LENGTH)
                        && exception.getHeaders().getContentLength() > 0)
                        || (exception.getContent() != null
                        && !exception.getContent().isEmpty())) {
                    logger.error("company-exemptions-data-api service is not available",
                            DataMapHolder.getLogMap());
                    throw new RetryableErrorException(
                            "company-exemptions-data-api service is not available", exception);
                }

                logger.debug(String.format("HTTP 404 Not Found returned for company number %s",
                        companyNumber), DataMapHolder.getLogMap());
                return new CompanyExemptions();
            } else {
                String message = String.format("Client error status code: [%s] "
                        + "while fetching exemptions list", exception.getStatusCode());
                logger.error(message, DataMapHolder.getLogMap());
                throw new NonRetryableErrorException(message, exception);
            }
        } catch (IllegalArgumentException exception) {
            logger.error("Illegal argument exception caught when handling API response",
                    DataMapHolder.getLogMap());
            throw new RetryableErrorException("Error returned when fetching statements", exception);
        } catch (URIValidationException exception) {
            String message = String.format("Invalid companyNumber [%s] when handling API request",
                    companyNumber);
            logger.error(message, DataMapHolder.getLogMap());
            throw new NonRetryableErrorException(message, exception);
        }
    }
}
