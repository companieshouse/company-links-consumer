package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

@Component
public class AddFilingHistoryClient implements LinkClient {

    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientFactory;

    public AddFilingHistoryClient(Logger logger,
            Supplier<InternalApiClient> internalApiClientFactory) {
        this.logger = logger;
        this.internalApiClientFactory = internalApiClientFactory;
    }

    /**
     * Sends a patch request to the add filing_history link endpoint in the company profile api and
     * handles any error responses.
     *
     * @param linkRequest PatchLinkRequest
     */
    @Override
    public void patchLink(PatchLinkRequest linkRequest) {
        InternalApiClient client = internalApiClientFactory.get();
        client.getHttpClient().setRequestId(linkRequest.getRequestId());
        try {
            client.privateCompanyLinksResourceHandler()
                    .addFilingHistoryLink(
                            String.format("/company/%s/links/filing-history",
                                    linkRequest.getCompanyNumber()))
                    .execute();
        } catch (ApiErrorResponseException ex) {
            int status = ex.getStatusCode();
            if (HttpStatus.valueOf(status).is5xxServerError()) {
                logger.error(String.format("Server error returned with status code: [%s] "
                                + "processing add filing history link request", status),
                        DataMapHolder.getLogMap());
                throw new RetryableErrorException("Server error returned when processing "
                        + "add filing_history link request", ex);
            } else if (status == 409) {
                logger.info("HTTP 409 Conflict returned; "
                                + "company profile already has a filing history link",
                        DataMapHolder.getLogMap());
            } else if (status == 404) {
                logger.info("HTTP 404 Not Found returned; company profile does not exist",
                        DataMapHolder.getLogMap());
                throw new RetryableErrorException(
                        String.format("Company profile [%s] does not exist"
                                        + " when processing add filing history link request",
                                linkRequest.getCompanyNumber()), ex);
            } else {
                logger.error(String.format("Add officers client error returned with status code: "
                                + "[%s] when processing add filing_history link request",
                        ex.getStatusCode()), DataMapHolder.getLogMap());
                throw new NonRetryableErrorException("UpsertClient error returned when "
                        + "processing add filing_history link request", ex);
            }
        } catch (IllegalArgumentException ex) {
            logger.error("Illegal argument exception caught when handling API response",
                    DataMapHolder.getLogMap());
            throw new RetryableErrorException("Server error returned when processing add "
                    + "filing_history link request", ex);
        } catch (URIValidationException ex) {
            logger.error("Invalid companyNumber specified when handling API request",
                    DataMapHolder.getLogMap());
            throw new NonRetryableErrorException("Invalid companyNumber specified", ex);
        }
    }
}
