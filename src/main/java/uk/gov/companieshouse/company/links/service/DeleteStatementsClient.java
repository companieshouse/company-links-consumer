package uk.gov.companieshouse.company.links.service;

import java.util.List;
import java.util.function.Supplier;

import org.springframework.stereotype.Component;

import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.Statement;
import uk.gov.companieshouse.api.psc.StatementList;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@Component
public class DeleteStatementsClient implements LinkClient {
    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientFactory;

    public DeleteStatementsClient(Logger logger, Supplier<InternalApiClient> internalApiClientFactory) {
        this.logger = logger;
        this.internalApiClientFactory = internalApiClientFactory;
    }

    @Override
    public void patchLink(String companyNumber) {
        InternalApiClient client = internalApiClientFactory.get();
        try {
            ApiResponse<StatementList> response = client.privateDeltaResourceHandler()
                                                        .getPscStatements(companyNumber)
                                                        .execute();
            List<Statement> statementsList = response.getData().getItems();

            if (statementsList.size() == 0 || response.getStatusCode() == 404) {
                client.privateCompanyLinksResourceHandler().deletePscStatementsCompanyLink(String
                            .format("/company/%s/links/persons-with-significant-control-statements/delete", 
                            companyNumber)).execute();
            }
        } catch (ApiErrorResponseException e) {
            if (e.getStatusCode() / 100 == 5) {
                logger.error(String.format("Server error returned with status code: [%s] "
                        + "processing remove statements link request", e.getStatusCode()));
                throw new RetryableErrorException("Server error returned when processing "
                        + "remove statements link request", e);
            } else if (e.getStatusCode() == 409) {
                logger.info("HTTP 409 Conflict returned; "
                        + "company profile does not have an statements link already");
            } else if (e.getStatusCode() == 404) {
                logger.info("HTTP 404 Not Found returned; "
                        + "company profile does not exist");
            } else {
                logger.error(String.format("remove statements client error returned with "
                          + "status code: [%s] when processing remove statements link request",
                        e.getStatusCode()));
                throw new NonRetryableErrorException("Client error returned when "
                        + "processing remove statements link request", e);
            }
        } catch (IllegalArgumentException e) {
            logger.error("Illegal argument exception caught when handling API response");
            throw new RetryableErrorException("Server error returned when processing remove "
                    + "statements link request", e);
        } catch (URIValidationException e) {
            logger.error("Invalid companyNumber specified when handling API request");
            throw new NonRetryableErrorException("Invalid companyNumber specified", e);
        }
    } 
}
