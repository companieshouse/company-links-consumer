package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.exemptions.InternalExemptionsApi;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class AddExemptionsClient {
    private final Logger logger;
    private final Supplier<InternalApiClient> internalApiClientFactory;

    public AddExemptionsClient(Logger logger, Supplier<InternalApiClient> internalApiClientFactory) {
        this.logger = logger;
        this.internalApiClientFactory = internalApiClientFactory;
    }

    public void addExemptionsLink(String path) {
        InternalApiClient client = internalApiClientFactory.get();
        // TODO define new request model & new handler

        try {
            client.privateDeltaCompanyAppointmentResourceHandler()
                    .upsertCompanyExemptionsResource(path, new InternalExemptionsApi())
                    .execute();
        } catch (ApiErrorResponseException e) {
            if(e.getStatusCode() / 100 == 5) {
                logger.error(String.format("Server error returned with status code: [%s] when upserting delta", e.getStatusCode()));
                throw new RetryableErrorException("Server error returned when upserting delta", e);
            } else {
                logger.error(String.format("Upsert client error returned with status code: [%s] when upserting delta", e.getStatusCode()));
                throw new NonRetryableErrorException("UpsertClient error returned when upserting delta", e);
            }
        } catch (IllegalArgumentException e) {
            logger.error("Illegal argument exception caught when handling API response");
            throw new RetryableErrorException("Server error returned when upserting delta", e);
        } catch (URIValidationException e) {
            logger.error("Invalid path specified when handling API request");
            throw new NonRetryableErrorException("Invalid path specified", e);
        }
    }
}
