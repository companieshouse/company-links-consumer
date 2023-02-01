package uk.gov.companieshouse.company.links.service;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@Component
public class NullLinkClient implements LinkClient {

    private final Logger logger;

    public NullLinkClient(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void patchLink(String companyNumber) {
        logger.error(String.format(
                "Invalid delta type and/or event type for company number %s", companyNumber));
        throw new NonRetryableErrorException("Invalid delta type and/or event type");
    }
}
