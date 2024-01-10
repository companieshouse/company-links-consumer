package uk.gov.companieshouse.company.links.service;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

@Component
public class NullLinkClient implements LinkClient {

    private final Logger logger;

    public NullLinkClient(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void patchLink(PatchLinkRequest linkRequest) {
        logger.error(String.format(
                "Invalid delta type and/or event type for company number %s",
                linkRequest.getCompanyNumber()), DataMapHolder.getLogMap());
        throw new NonRetryableErrorException("Invalid delta type and/or event type");
    }
}
