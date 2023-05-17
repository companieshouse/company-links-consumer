package uk.gov.companieshouse.company.links.service;

import uk.gov.companieshouse.company.links.type.PatchLinkRequest;

public interface LinkClient {
    void patchLink(PatchLinkRequest linkRequest);
}
