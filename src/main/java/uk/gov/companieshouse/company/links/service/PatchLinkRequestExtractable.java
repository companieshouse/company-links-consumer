package uk.gov.companieshouse.company.links.service;

import uk.gov.companieshouse.company.links.type.PatchLinkRequest;

public interface PatchLinkRequestExtractable {

    PatchLinkRequest extractPatchLinkRequest(String uri);
}
