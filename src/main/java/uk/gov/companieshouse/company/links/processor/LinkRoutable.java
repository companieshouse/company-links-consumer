package uk.gov.companieshouse.company.links.processor;

import uk.gov.companieshouse.company.links.type.ResourceChange;

public interface LinkRoutable {
    void route(ResourceChange message, String deltaType);
}
