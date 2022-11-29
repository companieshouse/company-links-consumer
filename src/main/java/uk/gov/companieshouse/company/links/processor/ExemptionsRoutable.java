package uk.gov.companieshouse.company.links.processor;

import uk.gov.companieshouse.company.links.type.ResourceChange;

public interface ExemptionsRoutable {
    void route(ResourceChange message);
}
