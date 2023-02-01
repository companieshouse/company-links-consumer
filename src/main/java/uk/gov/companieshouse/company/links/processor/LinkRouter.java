package uk.gov.companieshouse.company.links.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.service.CompanyNumberExtractable;
import uk.gov.companieshouse.company.links.service.LinkClientFactory;
import uk.gov.companieshouse.company.links.type.ResourceChange;

@Component
public class LinkRouter implements LinkRoutable {

    private final CompanyNumberExtractable extractor;
    private final LinkClientFactory factory;

    public LinkRouter(CompanyNumberExtractable extractor, LinkClientFactory factory) {
        this.extractor = extractor;
        this.factory = factory;
    }

    @Override
    public void route(ResourceChange message, String deltaType) {
        String eventType = message.getData().getEvent().getType();
        String companyNumber = extractor.extractCompanyNumber(message.getData().getResourceUri());
        factory.getLinkClient(deltaType, eventType).patchLink(companyNumber);
    }
}