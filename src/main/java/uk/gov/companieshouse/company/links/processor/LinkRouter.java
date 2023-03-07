package uk.gov.companieshouse.company.links.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.service.PatchLinkRequestExtractable;
import uk.gov.companieshouse.company.links.service.LinkClientFactory;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.company.links.type.ResourceChange;

@Component
public class LinkRouter implements LinkRoutable {

    private final PatchLinkRequestExtractable extractor;
    private final LinkClientFactory factory;

    public LinkRouter(PatchLinkRequestExtractable extractor, LinkClientFactory factory) {
        this.extractor = extractor;
        this.factory = factory;
    }

    @Override
    public void route(ResourceChange message, String deltaType) {
        String eventType = message.getData().getEvent().getType();
        PatchLinkRequest request = extractor.extractPatchLinkRequest(
                message.getData().getResourceUri());
        factory.getLinkClient(deltaType, eventType).patchLink(request);
    }
}