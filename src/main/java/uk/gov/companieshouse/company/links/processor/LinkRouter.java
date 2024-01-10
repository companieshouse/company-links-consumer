package uk.gov.companieshouse.company.links.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.service.LinkClientFactory;
import uk.gov.companieshouse.company.links.service.PatchLinkRequestExtractable;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.company.links.type.ResourceChange;
import uk.gov.companieshouse.stream.ResourceChangedData;

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
        ResourceChangedData data = message.getData();
        String eventType = data.getEvent().getType();
        PatchLinkRequest request = extractor.extractPatchLinkRequest(data.getResourceUri(),
                data.getContextId());
        DataMapHolder.get()
                .companyNumber(request.getCompanyNumber());
        factory.getLinkClient(deltaType, eventType).patchLink(request);
    }
}