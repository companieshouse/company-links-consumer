package uk.gov.companieshouse.company.links.service;

import java.util.Collections;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class LinkClientFactory {

    private final Map<String, Map<String, LinkClient>> linkClientMap;
    private final NullLinkClient nullLinkClient;

    public LinkClientFactory(Map<String, Map<String, LinkClient>> linkClientMap,
            NullLinkClient nullLinkClient) {
        this.linkClientMap = linkClientMap;
        this.nullLinkClient = nullLinkClient;
    }

    public LinkClient getLinkClient(String deltaType, String eventType) {
        return linkClientMap.getOrDefault(deltaType, Collections.emptyMap())
                .getOrDefault(eventType, nullLinkClient);
    }
}
