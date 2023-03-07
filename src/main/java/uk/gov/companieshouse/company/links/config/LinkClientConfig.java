package uk.gov.companieshouse.company.links.config;

import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.gov.companieshouse.company.links.service.LinkClient;

@Configuration
public class LinkClientConfig {

    private static final String EXEMPTIONS = "exemptions";
    private static final String CHANGED = "changed";
    private static final String DELETED = "deleted";
    private static final String OFFICERS = "officers";

    @Bean
    Map<String, Map<String, LinkClient>> linkClientMap(
            LinkClient addExemptionsClient,
            LinkClient deleteExemptionsClient,
            LinkClient addOfficersClient,
            LinkClient removeOfficersClient) {

        return Map.of(
                EXEMPTIONS, Map.of(
                        CHANGED, addExemptionsClient,
                        DELETED, deleteExemptionsClient),
                OFFICERS, Map.of(
                        CHANGED, addOfficersClient,
                        DELETED, removeOfficersClient));

    }
}
