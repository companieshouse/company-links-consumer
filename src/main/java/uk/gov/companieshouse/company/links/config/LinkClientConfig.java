package uk.gov.companieshouse.company.links.config;

import avro.shaded.com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.gov.companieshouse.company.links.service.LinkClient;

@Configuration
public class LinkClientConfig {

    @Bean
    Map<String, Map<String, LinkClient>> linkClientMap(LinkClient addExemptionsClient,
            LinkClient deleteExemptionsClient,
            LinkClient addOfficersClient,
            LinkClient removeOfficersClient) {
        return ImmutableMap.of(
                "exemptions", ImmutableMap.of(
                        "changed", addExemptionsClient,
                        "deleted", deleteExemptionsClient),
                "officers", ImmutableMap.of(
                        "changed", addOfficersClient,
                        "deleted", removeOfficersClient));
    }
}
