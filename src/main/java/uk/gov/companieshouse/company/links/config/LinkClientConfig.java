package uk.gov.companieshouse.company.links.config;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.gov.companieshouse.company.links.service.LinkClient;

@Configuration
public class LinkClientConfig {

    @Autowired
    private LinkClient addExemptionsClient;
    @Autowired
    private LinkClient deleteExemptionsClient;

    @Bean
    Map<String, Map<String, LinkClient>> linkClientMap() {
        Map<String, Map<String, LinkClient>> linkClientConfig = new HashMap<>();

        Map<String, LinkClient> exemptionsClientConfig = new HashMap<>();
        exemptionsClientConfig.put("changed", addExemptionsClient);
        exemptionsClientConfig.put("deleted", deleteExemptionsClient);
        linkClientConfig.put("exemptions", exemptionsClientConfig);

        return linkClientConfig;
    }
}
