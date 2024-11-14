package uk.gov.companieshouse.company.links.config;

import java.util.Map;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.gov.companieshouse.company.links.service.LinkClient;

@Configuration
public class LinkClientConfig {

    private static final String EXEMPTIONS = "exemptions";
    private static final String FILING_HISTORY = "filing-history";
    private static final String CHANGED = "changed";
    private static final String DELETED = "deleted";
    private static final String OFFICERS = "officers";
    private static final String STATEMENTS = "statements";
    private static final String PSCS = "pscs";

    @Bean
    Map<String, Map<String, LinkClient>> linkClientMap(
            @Qualifier("addExemptionsClient") LinkClient addExemptionsClient,
            @Qualifier("deleteExemptionsClient") LinkClient deleteExemptionsClient,
            @Qualifier("addOfficersClient") LinkClient addOfficersClient,
            @Qualifier("removeOfficersClient") LinkClient removeOfficersClient,
            @Qualifier("addStatementsClient") LinkClient addStatementsClient,
            @Qualifier("deleteStatementsClient") LinkClient deleteStatementsClient,
            @Qualifier("addPscClient") LinkClient addPscClient,
            @Qualifier("deletePscClient") LinkClient deletePscClient,
            @Qualifier("addFilingHistoryClient") LinkClient addFilingHistoryClient) {

        return Map.of(
                EXEMPTIONS, Map.of(
                        CHANGED, addExemptionsClient,
                        DELETED, deleteExemptionsClient),
                OFFICERS, Map.of(
                        CHANGED, addOfficersClient,
                        DELETED, removeOfficersClient),
                STATEMENTS, Map.of(
                    CHANGED, addStatementsClient,
                    DELETED, deleteStatementsClient),
                PSCS, Map.of(
                        CHANGED, addPscClient,
                        DELETED, deletePscClient),
                FILING_HISTORY, Map.of(
                        CHANGED, addFilingHistoryClient));
    }
}