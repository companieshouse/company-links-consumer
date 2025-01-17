package uk.gov.companieshouse.company.links.service;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class LinkClientFactoryTest {

    @Autowired
    private LinkClientFactory factory;

    @Test
    @DisplayName("Link factory correctly returns an add exemptions client")
    void getAddExemptionsClient() {
        // given
        // when
        LinkClient linkClient = factory.getLinkClient("exemptions", "changed");

        // then
        assertTrue(linkClient instanceof AddExemptionsClient);
    }

    @Test
    @DisplayName("Link factory correctly returns a delete exemptions client")
    void getDeleteExemptionsClient() {
        // given
        // when
        LinkClient linkClient = factory.getLinkClient("exemptions", "deleted");

        // then
        assertTrue(linkClient instanceof DeleteExemptionsClient);
    }

    @Test
    @DisplayName("Link factory correctly returns an add officers client")
    void getAddOfficersClient() {
        // given
        // when
        LinkClient linkClient = factory.getLinkClient("officers", "changed");

        // then
        assertTrue(linkClient instanceof AddOfficersClient);
    }

    @Test
    @DisplayName("Link factory correctly returns a remove officers client")
    void getRemoveOfficersClient() {
        // given
        // when
        LinkClient linkClient = factory.getLinkClient("officers", "deleted");

        // then
        assertTrue(linkClient instanceof RemoveOfficersClient);
    }

    @Test
    @DisplayName("Link factory correctly returns a remove psc client")
    void getRemovePscsClient() {
        // given
        // when
        LinkClient linkClient = factory.getLinkClient("pscs", "deleted");

        // then
        assertTrue(linkClient instanceof DeletePscClient);
    }

    @Test
    @DisplayName("Link factory correctly returns a add psc client")
    void getAddPscsClient() {
        // given
        // when
        LinkClient linkClient = factory.getLinkClient("pscs", "changed");

        // then
        assertTrue(linkClient instanceof AddPscClient);
    }

    @Test
    @DisplayName("Link factory correctly returns a null link client when deltaType mismatch")
    void getDefaultLinkClientDeltaType() {
        // given
        // when
        LinkClient linkClient = factory.getLinkClient("deltaType", "changed");

        // then
        assertTrue(linkClient instanceof NullLinkClient);
    }

    @Test
    @DisplayName("Link factory correctly returns a null link client when eventType mismatch")
    void getDefaultLinkClientEventType() {
        // given
        // when
        LinkClient linkClient = factory.getLinkClient("exemptions", "eventType");

        // then
        assertTrue(linkClient instanceof NullLinkClient);
    }

    @Test
    @DisplayName("Link factory correctly returns an add filing history client")
    void getAddFilingHistoryClient() {
        // given
        // when
        LinkClient linkClient = factory.getLinkClient("filing-history", "changed");

        // then
        assertInstanceOf(AddFilingHistoryClient.class, linkClient);
    }

}