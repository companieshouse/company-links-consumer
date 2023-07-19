package uk.gov.companieshouse.company.links.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.company.links.processor.LinkRouter;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.company.links.type.ResourceChange;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

@ExtendWith(MockitoExtension.class)
class LinkRouterTest {
    private static final String REQUEST_ID = "request_id";

    @Mock
    private PatchLinkRequestExtractable extractor;

    @Mock
    private LinkClientFactory factory;

    @Mock
    private AddExemptionsClient addExemptionsClient;

    @Mock
    private DeleteExemptionsClient deleteExemptionsClient;

    @Mock
    private AddOfficersClient addOfficersClient;

    @Mock
    private RemoveOfficersLinkClient removeOfficersLinkClient;

    private LinkRouter router;

    @Mock
    private ResourceChange message;

    @Mock
    private ResourceChangedData data;

    @Mock
    private EventRecord event;

    @Mock
    private PatchLinkRequest linkRequest;

    @BeforeEach
    void setup() {
        router = new LinkRouter(extractor, factory);
        when(data.getContextId()).thenReturn(REQUEST_ID);
    }

    @Test
    @DisplayName("Route should successfully route add changed events to the add exemptions service")
    void routeChangedExemptions() {
        // given
        when(message.getData()).thenReturn(data);
        when(data.getEvent()).thenReturn(event);
        when(event.getType()).thenReturn("changed");
        when(data.getResourceUri()).thenReturn("company/12345678/exemptions");
        when(extractor.extractPatchLinkRequest(any(), eq(REQUEST_ID))).thenReturn(linkRequest);
        when(factory.getLinkClient(any(), any())).thenReturn(addExemptionsClient);

        // when
        router.route(message, "deltaType");

        // then
        verify(extractor).extractPatchLinkRequest("company/12345678/exemptions", REQUEST_ID);
        verify(addExemptionsClient).patchLink(linkRequest);
    }

    @Test
    @DisplayName("Route should successfully route delete events to the delete exemptions service")
    void routeDeletedExemptions() {
        // given
        when(message.getData()).thenReturn(data);
        when(data.getEvent()).thenReturn(event);
        when(event.getType()).thenReturn("deleted");
        when(data.getResourceUri()).thenReturn("company/12345678/exemptions");
        when(extractor.extractPatchLinkRequest(any(), eq(REQUEST_ID))).thenReturn(linkRequest);
        when(factory.getLinkClient(any(), any())).thenReturn(deleteExemptionsClient);

        // when
        router.route(message, "deltaType");

        // then
        verify(extractor).extractPatchLinkRequest("company/12345678/exemptions", REQUEST_ID);
        verify(deleteExemptionsClient).patchLink(linkRequest);
    }

    @Test
    @DisplayName("Route should successfully route add changed events to the add officers service")
    void routeChangedOfficers() {
        // given
        when(message.getData()).thenReturn(data);
        when(data.getEvent()).thenReturn(event);
        when(event.getType()).thenReturn("changed");
        when(data.getResourceUri()).thenReturn("company/12345678/officers");
        when(extractor.extractPatchLinkRequest(any(), eq(REQUEST_ID))).thenReturn(linkRequest);
        when(factory.getLinkClient(any(), any())).thenReturn(addOfficersClient);

        // when
        router.route(message, "deltaType");

        // then
        verify(extractor).extractPatchLinkRequest("company/12345678/officers", REQUEST_ID);
        verify(addOfficersClient).patchLink(linkRequest);
    }

    @Test
    @DisplayName("Route should successfully route remove events to the delete officers service")
    void routeRemovedOfficers() {
        // given
        when(message.getData()).thenReturn(data);
        when(data.getEvent()).thenReturn(event);
        when(event.getType()).thenReturn("deleted");
        when(data.getResourceUri()).thenReturn("company/12345678/officers");
        when(extractor.extractPatchLinkRequest(any(), eq(REQUEST_ID))).thenReturn(linkRequest);
        when(factory.getLinkClient(any(), any())).thenReturn(removeOfficersLinkClient);

        // when
        router.route(message, "deltaType");

        // then
        verify(extractor).extractPatchLinkRequest("company/12345678/officers", REQUEST_ID);
        verify(removeOfficersLinkClient).patchLink(linkRequest);
    }
}
