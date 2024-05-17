package uk.gov.companieshouse.company.links.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.appointment.ItemLinkTypes;
import uk.gov.companieshouse.api.appointment.LinkTypes;
import uk.gov.companieshouse.api.appointment.OfficerList;
import uk.gov.companieshouse.api.appointment.OfficerSummary;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeleteOfficersClientTest {

    private static final String COMPANY_NUMBER = "12345678";
    private static final String REQUEST_ID = "request_id";
    private static final String RESOURCE_ID = "abcdefg";

    private final PatchLinkRequest linkRequest = new PatchLinkRequest(COMPANY_NUMBER, RESOURCE_ID,
            REQUEST_ID);

    @Mock
    private OfficerListClient officerListClient;

    @Mock
    private DeleteOfficersLinkClient deleteOfficersLinkClient;

    @Mock
    private Logger logger;

    @InjectMocks
    private DeleteOfficersClient deleteOfficersClient;

    @Test
    void shouldRemoveOfficersLinkWhenZeroAppointmentsFound() {
        when(officerListClient.getOfficers(linkRequest)).thenReturn(
                new OfficerList()
                        .totalResults(0));

        deleteOfficersClient.patchLink(linkRequest);

        verify(deleteOfficersLinkClient).patchLink(linkRequest);
    }

    @Test
    void shouldThrowRetryableErrorExceptionWhenOfficerStillPresentInOfficerList() {
        when(officerListClient.getOfficers(linkRequest)).thenReturn(
                new OfficerList()
                        .totalResults(1)
                        .items(List.of(new OfficerSummary()
                                .links(new ItemLinkTypes()
                                        .self(String.format("/company/%s/%s",
                                                COMPANY_NUMBER, RESOURCE_ID))))));

        Executable actual = () -> deleteOfficersClient.patchLink(linkRequest);

        assertThrows(RetryableErrorException.class, actual);
        verifyNoInteractions(deleteOfficersLinkClient);
    }

    @Test
    void shouldNotRemoveOfficerLinkWhenOfficerListContainsOtherOfficers() {
        when(officerListClient.getOfficers(linkRequest)).thenReturn(
                new OfficerList()
                        .totalResults(1)
                        .links(new LinkTypes().self(String.format("/company/%s/%s",
                                COMPANY_NUMBER, "some-other-id"))));

        deleteOfficersClient.patchLink(linkRequest);

        verifyNoInteractions(deleteOfficersLinkClient);
    }
}
