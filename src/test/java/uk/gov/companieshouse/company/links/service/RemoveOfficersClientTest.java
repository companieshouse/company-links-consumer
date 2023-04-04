package uk.gov.companieshouse.company.links.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
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

@ExtendWith(MockitoExtension.class)
class RemoveOfficersClientTest {

    private static final String COMPANY_NUMBER = "12345678";
    private static final String RESOURCE_ID = "abcdefg";

    private final PatchLinkRequest linkRequest = new PatchLinkRequest(COMPANY_NUMBER, RESOURCE_ID);

    @Mock
    private AppointmentsListClient appointmentsListClient;

    @Mock
    private RemoveOfficersLinkClient removeOfficersLinkClient;

    @Mock
    private Logger logger;

    @InjectMocks
    private RemoveOfficersClient removeOfficersClient;

    @Test
    void shouldRemoveOfficersLinkWhenZeroAppointmentsFound() {
        when(appointmentsListClient.getAppointmentsList(COMPANY_NUMBER)).thenReturn(
                new OfficerList()
                        .totalResults(0));

        removeOfficersClient.patchLink(linkRequest);

        verify(removeOfficersLinkClient).patchLink(linkRequest);
    }

    @Test
    void shouldThrowRetryableErrorExceptionWhenOfficerStillPresentInOfficerList() {
        when(appointmentsListClient.getAppointmentsList(COMPANY_NUMBER)).thenReturn(
                new OfficerList()
                        .totalResults(1)
                        .items(List.of(new OfficerSummary()
                                .links(new ItemLinkTypes()
                                        .self(String.format("/company/%s/%s",
                                                COMPANY_NUMBER, RESOURCE_ID))))));

        Executable actual = () -> removeOfficersClient.patchLink(linkRequest);

        assertThrows(RetryableErrorException.class, actual);
        verifyNoInteractions(removeOfficersLinkClient);
    }

    @Test
    void shouldNotRemoveOfficerLinkWhenOfficerListContainsOtherOfficers() {
        when(appointmentsListClient.getAppointmentsList(COMPANY_NUMBER)).thenReturn(
                new OfficerList()
                        .totalResults(1)
                        .links(new LinkTypes().self(String.format("/company/%s/%s",
                                COMPANY_NUMBER, "some-other-id"))));

        removeOfficersClient.patchLink(linkRequest);

        verifyNoInteractions(removeOfficersLinkClient);
    }
}
