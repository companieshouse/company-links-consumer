package uk.gov.companieshouse.company.links.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.processor.ExemptionsRouter;
import uk.gov.companieshouse.company.links.type.ResourceChange;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

@ExtendWith(MockitoExtension.class)
class ExemptionsRouterTest {

    @Mock
    private AddExemptionsService addExemptionsService;

    @Mock
    private DeleteExemptionsService deleteExemptionsService;

    @InjectMocks
    private ExemptionsRouter router;

    @Mock
    private ResourceChange message;

    @Mock
    private ResourceChangedData data;

    @Mock
    private EventRecord event;

    @Test
    @DisplayName("Route should successfully route add changed events to the add exemptions service")
    void routeChanged() {
        // given
        when(message.getData()).thenReturn(data);
        when(data.getEvent()).thenReturn(event);
        when(event.getType()).thenReturn("changed");
        when(data.getResourceUri()).thenReturn("company/12345678/exemptions");

        // when
        router.route(message);

        // then
        verify(addExemptionsService).process("company/12345678/exemptions");
        verifyNoInteractions(deleteExemptionsService);
    }

    @Test
    @DisplayName("Route should successfully route delete events to the delete exemptions service")
    void routeDeleted() {
        // given
        when(message.getData()).thenReturn(data);
        when(data.getEvent()).thenReturn(event);
        when(event.getType()).thenReturn("deleted");
        when(data.getResourceUri()).thenReturn("company/12345678/exemptions");

        // when
        router.route(message);

        // then
        verify(deleteExemptionsService).process("company/12345678/exemptions");
        verifyNoInteractions(addExemptionsService);
    }

    @Test
    @DisplayName("Route should not route events to the add or deleted exemptions service if invalid event type")
    void routeError() {
        // given
        when(message.getData()).thenReturn(data);
        when(data.getEvent()).thenReturn(event);
        when(event.getType()).thenReturn("");

        // when
        Executable executable = () -> router.route(message);

        // then
        Exception exception = assertThrows(NonRetryableErrorException.class, executable);
        assertEquals("Invalid event type: ", exception.getMessage());
        verifyNoInteractions(addExemptionsService);
    }
}
