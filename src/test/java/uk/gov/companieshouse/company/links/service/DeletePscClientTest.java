package uk.gov.companieshouse.company.links.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.psc.ListSummary;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeletePscClientTest {
    private static final String COMPANY_NUMBER = "12345678";
    private static final String RESOURCE_ID = "abcdefg";
    private static final String REQUEST_ID = "reqId";

    private final PatchLinkRequest linkRequest = new PatchLinkRequest(COMPANY_NUMBER, RESOURCE_ID, REQUEST_ID);

    @Mock
    private PscListClient pscListClient;

    @Mock
    private DeletePscLinkClient deletePscLinkClient;

    @InjectMocks
    private DeletePscClient deletePscClient;

    @Test
    void shouldRemovePscLinkWhenZeroPscsFound() {
        when(pscListClient.getPscs(linkRequest)).thenReturn(
                new PscList()
                        .totalResults(0));

        deletePscClient.patchLink(linkRequest);

        verify(deletePscLinkClient).patchLink(linkRequest);
    }

    @Test
    void shouldNotRemovePscLinkWhenPscListContainsOtherPscs() {
        when(pscListClient.getPscs(linkRequest)).thenReturn(
                new PscList()
                        .totalResults(1).items(List.of(new ListSummary()
                                .links(String.format("/company/%s/persons-with-significant-control/%s",
                                        COMPANY_NUMBER, RESOURCE_ID)))));

        assertThrows(RetryableErrorException.class,
                () -> deletePscClient.patchLink(linkRequest));
        verifyNoInteractions(deletePscLinkClient);
    }
}
