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

import uk.gov.companieshouse.api.psc.Statement;
import uk.gov.companieshouse.api.psc.StatementLinksType;
import uk.gov.companieshouse.api.psc.StatementList;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

@ExtendWith(MockitoExtension.class)
class DeleteStatementsClientTest {

    private static final String COMPANY_NUMBER = "12345678";
    private static final String REQUEST_ID = "request_id";
    private static final String RESOURCE_ID = "abcdefg";

    private final PatchLinkRequest linkRequest = new PatchLinkRequest(COMPANY_NUMBER, RESOURCE_ID,
            REQUEST_ID);

    @Mock
    private StatementsListClient statementsListClient;

    @Mock
    private DeleteStatementsLinkClient deleteStatementsLinkClient;

    @Mock
    private Logger logger;

    @InjectMocks
    private DeleteStatementsClient deleteStatementsClient;

    @Test
    void shouldRemoveStatementsLinkWhenZeroAppointmentsFound() {
        when(statementsListClient.getStatementsList(COMPANY_NUMBER, linkRequest.getRequestId())).thenReturn(
                new StatementList()
                        .totalResults(0));

        deleteStatementsClient.patchLink(linkRequest);

        verify(deleteStatementsLinkClient).patchLink(linkRequest);
    }

    @Test
    void shouldThrowRetryableErrorExceptionWhenStatementStillPresentInStatementList() {
        when(statementsListClient.getStatementsList(COMPANY_NUMBER, linkRequest.getRequestId())).thenReturn(
                new StatementList()
                        .totalResults(1)
                        .items(List.of(new Statement()
                        .links(new StatementLinksType()
                        .self(String.format("/company/%s/persons-with-significant-control/%s",
                                COMPANY_NUMBER, RESOURCE_ID))))));

        Executable actual = () -> deleteStatementsClient.patchLink(linkRequest);

        assertThrows(RetryableErrorException.class, actual);
        verifyNoInteractions(deleteStatementsLinkClient);
    }

    @Test
    void shouldNotRemoveStatementLinkWhenStatementListContainsOtherStatements() {
        when(statementsListClient.getStatementsList(COMPANY_NUMBER, linkRequest.getRequestId())).thenReturn(
                new StatementList()
                        .totalResults(1)
                        .items(List.of(new Statement()
                        .links(new StatementLinksType()
                        .self(String.format("/company/%s/persons-with-significant-control/%s",
                                COMPANY_NUMBER, "randomId2"))))));

        deleteStatementsClient.patchLink(linkRequest);

        verifyNoInteractions(deleteStatementsLinkClient);
    }
}
