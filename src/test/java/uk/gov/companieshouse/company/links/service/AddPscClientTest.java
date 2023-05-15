package uk.gov.companieshouse.company.links.service;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.delta.Psc;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.company.PrivateCompanyLinksResourceHandler;
import uk.gov.companieshouse.api.handler.company.links.request.PrivatePscLinksAdd;
import uk.gov.companieshouse.api.handler.company.links.request.PrivatePscStatementsLinksAdd;
import uk.gov.companieshouse.api.handler.delta.PrivateDeltaResourceHandler;
import uk.gov.companieshouse.api.handler.delta.pscfullrecord.request.PscGetAll;
import uk.gov.companieshouse.api.handler.delta.pscstatements.request.PscStatementsGetAll;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.ListSummary;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.api.psc.Statement;
import uk.gov.companieshouse.api.psc.StatementList;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AddPscClientTest {

    private static final String COMPANY_NUMBER = "12345678";
    private static final String PATH = String.format("/company/%s/links/persons-with-significant-control", COMPANY_NUMBER);

    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Mock
    private InternalApiClient internalApiClient;

    @Mock
    private PrivateCompanyLinksResourceHandler resourceHandler;

    @Mock
    private PrivateDeltaResourceHandler deltaResourceHandler;

    @Mock
    private PrivatePscLinksAdd pscLinkAddHandler;

    @Mock
    private PscGetAll getAll;

    @Mock
    private ApiResponse<PscList> response;

    @Mock
    private PscList getData;

    private List<ListSummary> pscList;

    @Mock
    private Logger logger;

    @InjectMocks
    private AddPscClient client;

    @BeforeEach
    void setUp() throws ApiErrorResponseException, URIValidationException {
        pscList = new ArrayList<>();
        pscList.add(new ListSummary());

        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(internalApiClient.privateDeltaResourceHandler()).thenReturn(deltaResourceHandler);
        when(resourceHandler.addPscCompanyLink(anyString())).thenReturn(pscLinkAddHandler);
        when(deltaResourceHandler.getPscs(COMPANY_NUMBER)).thenReturn(getAll);
        when(getAll.execute()).thenReturn(response);
        when(response.getData()).thenReturn(getData);
        when(getData.getItems()).thenReturn(pscList);
    }

    @Test
    void testUpsert() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscLinkAddHandler.execute()).thenReturn(new ApiResponse<>(200, Collections.emptyMap()));

        //when
        client.patchLink(new PatchLinkRequest(COMPANY_NUMBER));

        //then
        verify(resourceHandler).addPscCompanyLink(PATH);
        verify(pscLinkAddHandler).execute();
    }

    @Test
    void testThrowNonRetryableExceptionIfClientErrorReturned() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscLinkAddHandler.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(404, "Not found", new HttpHeaders())));

        //when
        client.patchLink(new PatchLinkRequest(COMPANY_NUMBER));

        //then
        verify(resourceHandler).addPscCompanyLink(PATH);
        verify(pscLinkAddHandler).execute();
        verify(logger).info("HTTP 404 Not Found returned; company profile does not exist");
    }

    @Test
    void testThrowsNonRetryableExceptionIf409Returned() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscLinkAddHandler.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(409, "Conflict", new HttpHeaders())));

        //when
        client.patchLink(new PatchLinkRequest(COMPANY_NUMBER));

        //then
        verify(resourceHandler).addPscCompanyLink(PATH);
        verify(pscLinkAddHandler).execute();
        verify(logger).info("HTTP 409 Conflict returned; company profile already has a PSC link");
    }

    @Test
    void testThrowRetryableExceptionIfServerErrorReturned() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscLinkAddHandler.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(500, "Internal server error", new HttpHeaders())));

        //when
        Executable actual = () -> client.patchLink(new PatchLinkRequest(COMPANY_NUMBER));

        //then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).addPscCompanyLink(PATH);
        verify(pscLinkAddHandler).execute();
    }

    @Test
    void testThrowRetryableExceptionIfIllegalArgumentExceptionIsCaught() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscLinkAddHandler.execute()).thenThrow(new IllegalArgumentException("Internal server error"));

        //when
        Executable actual = () -> client.patchLink(new PatchLinkRequest(COMPANY_NUMBER));

        //then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).addPscCompanyLink(PATH);
        verify(pscLinkAddHandler).execute();
    }

    @Test
    void testThrowNonRetryableExceptionIfCompanyNumberInvalid() throws ApiErrorResponseException, URIValidationException {
        //given
        when(deltaResourceHandler.getPscs("invalid/path")).thenReturn(getAll);
        when(pscLinkAddHandler.execute()).thenThrow(new URIValidationException("Invalid/URI"));

        //when
        Executable actual = () -> client.patchLink(new PatchLinkRequest("invalid/path"));

        //then
        assertThrows(NonRetryableErrorException.class, actual);
        verify(resourceHandler).addPscCompanyLink("/company/invalid/path/links/persons-with-significant-control");
        verify(pscLinkAddHandler).execute();
    }
}
