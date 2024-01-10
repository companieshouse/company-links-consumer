package uk.gov.companieshouse.company.links.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;

import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.company.PrivateCompanyLinksResourceHandler;
import uk.gov.companieshouse.api.handler.company.links.request.PrivatePscStatementsLinksAdd;
import uk.gov.companieshouse.api.handler.delta.PrivateDeltaResourceHandler;
import uk.gov.companieshouse.api.handler.delta.pscstatements.request.PscStatementsGetAll;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.http.HttpClient;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.Statement;
import uk.gov.companieshouse.api.psc.StatementList;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AddStatementsClientTest {
    private static final String COMPANY_NUMBER = "12345678";
    private static final String REQUEST_ID = "request_id";
    private static final String PATH = String.format("/company/%s/links/persons-with-significant-control-statements", COMPANY_NUMBER);

    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Mock
    private InternalApiClient internalApiClient;

    @Mock
    private PrivateCompanyLinksResourceHandler resourceHandler;

    @Mock
    private PrivateDeltaResourceHandler deltaResourceHandler;

    @Mock
    private PrivatePscStatementsLinksAdd pscStatementsLinkAddHandler;

    @Mock 
    private PscStatementsGetAll getAll;

    @Mock 
    private ApiResponse<StatementList> response;

    @Mock
    private StatementList getData;

    private List<Statement> statementsList;

    @Mock
    private HttpClient httpClient;

    @Mock
    private Logger logger;

    @InjectMocks
    private AddStatementsClient client;

    @BeforeEach
    void setUp() throws ApiErrorResponseException, URIValidationException {
        statementsList = new ArrayList<>();
        statementsList.add(new Statement());

        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.getHttpClient()).thenReturn(httpClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(internalApiClient.privateDeltaResourceHandler()).thenReturn(deltaResourceHandler);
        when(resourceHandler.addPscStatementsCompanyLink(anyString())).thenReturn(pscStatementsLinkAddHandler);
        when(deltaResourceHandler.getPscStatements(COMPANY_NUMBER)).thenReturn(getAll);
        when(getAll.execute()).thenReturn(response);
        when(response.getData()).thenReturn(getData);
        when(getData.getItems()).thenReturn(statementsList);
    }

    @Test
    void testUpsert() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscStatementsLinkAddHandler.execute()).thenReturn(new ApiResponse<>(200, Collections.emptyMap()));
        
        //when
        client.patchLink(new PatchLinkRequest(COMPANY_NUMBER, REQUEST_ID));

        //then
        verify(resourceHandler).addPscStatementsCompanyLink(PATH);
        verify(pscStatementsLinkAddHandler).execute();
    }

    @Test
    void testThrowNonRetryableExceptionIfClientErrorReturned() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscStatementsLinkAddHandler.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(404, "Not found", new HttpHeaders())));
          
        //when
        client.patchLink(new PatchLinkRequest(COMPANY_NUMBER, REQUEST_ID));
 
        //then
        verify(resourceHandler).addPscStatementsCompanyLink(PATH);
        verify(pscStatementsLinkAddHandler).execute();
        verify(logger).info(eq("HTTP 404 Not Found returned; company profile does not exist"), any());
    }

    @Test
    void testThrowsNonRetryableExceptionIf409Returned() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscStatementsLinkAddHandler.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(409, "Conflict", new HttpHeaders())));
          
        //when
        client.patchLink(new PatchLinkRequest(COMPANY_NUMBER, REQUEST_ID));
 
        //then
        verify(resourceHandler).addPscStatementsCompanyLink(PATH);
        verify(pscStatementsLinkAddHandler).execute();
        verify(logger).info(eq("HTTP 409 Conflict returned; company profile already has a PSC statements link"), any());
    }

    @Test
    void testThrowRetryableExceptionIfServerErrorReturned() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscStatementsLinkAddHandler.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(500, "Internal server error", new HttpHeaders())));
          
        //when
        Executable actual = () -> client.patchLink(new PatchLinkRequest(COMPANY_NUMBER, REQUEST_ID));
 
        //then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).addPscStatementsCompanyLink(PATH);
        verify(pscStatementsLinkAddHandler).execute();
    }

    @Test
    void testThrowRetryableExceptionIfIllegalArgumentExceptionIsCaught() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscStatementsLinkAddHandler.execute()).thenThrow(new IllegalArgumentException("Internal server error"));
          
        //when
        Executable actual = () -> client.patchLink(new PatchLinkRequest(COMPANY_NUMBER, REQUEST_ID));
 
        //then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).addPscStatementsCompanyLink(PATH);
        verify(pscStatementsLinkAddHandler).execute();        
    }
    
    @Test
    void testThrowNonRetryableExceptionIfCompanyNumberInvalid() throws ApiErrorResponseException, URIValidationException {
    //given
    when(deltaResourceHandler.getPscStatements("invalid/path")).thenReturn(getAll);
    when(pscStatementsLinkAddHandler.execute()).thenThrow(new URIValidationException("Invalid/URI"));
      
    //when
    Executable actual = () -> client.patchLink(new PatchLinkRequest("invalid/path", REQUEST_ID));

    //then
    assertThrows(NonRetryableErrorException.class, actual);
    verify(resourceHandler).addPscStatementsCompanyLink("/company/invalid/path/links/persons-with-significant-control-statements");
    verify(pscStatementsLinkAddHandler).execute();          
    }
}
