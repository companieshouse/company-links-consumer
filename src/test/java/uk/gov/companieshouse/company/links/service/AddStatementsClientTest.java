package uk.gov.companieshouse.company.links.service;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
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
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.company.links.util.ResponseHandler;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AddStatementsClientTest {
    private static final String COMPANY_NUMBER = "12345678";
    private static final String REQUEST_ID = "request_id";
    private static final String PATH = String.format("/company/%s/links/persons-with-significant-control-statements", COMPANY_NUMBER);
    private static final String LINK_TYPE = "PSC statements";
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

    @Mock
    private HttpClient httpClient;

    @Mock
    private ResponseHandler responseHandler;

    @InjectMocks
    private AddStatementsClient client;

    @BeforeEach
    void setUp() throws ApiErrorResponseException, URIValidationException {
        List<Statement> statementsList = new ArrayList<>();
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

    @ParameterizedTest(name = "Input [{0}] and [{1}] result in output [{2}]")
    @MethodSource("apiErrorsAndResponses")
    void testHandleApiErrorResponseExceptionsIfClientErrorsReturned(int inputOne, String inputTwo, int output) throws ApiErrorResponseException, URIValidationException {
        // given
        ApiErrorResponseException apiErrorResponseException = new ApiErrorResponseException(new HttpResponseException.Builder(inputOne, inputTwo, new HttpHeaders()));
        when(pscStatementsLinkAddHandler.execute()).thenThrow(apiErrorResponseException);

        // when
        client.patchLink(new PatchLinkRequest(COMPANY_NUMBER, REQUEST_ID));

        // then
        verify(resourceHandler).addPscStatementsCompanyLink(PATH);
        verify(pscStatementsLinkAddHandler).execute();
        verify(responseHandler).handle(output, LINK_TYPE, apiErrorResponseException);
    }

    private static Stream<Arguments> apiErrorsAndResponses() {
        return Stream.of(
                Arguments.of(404, "Not Found", 404),
                Arguments.of(409, "Conflict", 409),
                Arguments.of(500, "Internal server error", 500)
        );
    }

    @Test
    void testThrowRetryableExceptionIfIllegalArgumentExceptionIsCaught() throws ApiErrorResponseException, URIValidationException {
        //given
        IllegalArgumentException illegalArgumentException = new IllegalArgumentException("Internal server error");
        when(pscStatementsLinkAddHandler.execute()).thenThrow(illegalArgumentException);
          
        //when
        client.patchLink(new PatchLinkRequest(COMPANY_NUMBER, REQUEST_ID));
 
        //then
        verify(resourceHandler).addPscStatementsCompanyLink(PATH);
        verify(pscStatementsLinkAddHandler).execute();        
    }
    
    @Test
    void testThrowNonRetryableExceptionIfCompanyNumberInvalid() throws ApiErrorResponseException, URIValidationException {
    //given
    when(deltaResourceHandler.getPscStatements("invalid/path")).thenReturn(getAll);
    when(pscStatementsLinkAddHandler.execute()).thenThrow(new URIValidationException("Invalid/URI"));
      
    //when
    client.patchLink(new PatchLinkRequest("invalid/path", REQUEST_ID));

    //then
    verify(resourceHandler).addPscStatementsCompanyLink("/company/invalid/path/links/persons-with-significant-control-statements");
    verify(pscStatementsLinkAddHandler).execute();          
    }
}
