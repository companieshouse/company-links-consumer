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
import uk.gov.companieshouse.api.handler.company.links.request.PrivatePscLinksAdd;
import uk.gov.companieshouse.api.handler.delta.PrivateDeltaResourceHandler;
import uk.gov.companieshouse.api.handler.delta.pscfullrecord.request.PscGetAll;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.ListSummary;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.company.links.util.ResponseHandler;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AddPscClientTest {

    private static final String COMPANY_NUMBER = "12345678";
    private static final String REQUEST_ID = "request_id";

    private final PatchLinkRequest linkRequest = new PatchLinkRequest(COMPANY_NUMBER, REQUEST_ID);
    private static final String PATH = String.format("/company/%s/links/persons-with-significant-control", COMPANY_NUMBER);
    private static final String LINK_TYPE = "PSC";

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

    @Mock
    private ResponseHandler responseHandler;

    @InjectMocks
    private AddPscClient client;

    @BeforeEach
    void setUp() throws ApiErrorResponseException, URIValidationException {
        List<ListSummary> pscList = new ArrayList<>();
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
        client.patchLink(linkRequest);

        //then
        verify(resourceHandler).addPscCompanyLink(PATH);
        verify(pscLinkAddHandler).execute();
    }

    @ParameterizedTest(name = "Input [{0}] and [{1}] result in output [{2}]")
    @MethodSource("apiErrorsAndResponses")
    void testHandleApiErrorResponseExceptionsIfClientErrorsReturned(int inputOne, String inputTwo, int output) throws ApiErrorResponseException, URIValidationException {
        // given
        ApiErrorResponseException apiErrorResponseException = new ApiErrorResponseException(new HttpResponseException.Builder(inputOne, inputTwo, new HttpHeaders()));
        when(pscLinkAddHandler.execute()).thenThrow(apiErrorResponseException);

        // when
        client.patchLink(linkRequest);

        // then
        verify(resourceHandler).addPscCompanyLink(PATH);
        verify(pscLinkAddHandler).execute();
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
        when(pscLinkAddHandler.execute()).thenThrow(illegalArgumentException);

        //when
        client.patchLink(linkRequest);

        //then
        verify(resourceHandler).addPscCompanyLink(PATH);
        verify(pscLinkAddHandler).execute();
        verify(responseHandler).handle(illegalArgumentException);
    }

    @Test
    void testThrowNonRetryableExceptionIfCompanyNumberInvalid() throws ApiErrorResponseException, URIValidationException {
        //given
        when(deltaResourceHandler.getPscs("invalid/path")).thenReturn(getAll);
        URIValidationException uriValidationException = new URIValidationException("Invalid/URI");
        when(pscLinkAddHandler.execute()).thenThrow(uriValidationException);

        //when
        client.patchLink(new PatchLinkRequest("invalid/path", "invalid-id"));

        //then
        verify(resourceHandler).addPscCompanyLink("/company/invalid/path/links/persons-with-significant-control");
        verify(pscLinkAddHandler).execute();
        verify(responseHandler).handle("invalid/path", uriValidationException);
    }
}
