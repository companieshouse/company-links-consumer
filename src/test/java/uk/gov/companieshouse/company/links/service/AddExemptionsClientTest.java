package uk.gov.companieshouse.company.links.service;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import java.util.Collections;
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
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.company.PrivateCompanyLinksResourceHandler;
import uk.gov.companieshouse.api.handler.company.links.request.PrivateCompanyExemptionsLinksPatch;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.http.HttpClient;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.company.links.util.ResponseHandler;

@ExtendWith(MockitoExtension.class)
class AddExemptionsClientTest {

    private static final String COMPANY_NUMBER = "12345678";
    private static final String REQUEST_ID = "request_id";
    private static final String PATH = String.format("/company/%s/links/exemptions", COMPANY_NUMBER);
    private static final String LINK_TYPE = "exemptions";

    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Mock
    private InternalApiClient internalApiClient;

    @Mock
    private PrivateCompanyLinksResourceHandler resourceHandler;

    @Mock
    private PrivateCompanyExemptionsLinksPatch exemptionsLinksPatchHandler;

    @Mock
    private HttpClient httpClient;

    @Mock
    private ResponseHandler responseHandler;

    @InjectMocks
    private AddExemptionsClient client;

    private final PatchLinkRequest linkRequest = new PatchLinkRequest(COMPANY_NUMBER, REQUEST_ID);

    @BeforeEach
    void setup() {
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.getHttpClient()).thenReturn(httpClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.addExemptionsCompanyLink(anyString())).thenReturn(exemptionsLinksPatchHandler);
    }

    @Test
    void testUpsert() throws ApiErrorResponseException, URIValidationException {
        // given
        when(exemptionsLinksPatchHandler.execute()).thenReturn(new ApiResponse<>(200, Collections.emptyMap()));

        // when
        client.patchLink(linkRequest);

        // then
        verify(resourceHandler).addExemptionsCompanyLink(PATH);
        verify(exemptionsLinksPatchHandler).execute();
    }

    @ParameterizedTest(name = "Input [{0}] and [{1}] result in output [{2}]")
    @MethodSource("apiErrorsAndResponses")
    void testHandleApiErrorResponseExceptionsIfClientErrorsReturned(int inputOne, String inputTwo, int output) throws ApiErrorResponseException, URIValidationException {
        // given
        ApiErrorResponseException apiErrorResponseException = new ApiErrorResponseException(new HttpResponseException.Builder(inputOne, inputTwo, new HttpHeaders()));
        when(exemptionsLinksPatchHandler.execute()).thenThrow(apiErrorResponseException);

        // when
        client.patchLink(linkRequest);

        // then
        verify(resourceHandler).addExemptionsCompanyLink(PATH);
        verify(exemptionsLinksPatchHandler).execute();
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
        // given
        IllegalArgumentException illegalArgumentException = new IllegalArgumentException("Internal server error");
        when(exemptionsLinksPatchHandler.execute()).thenThrow(illegalArgumentException);

        // when
        client.patchLink(linkRequest);

        // then
        verify(resourceHandler).addExemptionsCompanyLink(PATH);
        verify(exemptionsLinksPatchHandler).execute();
        verify(responseHandler).handle(illegalArgumentException);
    }

    @Test
    void testThrowNonRetryableExceptionIfCompanyNumberInvalid() throws ApiErrorResponseException, URIValidationException {
        // given
        URIValidationException uriValidationException = new URIValidationException("Invalid URI");
        when(exemptionsLinksPatchHandler.execute()).thenThrow(uriValidationException);

        // when
        client.patchLink(new PatchLinkRequest("invalid/path", REQUEST_ID));

        // then
        verify(resourceHandler).addExemptionsCompanyLink("/company/invalid/path/links/exemptions");
        verify(exemptionsLinksPatchHandler).execute();
        verify(responseHandler).handle("invalid/path", uriValidationException);
    }
}
