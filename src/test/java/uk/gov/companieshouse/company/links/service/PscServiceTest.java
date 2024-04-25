package uk.gov.companieshouse.company.links.service;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.delta.PrivateDeltaResourceHandler;
import uk.gov.companieshouse.api.handler.delta.pscfullrecord.request.PscGetAll;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.http.HttpClient;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

import java.util.Collections;
import java.util.function.Supplier;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PscServiceTest {
    private static final String MOCK_CONTEXT_ID = "context_id";
    private static final String MOCK_COMPANY_NUMBER = "6146287";
    private static final String MOCK_COMPANY_LINKS_URI = String.format("/company/%s/persons-with-significant-control",
            MOCK_COMPANY_NUMBER);

    private PscService pscService;
    @Mock
    private PscList pscList;
    @Mock
    private Logger logger;
    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;
    @Mock
    private InternalApiClient internalApiClient;
    @Mock
    HttpClient httpClient;
    @Mock
    private PrivateDeltaResourceHandler privateDeltaResourceHandler;
    @Mock
    private PscGetAll pscGetAll;

    @BeforeEach
    void setup() {
        pscService = spy(new PscService(logger, internalApiClientSupplier));
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.getHttpClient()).thenReturn(httpClient);
        when(internalApiClient.privatePscFullRecordResourceHandler()).thenReturn(privateDeltaResourceHandler);
    }

    @Test
    @DisplayName("Successfully retrieve a list of PSCs")
    void getPscList() throws ApiErrorResponseException, URIValidationException {
        final ApiResponse<PscList> expected = new ApiResponse<>(
                HttpStatus.OK.value(), Collections.emptyMap(), pscList);

        when(privateDeltaResourceHandler.getPscs(MOCK_COMPANY_LINKS_URI)).thenReturn(pscGetAll);
        when(pscGetAll.execute()).thenReturn(expected);

        final ApiResponse<PscList> response = pscService.getPscList(MOCK_CONTEXT_ID, MOCK_COMPANY_NUMBER);
        assertThat(response).isSameAs(expected);
    }

    @Test
    @DisplayName("Given a bad URI when retrieving PSCs List, return 404 not found")
    void getPscListBadUri() throws ApiErrorResponseException, URIValidationException {
        when(privateDeltaResourceHandler.getPscs(MOCK_COMPANY_LINKS_URI)).thenReturn(pscGetAll);
        when(pscGetAll.execute()).thenThrow(new URIValidationException("expected"));

        assertThrows(RetryableErrorException.class,
                () -> pscService.getPscList(MOCK_CONTEXT_ID, MOCK_COMPANY_NUMBER)
        );
    }

    @Test
    @DisplayName("Given a company number with no PSCs, return 404 not found")
    void getPscListNotFound() throws ApiErrorResponseException, URIValidationException {
        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.NOT_FOUND.value(), HttpStatus.NOT_FOUND.getReasonPhrase(), new
                HttpHeaders()).build();

        when(privateDeltaResourceHandler.getPscs(MOCK_COMPANY_LINKS_URI)).thenReturn(pscGetAll);
        when(pscGetAll.execute()).thenThrow(
                ApiErrorResponseException.fromHttpResponseException(httpResponseException));

        ApiResponse<PscList>  response = pscService.getPscList(MOCK_CONTEXT_ID,
                MOCK_COMPANY_NUMBER);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    }

    @Test
    @DisplayName("Given an internal server error when retrieving a PSCs, return 500")
    void getCompanyProfileInternalServerError() throws ApiErrorResponseException,
            URIValidationException {
        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
                new HttpHeaders()).build();

        when(privateDeltaResourceHandler.getPscs(MOCK_COMPANY_LINKS_URI)).thenReturn(pscGetAll);
        when(pscGetAll.execute()).thenThrow(
                ApiErrorResponseException.fromHttpResponseException(httpResponseException));

        ApiResponse<PscList>  response = pscService.getPscList(MOCK_CONTEXT_ID,
                MOCK_COMPANY_NUMBER);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
    }
}