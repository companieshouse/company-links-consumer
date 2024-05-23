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
import uk.gov.companieshouse.api.handler.delta.insolvency.request.PrivateInsolvencyGet;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.http.HttpClient;
import uk.gov.companieshouse.api.insolvency.CompanyInsolvency;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

import java.util.Collections;
import java.util.function.Supplier;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InsolvencyServiceTest {
    private static final String MOCK_CONTEXT_ID = "context_id";
    private static final String MOCK_COMPANY_NUMBER = "6146287";
    private static final String MOCK_COMPANY_INSOLVENCY_URI =
            String.format("/company/%s/insolvency", MOCK_COMPANY_NUMBER);

    private InsolvencyService insolvencyService;

    @Mock
    private CompanyInsolvency companyInsolvency;

    @Mock
    private Logger logger;

    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Mock
    private InternalApiClient internalApiClient;

    @Mock
    HttpClient httpClient;

    @Mock
    private InternalApiClient apiClient;

    @Mock
    private PrivateDeltaResourceHandler privateDeltaResourceHandler;

    @Mock
    private PrivateInsolvencyGet privateCompanyInsolvencyGet;

    @BeforeEach
    void setup() {
        insolvencyService = spy(new InsolvencyService(logger, internalApiClientSupplier));
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.getHttpClient()).thenReturn(httpClient);
        when(internalApiClient.privateDeltaInsolvencyResourceHandler()).thenReturn(privateDeltaResourceHandler);
    }

    @Test
    @DisplayName("Successfully retrieve a company insolvency")
    void getCompanyInsolvency() throws ApiErrorResponseException, URIValidationException {
        final ApiResponse<CompanyInsolvency> expected = new ApiResponse<>(
                HttpStatus.OK.value(), Collections.emptyMap(), companyInsolvency);

        when(privateDeltaResourceHandler.getInsolvency(MOCK_COMPANY_INSOLVENCY_URI)).thenReturn(privateCompanyInsolvencyGet);
        when(privateCompanyInsolvencyGet.execute()).thenReturn(expected);

        var response = insolvencyService
                .getInsolvency(MOCK_CONTEXT_ID, MOCK_COMPANY_NUMBER);

        assertThat(response).isSameAs(expected);
    }

    @Test
    @DisplayName("Given a company number with no matching company insolvency return a 410 Document Gone response with no data")
    void getCompanyInsolvencyGone() throws ApiErrorResponseException, URIValidationException {
        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.GONE.value(), HttpStatus.GONE.getReasonPhrase(), new
                HttpHeaders()).build();

        when(privateDeltaResourceHandler.getInsolvency(MOCK_COMPANY_INSOLVENCY_URI)).thenReturn(privateCompanyInsolvencyGet);
        when(privateCompanyInsolvencyGet.execute()).thenThrow(
                ApiErrorResponseException.fromHttpResponseException(httpResponseException));

        var response = insolvencyService
                .getInsolvency(MOCK_CONTEXT_ID, MOCK_COMPANY_NUMBER);

        assertThat(response.getStatusCode()).isEqualTo(410);
        assertThat(response.getData()).isNull();
    }

    @Test
    @DisplayName("Given a bad URI when retrieving company insolvency throw a Retryable exception")
    void getCompanyInsolvencyBadUri() throws ApiErrorResponseException, URIValidationException {
        when(privateDeltaResourceHandler.getInsolvency(MOCK_COMPANY_INSOLVENCY_URI)).thenReturn(privateCompanyInsolvencyGet);
        when(privateCompanyInsolvencyGet.execute()).thenThrow(new URIValidationException("expected"));

        assertThrows(
                RetryableErrorException.class,
                () -> insolvencyService.getInsolvency(MOCK_CONTEXT_ID, MOCK_COMPANY_NUMBER));

    }

    @Test
    @DisplayName("Given a bad Request status code when retrieving company insolvency throw a Non-retryable exception")
    void getCompanyInsolvencyBadRequest() throws ApiErrorResponseException, URIValidationException {
        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.BAD_REQUEST.value(), HttpStatus.BAD_REQUEST.getReasonPhrase(), new
                HttpHeaders()).build();

        when(privateDeltaResourceHandler.getInsolvency(MOCK_COMPANY_INSOLVENCY_URI)).thenReturn(privateCompanyInsolvencyGet);
        when(privateCompanyInsolvencyGet.execute()).thenThrow(
                ApiErrorResponseException.fromHttpResponseException(httpResponseException));

        ApiResponse<CompanyInsolvency>  response = insolvencyService.getInsolvency(MOCK_CONTEXT_ID, MOCK_COMPANY_NUMBER);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    }

    @Test
    @DisplayName("Given an internal server error when retrieving a company insolvency throw a Retryable exception")
    void getCompanyInsolvencyInternalServerError() throws ApiErrorResponseException, URIValidationException {
        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
                new HttpHeaders()).build();

        when(privateDeltaResourceHandler.getInsolvency(MOCK_COMPANY_INSOLVENCY_URI)).thenReturn(privateCompanyInsolvencyGet);
        when(privateCompanyInsolvencyGet.execute()).thenThrow(
                ApiErrorResponseException.fromHttpResponseException(httpResponseException));

        ApiResponse<CompanyInsolvency>  response = insolvencyService.getInsolvency(MOCK_CONTEXT_ID, MOCK_COMPANY_NUMBER);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
    }
}