package uk.gov.companieshouse.company.links.service;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.delta.PrivateDeltaResourceHandler;
import uk.gov.companieshouse.api.handler.delta.charges.request.PrivateChargesGetAll;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@ExtendWith(MockitoExtension.class)
class ChargesServiceTest {
    private static final String MOCK_CONTEXT_ID = "context_id";
    private static final String MOCK_COMPANY_NUMBER = "6146287";
    private static final String MOCK_COMPANY_LINKS_URI = String.format("/company/%s/charges",
            MOCK_COMPANY_NUMBER);

    private ChargesService chargesService;

    @Mock
    private ChargesApi chargesApi;

    @Mock
    private Logger logger;

    @Mock
    private InternalApiClient apiClient;

    @Mock
    private PrivateDeltaResourceHandler deltaResourceHandler;

    @Mock
    private PrivateChargesGetAll privateChargesGetAll;

    @BeforeEach
    void setup() {
        chargesService = spy(new ChargesService(logger));
        when(chargesService.getApiClient(MOCK_CONTEXT_ID)).thenReturn(apiClient);
        when(apiClient.privateDeltaChargeResourceHandler()).thenReturn(deltaResourceHandler);
    }

    @Test
    @DisplayName("Successfully retrieve charges")
    void getCharges() throws ApiErrorResponseException, URIValidationException {
        final ApiResponse<ChargesApi> expected = new ApiResponse<>(
                HttpStatus.OK.value(), Collections.emptyMap(), chargesApi);

        when(deltaResourceHandler.getCharges(MOCK_COMPANY_LINKS_URI)).thenReturn(
            privateChargesGetAll);
        when(privateChargesGetAll.execute()).thenReturn(expected);

        final ApiResponse<ChargesApi> response = chargesService.getCharges(
                MOCK_CONTEXT_ID, MOCK_COMPANY_NUMBER);

        assertThat(response).isSameAs(expected);
    }

    @Test
    @DisplayName("Given a bad URI when retrieving charges, return 404 not found")
    void getChargesBadUri() throws ApiErrorResponseException, URIValidationException {
        when(deltaResourceHandler.getCharges(MOCK_COMPANY_LINKS_URI)).thenReturn(
            privateChargesGetAll);
        when(privateChargesGetAll.execute()).thenThrow(new URIValidationException("expected"));

        assertThrows(
                RetryableErrorException.class,
                () -> chargesService.getCharges(MOCK_CONTEXT_ID,
                        MOCK_COMPANY_NUMBER));

    }

    @Test
    @DisplayName("Given a company number with no matching charges record, return 404 not found")
    void getChargesNotFound() throws ApiErrorResponseException, URIValidationException {
        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.NOT_FOUND.value(), HttpStatus.NOT_FOUND.getReasonPhrase(), new
                HttpHeaders()).build();

        when(deltaResourceHandler.getCharges(MOCK_COMPANY_LINKS_URI)).thenReturn(
            privateChargesGetAll);
        when(privateChargesGetAll.execute()).thenThrow(
                ApiErrorResponseException.fromHttpResponseException(httpResponseException));

        assertThrows(
                RetryableErrorException.class,
                () -> chargesService.getCharges(MOCK_CONTEXT_ID,
                        MOCK_COMPANY_NUMBER));
    }

    @Test
    @DisplayName("Given an internal server error when retrieving a company profile, return 500")
    void getChargesInternalServerError() throws ApiErrorResponseException,
            URIValidationException {
        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
                new HttpHeaders()).build();

        when(deltaResourceHandler.getCharges(MOCK_COMPANY_LINKS_URI)).thenReturn(
            privateChargesGetAll);
        when(privateChargesGetAll.execute()).thenThrow(
                ApiErrorResponseException.fromHttpResponseException(httpResponseException));

        assertThrows(
                RetryableErrorException.class,
                () -> chargesService.getCharges(MOCK_CONTEXT_ID,
                        MOCK_COMPANY_NUMBER));

    }
}