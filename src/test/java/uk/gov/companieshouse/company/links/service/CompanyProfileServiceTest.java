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
import org.springframework.web.server.ResponseStatusException;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.company.PrivateCompanyResourceHandler;
import uk.gov.companieshouse.api.handler.company.request.PrivateCompanyProfileGet;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.logging.Logger;

@ExtendWith(MockitoExtension.class)
class CompanyProfileServiceTest {
    private static final String MOCK_CONTEXT_ID = "context_id";
    private static final String MOCK_COMPANY_NUMBER = "6146287";
    private static final String MOCK_COMPANY_URI = String.format("/company/%s",
            MOCK_COMPANY_NUMBER);

    private CompanyProfileService companyProfileService;

    @Mock
    private CompanyProfile companyProfile;

    @Mock
    private Logger logger;

    @Mock
    private InternalApiClient apiClient;

    @Mock
    private PrivateCompanyResourceHandler companyResourceHandler;

    @Mock
    private PrivateCompanyProfileGet privateCompanyProfileGet;

    @BeforeEach
    void setup() {
        companyProfileService = spy(new CompanyProfileService(logger));
        when(companyProfileService.getApiClient(MOCK_CONTEXT_ID)).thenReturn(apiClient);
        when(apiClient.privateCompanyResourceHandler()).thenReturn(companyResourceHandler);
    }

    @Test
    @DisplayName("Successfully retrieve a company profile")
    void getCompanyProfile() throws ApiErrorResponseException, URIValidationException {
        final ApiResponse<CompanyProfile> expected = new ApiResponse<>(
                HttpStatus.OK.value(), Collections.emptyMap(), companyProfile);

        when(companyResourceHandler.getCompanyProfile(MOCK_COMPANY_URI)).thenReturn(privateCompanyProfileGet);
        when(privateCompanyProfileGet.execute()).thenReturn(expected);

        final ApiResponse<CompanyProfile> response = companyProfileService.getCompanyProfile(
                MOCK_CONTEXT_ID, MOCK_COMPANY_NUMBER);

        assertThat(response).isSameAs(expected);
    }

    @Test
    @DisplayName("Given a bad URI when retrieving company profile, return 404 not found")
    void getCompanyProfileBadUri() throws ApiErrorResponseException, URIValidationException {
        when(companyResourceHandler.getCompanyProfile(MOCK_COMPANY_URI)).thenReturn(privateCompanyProfileGet);
        when(privateCompanyProfileGet.execute()).thenThrow(new URIValidationException("expected"));

        final ResponseStatusException exception = assertThrows(
                ResponseStatusException.class,
                () -> companyProfileService.getCompanyProfile(MOCK_CONTEXT_ID,
                        MOCK_COMPANY_NUMBER));

        assertThat(exception.getStatus()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("Given a company number with no matching company profile, return 404 not found")
    void getCompanyProfileNotFound() throws ApiErrorResponseException, URIValidationException {
        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.NOT_FOUND.value(), HttpStatus.NOT_FOUND.getReasonPhrase(), new
                HttpHeaders()).build();

        when(companyResourceHandler.getCompanyProfile(MOCK_COMPANY_URI)).thenReturn(privateCompanyProfileGet);
        when(privateCompanyProfileGet.execute()).thenThrow(
                ApiErrorResponseException.fromHttpResponseException(httpResponseException));

        final ResponseStatusException exception = assertThrows(
                ResponseStatusException.class,
                () -> companyProfileService.getCompanyProfile(MOCK_CONTEXT_ID,
                        MOCK_COMPANY_NUMBER));

        assertThat(exception.getStatus()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("Given an internal server error when retrieving a company profile, return 500")
    void getCompanyProfileInternalServerError() throws ApiErrorResponseException,
            URIValidationException {
        final HttpResponseException httpResponseException = new HttpResponseException.Builder(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
                new HttpHeaders()).build();

        when(companyResourceHandler.getCompanyProfile(MOCK_COMPANY_URI)).thenReturn(privateCompanyProfileGet);
        when(privateCompanyProfileGet.execute()).thenThrow(
                ApiErrorResponseException.fromHttpResponseException(httpResponseException));

        final ResponseStatusException exception = assertThrows(
                ResponseStatusException.class,
                () -> companyProfileService.getCompanyProfile(MOCK_CONTEXT_ID,
                        MOCK_COMPANY_NUMBER));

        assertThat(exception.getStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }
}