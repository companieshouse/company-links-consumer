package uk.gov.companieshouse.company.links.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import java.util.Collections;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.company.PrivateCompanyLinksResourceHandler;
import uk.gov.companieshouse.api.handler.company.links.request.PrivateFilingHistoryLinksAdd;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.http.HttpClient;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

@ExtendWith(MockitoExtension.class)
class AddFilingHistoryClientTest {

    private static final String COMPANY_NUMBER = "12345678";
    private static final String REQUEST_ID = "request_id";
    private static final String PATH = String.format("/company/%s/links/filing-history",
            COMPANY_NUMBER);

    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Mock
    private InternalApiClient internalApiClient;

    @Mock
    private PrivateCompanyLinksResourceHandler resourceHandler;

    @Mock
    private PrivateFilingHistoryLinksAdd filingHistoryLinksAdd;
    @Mock
    private HttpClient httpClient;
    @Mock
    private Logger logger;

    @InjectMocks
    private AddFilingHistoryClient client;

    private final PatchLinkRequest linkRequest = new PatchLinkRequest(COMPANY_NUMBER, REQUEST_ID);

    @BeforeEach
    void setup() {
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.getHttpClient()).thenReturn(httpClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.addFilingHistoryLink(anyString())).thenReturn(
                filingHistoryLinksAdd);
    }

    @Test
    void testUpsert() throws ApiErrorResponseException, URIValidationException {
        // given
        when(resourceHandler.addFilingHistoryLink(anyString())).thenReturn(
                filingHistoryLinksAdd);
        when(filingHistoryLinksAdd.execute()).thenReturn(
                new ApiResponse<>(200, Collections.emptyMap()));

        // when
        client.patchLink(linkRequest);

        // then
        verify(resourceHandler).addFilingHistoryLink(PATH);
        verify(filingHistoryLinksAdd).execute();
    }

    @Test
    void testThrowRetryableExceptionIf404Returned()
            throws ApiErrorResponseException, URIValidationException {
        // given
        when(filingHistoryLinksAdd.execute())
                .thenThrow(new ApiErrorResponseException(
                        new HttpResponseException.Builder(404, "Not found", new HttpHeaders())));

        // when
        Executable actual = () -> client.patchLink(linkRequest);

        // then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).addFilingHistoryLink(PATH);
        verify(filingHistoryLinksAdd).execute();
    }

    @Test
    void testThrowNonRetryableExceptionIf409Returned()
            throws ApiErrorResponseException, URIValidationException {
        // given
        when(filingHistoryLinksAdd.execute())
                .thenThrow(new ApiErrorResponseException(
                        new HttpResponseException.Builder(409, "Conflict", new HttpHeaders())));

        // when
        client.patchLink(linkRequest);

        // then
        verify(resourceHandler).addFilingHistoryLink(PATH);
        verify(filingHistoryLinksAdd).execute();
        verify(logger).info(
                eq("HTTP 409 Conflict returned; company profile already has a filing history link"),
                anyMap());
    }

    @Test
    void testThrowRetryableExceptionIfServerErrorReturned()
            throws ApiErrorResponseException, URIValidationException {
        // given
        when(filingHistoryLinksAdd.execute()).thenThrow(
                new ApiErrorResponseException(
                        new HttpResponseException.Builder(500, "Internal server error",
                                new HttpHeaders())));

        // when
        Executable actual = () -> client.patchLink(linkRequest);

        // then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).addFilingHistoryLink(PATH);
        verify(filingHistoryLinksAdd).execute();
    }

    @Test
    void testThrowRetryableExceptionIfIllegalArgumentExceptionIsCaught()
            throws ApiErrorResponseException, URIValidationException {
        // given
        when(filingHistoryLinksAdd.execute()).thenThrow(
                new IllegalArgumentException("Internal server error"));

        // when
        Executable actual = () -> client.patchLink(linkRequest);

        // then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).addFilingHistoryLink(PATH);
        verify(filingHistoryLinksAdd).execute();
    }

    @Test
    void testThrowNonRetryableExceptionIfCompanyNumberInvalid()
            throws ApiErrorResponseException, URIValidationException {
        // given
        when(filingHistoryLinksAdd.execute()).thenThrow(new URIValidationException("Invalid URI"));

        // when
        Executable actual = () -> client.patchLink(
                new PatchLinkRequest("invalid/path", REQUEST_ID));

        // then
        assertThrows(NonRetryableErrorException.class, actual);
        verify(resourceHandler).addFilingHistoryLink("/company/invalid/path/links/filing-history");
        verify(filingHistoryLinksAdd).execute();
    }
}
