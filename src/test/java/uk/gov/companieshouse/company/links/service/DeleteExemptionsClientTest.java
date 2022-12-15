package uk.gov.companieshouse.company.links.service;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.company.PrivateCompanyLinksResourceHandler;
import uk.gov.companieshouse.api.handler.company.links.request.PrivateCompanyExemptionsLinksDelete;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

import java.util.Collections;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeleteExemptionsClientTest {

    private static final String PATH = "/company/12345678/links/exemptions/delete";

    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Mock
    private InternalApiClient internalApiClient;

    @Mock
    private PrivateCompanyLinksResourceHandler resourceHandler;

    @Mock
    private PrivateCompanyExemptionsLinksDelete exemptionsLinksDelete;

    @Mock
    private Logger logger;

    @InjectMocks
    private DeleteExemptionsClient client;

    @Test
    void testDelete() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deleteExemptionsCompanyLink(anyString())).thenReturn(exemptionsLinksDelete);
        when(exemptionsLinksDelete.execute()).thenReturn(new ApiResponse<>(200, Collections.emptyMap()));

        // when
        client.deleteExemptionsLink(PATH);

        // then
        verify(resourceHandler).deleteExemptionsCompanyLink(PATH);
        verify(exemptionsLinksDelete).execute();
    }

    @Test
    void testThrowNonRetryableExceptionIfClientErrorReturned() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deleteExemptionsCompanyLink(anyString())).thenReturn(exemptionsLinksDelete);
        when(exemptionsLinksDelete.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(404, "Not found", new HttpHeaders())));

        // when
        client.deleteExemptionsLink(PATH);

        // then
        verify(resourceHandler).deleteExemptionsCompanyLink(PATH);
        verify(exemptionsLinksDelete).execute();
    }

    @Test
    void testThrowRetryableExceptionIfServerErrorReturned() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deleteExemptionsCompanyLink(anyString())).thenReturn(exemptionsLinksDelete);
        when(exemptionsLinksDelete.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(500, "Internal server error", new HttpHeaders())));

        // when
        Executable actual = () -> client.deleteExemptionsLink(PATH);

        // then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).deleteExemptionsCompanyLink(PATH);
        verify(exemptionsLinksDelete).execute();
    }

    @Test
    void testThrowRetryableExceptionIfIllegalArgumentExceptionIsCaught() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deleteExemptionsCompanyLink(anyString())).thenReturn(exemptionsLinksDelete);
        when(exemptionsLinksDelete.execute()).thenThrow(new IllegalArgumentException("Internal server error"));

        // when
        Executable actual = () -> client.deleteExemptionsLink(PATH);

        // then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).deleteExemptionsCompanyLink(PATH);
        verify(exemptionsLinksDelete).execute();
    }

    @Test
    void testThrowNonRetryableExceptionIfPathInvalid() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deleteExemptionsCompanyLink(anyString())).thenReturn(exemptionsLinksDelete);
        when(exemptionsLinksDelete.execute()).thenThrow(new URIValidationException("Invalid URI"));

        // when
        Executable actual = () -> client.deleteExemptionsLink("/invalid/path");

        // then
        assertThrows(NonRetryableErrorException.class, actual);
        verify(resourceHandler).deleteExemptionsCompanyLink("/invalid/path");
        verify(exemptionsLinksDelete).execute();
    }
}
