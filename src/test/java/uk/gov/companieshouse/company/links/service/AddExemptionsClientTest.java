package uk.gov.companieshouse.company.links.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import java.util.Collections;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.company.PrivateCompanyLinksResourceHandler;
import uk.gov.companieshouse.api.handler.company.links.request.PrivateCompanyExemptionsLinksPatch;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@ExtendWith(MockitoExtension.class)
class AddExemptionsClientTest {

    private static final String PATH = "/company/12345678/links/exemptions";

    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Mock
    private InternalApiClient internalApiClient;

    @Mock
    private PrivateCompanyLinksResourceHandler resourceHandler;

    @Mock
    private PrivateCompanyExemptionsLinksPatch exemptionsLinksPatchHandler;

    @Mock
    private Logger logger;

    @InjectMocks
    private AddExemptionsClient client;


    @Test
    void testUpsert() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.addExemptionsCompanyLink(anyString())).thenReturn(exemptionsLinksPatchHandler);
        when(exemptionsLinksPatchHandler.execute()).thenReturn(new ApiResponse<>(200, Collections.emptyMap()));

        // when
        client.addExemptionsLink(PATH);

        // then
        verify(resourceHandler).addExemptionsCompanyLink(PATH);
        verify(exemptionsLinksPatchHandler).execute();
    }

    @Test
    void testThrowNonRetryableExceptionIfClientErrorReturned() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.addExemptionsCompanyLink(anyString())).thenReturn(exemptionsLinksPatchHandler);
        when(exemptionsLinksPatchHandler.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(404, "Not found", new HttpHeaders())));

        // when
        Executable actual = () -> client.addExemptionsLink(PATH);

        // then
        assertThrows(NonRetryableErrorException.class, actual);
        verify(resourceHandler).addExemptionsCompanyLink(PATH);
        verify(exemptionsLinksPatchHandler).execute();
    }

    @Test
    void testThrowRetryableExceptionIfServerErrorReturned() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.addExemptionsCompanyLink(anyString())).thenReturn(exemptionsLinksPatchHandler);
        when(exemptionsLinksPatchHandler.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(500, "Internal server error", new HttpHeaders())));

        // when
        Executable actual = () -> client.addExemptionsLink(PATH);

        // then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).addExemptionsCompanyLink(PATH);
        verify(exemptionsLinksPatchHandler).execute();
    }

    @Test
    void testThrowRetryableExceptionIfIllegalArgumentExceptionIsCaught() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.addExemptionsCompanyLink(anyString())).thenReturn(exemptionsLinksPatchHandler);
        when(exemptionsLinksPatchHandler.execute()).thenThrow(new IllegalArgumentException("Internal server error"));

        // when
        Executable actual = () -> client.addExemptionsLink(PATH);

        // then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).addExemptionsCompanyLink(PATH);
        verify(exemptionsLinksPatchHandler).execute();
    }

    @Test
    void testThrowNonRetryableExceptionIfPathInvalid() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.addExemptionsCompanyLink(anyString())).thenReturn(exemptionsLinksPatchHandler);
        when(exemptionsLinksPatchHandler.execute()).thenThrow(new URIValidationException("Invalid URI"));

        // when
        Executable actual = () -> client.addExemptionsLink("/invalid/path");

        // then
        assertThrows(NonRetryableErrorException.class, actual);
        verify(resourceHandler).addExemptionsCompanyLink("/invalid/path");
        verify(exemptionsLinksPatchHandler).execute();
    }
}
