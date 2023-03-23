package uk.gov.companieshouse.company.links.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;

import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.company.PrivateCompanyLinksResourceHandler;
import uk.gov.companieshouse.api.handler.company.links.request.PrivatePscStatementsLinksDelete;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@ExtendWith(MockitoExtension.class)
public class DeleteStatementsClientTest {

    private static final String COMPANY_NUMBER = "12345678";
    private static final String PATH = String.format("/company/%s/links/persons-with-significant-control-statements/delete", COMPANY_NUMBER);

    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Mock
    private InternalApiClient internalApiClient;

    @Mock
    private PrivateCompanyLinksResourceHandler resourceHandler;

    @Mock
    private PrivatePscStatementsLinksDelete statementsLinksDeleteHandler;

    @Mock
    private Logger logger;

    @InjectMocks
    private DeleteStatementsClient client;

    @Test
    void testUpsert() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deletePscStatementsCompanyLink(anyString())).thenReturn(statementsLinksDeleteHandler);
        when(statementsLinksDeleteHandler.execute()).thenReturn(new ApiResponse<>(200, Collections.emptyMap()));

        // when
        client.patchLink(COMPANY_NUMBER);

        // then
        verify(resourceHandler).deletePscStatementsCompanyLink(PATH);
        verify(statementsLinksDeleteHandler).execute();
    }

    @Test
    void testThrowNonRetryableExceptionIfClientErrorReturned() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deletePscStatementsCompanyLink(anyString())).thenReturn(statementsLinksDeleteHandler);
        when(statementsLinksDeleteHandler.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(404, "Not found", new HttpHeaders())));

        // when
        client.patchLink(COMPANY_NUMBER);

        // then
        verify(resourceHandler).deletePscStatementsCompanyLink(PATH);
        verify(statementsLinksDeleteHandler).execute();
        verify(logger).info("HTTP 404 Not Found returned; company profile does not exist");
    }

    @Test
    void testThrowNonRetryableExceptionIf409Returned() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deletePscStatementsCompanyLink(anyString())).thenReturn(statementsLinksDeleteHandler);
        when(statementsLinksDeleteHandler.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(409, "Conflict", new HttpHeaders())));

        // when
        client.patchLink(COMPANY_NUMBER);

        // then
        verify(resourceHandler).deletePscStatementsCompanyLink(PATH);
        verify(statementsLinksDeleteHandler).execute();
        verify(logger).info("HTTP 409 Conflict returned; company profile does not have an statements link already");
    }
    
    @Test
    void testThrowRetryableExceptionIfServerErrorReturned() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deletePscStatementsCompanyLink(anyString())).thenReturn(statementsLinksDeleteHandler);
        when(statementsLinksDeleteHandler.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(500, "Internal server error", new HttpHeaders())));

        // when
        Executable actual = () -> client.patchLink(COMPANY_NUMBER);

        // then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).deletePscStatementsCompanyLink(PATH);
        verify(statementsLinksDeleteHandler).execute();
    }

    @Test
    void testThrowRetryableExceptionIfIllegalArgumentExceptionIsCaught() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deletePscStatementsCompanyLink(anyString())).thenReturn(statementsLinksDeleteHandler);
        when(statementsLinksDeleteHandler.execute()).thenThrow(new IllegalArgumentException("Internal server error"));

        // when
        Executable actual = () -> client.patchLink(COMPANY_NUMBER);

        // then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).deletePscStatementsCompanyLink(PATH);
        verify(statementsLinksDeleteHandler).execute();
    }

    @Test
    void testThrowNonRetryableExceptionIfComapnyNumberInvalid() throws ApiErrorResponseException, URIValidationException {
        // given
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deletePscStatementsCompanyLink(anyString())).thenReturn(statementsLinksDeleteHandler);
        when(statementsLinksDeleteHandler.execute()).thenThrow(new URIValidationException("Invalid URI"));

        // when
        Executable actual = () -> client.patchLink("invalid/path");

        // then
        assertThrows(NonRetryableErrorException.class, actual);
        verify(resourceHandler).deletePscStatementsCompanyLink("/company/invalid/path/links/persons-with-significant-control-statements/delete");
        verify(statementsLinksDeleteHandler).execute();
    }
}
