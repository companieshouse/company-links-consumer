package uk.gov.companieshouse.company.links.service;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
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
import uk.gov.companieshouse.api.handler.company.links.request.PrivatePscLinksDelete;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.http.HttpClient;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

import java.util.Collections;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeletePscLinkClientTest {

    @InjectMocks
    DeletePscLinkClient deletePscLinkClient;

    @Mock
    private Logger logger;
    @Mock
    private InternalApiClient internalApiClient;
    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Mock
    private PrivateCompanyLinksResourceHandler resourceHandler;
    @Mock
    private PrivatePscLinksDelete pscLinksDelete;
    private static final String COMPANY_NUMBER = "12345678";
    private static final String REQUEST_ID = "request_id";
    private final PatchLinkRequest linkRequest = new PatchLinkRequest(COMPANY_NUMBER, REQUEST_ID);
    private static final String PATH = String.format("/company/%s/links/persons-with-significant-control/delete", COMPANY_NUMBER);



    @Mock
    private HttpClient httpClient;

    @BeforeEach
    void setup() {
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.privateCompanyLinksResourceHandler()).thenReturn(resourceHandler);
        when(resourceHandler.deletePscCompanyLink(anyString())).thenReturn(pscLinksDelete);
    }

    @Test
    void testDelete() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscLinksDelete.execute()).thenReturn(new ApiResponse<>(200, Collections.emptyMap()));

        //when
        deletePscLinkClient.patchLink(linkRequest);

        //then
        verify(resourceHandler).deletePscCompanyLink(PATH);
        verify(pscLinksDelete).execute();
    }


    @Test
    void testThrowNonRetryableExceptionIfClientErrorReturned() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscLinksDelete.execute()).thenThrow(new ApiErrorResponseException(
                new HttpResponseException.Builder(404, "Not found", new HttpHeaders())));

        //when
        deletePscLinkClient.patchLink(linkRequest);

        //then
        verify(resourceHandler).deletePscCompanyLink(PATH);
        verify(pscLinksDelete).execute();
        verify(logger).info("HTTP 404 Not Found returned; company profile does not exist");
    }

    @Test
    void testThrowsNonRetryableExceptionIf409Returned() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscLinksDelete.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(409, "Conflict", new HttpHeaders())));

        //when
        deletePscLinkClient.patchLink(linkRequest);

        //then
        verify(resourceHandler).deletePscCompanyLink(PATH);
        verify(pscLinksDelete).execute();
        verify(logger).info("HTTP 409 Conflict returned; company profile does not have a PSC link already");
    }

    @Test
    void testThrowRetryableExceptionIfServerErrorReturned() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscLinksDelete.execute()).thenThrow(new ApiErrorResponseException(new HttpResponseException.Builder(500, "Internal server error", new HttpHeaders())));

        //when
        Executable actual = () -> deletePscLinkClient.patchLink(linkRequest);

        //then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).deletePscCompanyLink(PATH);
        verify(pscLinksDelete).execute();
    }

    @Test
    void testThrowRetryableExceptionIfIllegalArgumentExceptionIsCaught() throws ApiErrorResponseException, URIValidationException {
        //given
        when(pscLinksDelete.execute()).thenThrow(new IllegalArgumentException("Internal server error"));

        //when
        Executable actual = () -> deletePscLinkClient.patchLink(linkRequest);

        //then
        assertThrows(RetryableErrorException.class, actual);
        verify(resourceHandler).deletePscCompanyLink(PATH);
        verify(pscLinksDelete).execute();
    }
}
