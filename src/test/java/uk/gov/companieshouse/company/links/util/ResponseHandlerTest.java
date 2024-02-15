package uk.gov.companieshouse.company.links.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@ExtendWith(MockitoExtension.class)
class ResponseHandlerTest {

    private static final String COMPANY_NUMBER = "12345678";

    @Mock
    private Logger logger;

    @InjectMocks
    private ResponseHandler responseHandler;

    @Test
    void handleURIValidationException() {
        // when
        Executable executable = () -> responseHandler.handle(COMPANY_NUMBER, new URIValidationException("Invalid URI"));

        // then
        NonRetryableErrorException exception = assertThrows(NonRetryableErrorException.class, executable);
        assertEquals(String.format("Invalid companyNumber [%s] when " +
                "handling API request", COMPANY_NUMBER), exception.getMessage());
    }

    @Test
    void handleIllegalArgumentException() {
        // when
        Executable executable = () -> responseHandler.handle(new IllegalArgumentException("Illegal Argument"));

        // then
        RetryableErrorException exception = assertThrows(RetryableErrorException.class, executable);
        assertEquals("Illegal argument exception caught when " +
                "handling API response", exception.getMessage());
    }

    @Test
    void handleApiErrorResponseExceptionRetryableServiceUnavailable() {
        // given
        HttpResponseException.Builder builder = new HttpResponseException.Builder(503, "service unavailable", new HttpHeaders());
        ApiErrorResponseException apiErrorResponseException = new ApiErrorResponseException(builder);

        // when
        Executable executable = () -> responseHandler.handle(503, "exemptions", apiErrorResponseException);

        // then
        RetryableErrorException exception = assertThrows(RetryableErrorException.class, executable);
        assertEquals("Server error returned with status code: [503] "
                + "when processing add company exemptions link", exception.getMessage());
    }

    @Test
    void handleApiErrorResponseExceptionRetryableNotFound() {
        // given
        HttpResponseException.Builder builder = new HttpResponseException.Builder(404, "not found", new HttpHeaders());
        ApiErrorResponseException apiErrorResponseException = new ApiErrorResponseException(builder);

        // when
        Executable executable = () -> responseHandler.handle(404,"filing history", apiErrorResponseException);

        // then
        RetryableErrorException exception = assertThrows(RetryableErrorException.class, executable);
        assertEquals("HTTP 404 Not Found returned; " +
                "company profile does not exist", exception.getMessage());
    }

    @Test
    void handleApiErrorResponseExceptionNonRetryableConflict() {
        // given
        HttpResponseException.Builder builder = new HttpResponseException.Builder(409, "conflict", new HttpHeaders());
        ApiErrorResponseException apiErrorResponseException = new ApiErrorResponseException(builder);

        // when
        Executable executable = () -> responseHandler.handle(409, "PSC statements", apiErrorResponseException);

        // then
        NonRetryableErrorException exception = assertThrows(NonRetryableErrorException.class, executable);
        assertEquals("HTTP 409 Conflict returned; "
                + "company profile already has a PSC statements link", exception.getMessage());
    }

    @Test
    void handleGenericApiErrorResponseExceptionNonRetryableError(){
        HttpResponseException.Builder builder = new HttpResponseException.Builder(405, "method not allowed", new HttpHeaders());
        ApiErrorResponseException apiErrorResponseException = new ApiErrorResponseException(builder);

        // when
        Executable executable = () -> responseHandler.handle(405,"filing history", apiErrorResponseException);

        // then
        NonRetryableErrorException exception = assertThrows(NonRetryableErrorException.class, executable);
        assertEquals("Add filing history client error returned with "
                + "status code: [405] when processing link request", exception.getMessage());
    }
}