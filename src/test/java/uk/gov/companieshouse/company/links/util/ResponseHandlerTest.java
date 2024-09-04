package uk.gov.companieshouse.company.links.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

    @ParameterizedTest
    @CsvSource({
            "401 , Unauthorised",
            "403 , Forbidden",
            "404 , Not Found",
            "405 , Method Not Allowed",
            "410 , Gone",
            "500 , Internal Server Error",
            "503 , Service Unavailable"
    })
    void handleApiErrorResponseExceptionRetryableNotFound(final int code, final String status) {
        // given
        HttpResponseException.Builder builder = new HttpResponseException.Builder(code, status, new HttpHeaders());
        ApiErrorResponseException apiErrorResponseException = new ApiErrorResponseException(builder);

        // when
        Executable executable = () -> responseHandler.handle(code,"filing history", apiErrorResponseException);

        // then
        assertThrows(RetryableErrorException.class, executable);
    }

    @Test
    void handleApiErrorResponseExceptionConflict() {
        // given
        HttpResponseException.Builder builder = new HttpResponseException.Builder(409, "conflict", new HttpHeaders());
        ApiErrorResponseException apiErrorResponseException = new ApiErrorResponseException(builder);

        // when
        Executable executable = () -> responseHandler.handle(409, "PSC statements", apiErrorResponseException);

        // then
        assertDoesNotThrow(executable);
        verify(logger).info(eq("Link already present in target resource - continuing with process"), any());
    }

    @Test
    void handleGenericApiErrorResponseExceptionNonRetryableError(){
        HttpResponseException.Builder builder = new HttpResponseException.Builder(400, "Bad Request", new HttpHeaders());
        ApiErrorResponseException apiErrorResponseException = new ApiErrorResponseException(builder);

        // when
        Executable executable = () -> responseHandler.handle(400,"filing history", apiErrorResponseException);

        // then
        assertThrows(NonRetryableErrorException.class, executable);
    }
}