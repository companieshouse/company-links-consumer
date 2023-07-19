package uk.gov.companieshouse.company.links.service;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;

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
import uk.gov.companieshouse.api.appointment.OfficerList;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.appointment.PrivateCompanyAppointmentsList;
import uk.gov.companieshouse.api.handler.appointment.PrivateCompanyAppointmentsListHandler;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.api.http.HttpClient;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@ExtendWith(MockitoExtension.class)
class AppointmentsListClientTest {

    private static final String COMPANY_NUMBER = "12345678";
    private static final String REQUEST_ID = "request_id";
    private static final String PATH = String.format("/company/%s/officers-test", COMPANY_NUMBER);

    @Mock
    private Supplier<InternalApiClient> internalApiClientSupplier;

    @Mock
    private InternalApiClient internalApiClient;

    @Mock
    private PrivateCompanyAppointmentsListHandler resourceHandler;

    @Mock
    private PrivateCompanyAppointmentsList privateCompanyAppointmentsList;

    @Mock
    private HttpClient httpClient;

    @Mock
    private Logger logger;

    @InjectMocks
    private AppointmentsListClient client;

    @BeforeEach
    void setup() {
        when(internalApiClientSupplier.get()).thenReturn(internalApiClient);
        when(internalApiClient.getHttpClient()).thenReturn(httpClient);
        when(internalApiClient.privateCompanyAppointmentsListHandler()).thenReturn(resourceHandler);
        when(resourceHandler.getCompanyAppointmentsList(anyString())).thenReturn(
                privateCompanyAppointmentsList);
    }

    @Test
    void shouldReturnOfficerList() throws Exception {
        // given
        when(privateCompanyAppointmentsList.execute()).thenReturn(
                new ApiResponse<>(200, Collections.emptyMap(),
                        new OfficerList()
                                .totalResults(0)));

        // when
        OfficerList officerList = client.getAppointmentsList(COMPANY_NUMBER, REQUEST_ID);

        // then
        verify(resourceHandler).getCompanyAppointmentsList(PATH);
        verify(privateCompanyAppointmentsList).execute();
        assertThat(officerList).isNotNull();
        assertThat(officerList.getTotalResults()).isZero();
    }

    @Test
    void shouldThrowApiErrorResponseExceptionWhenServerError() throws Exception {
        // given
        when(privateCompanyAppointmentsList.execute())
                .thenThrow(new ApiErrorResponseException(
                        new HttpResponseException
                                .Builder(INTERNAL_SERVER_ERROR.value(),
                                "Internal server error", new HttpHeaders())));

        Executable actual = () -> client.getAppointmentsList(COMPANY_NUMBER, REQUEST_ID);

        Exception ex = assertThrows(RetryableErrorException.class, actual);
        assertThat(ex.getMessage())
                .isEqualTo("Server error status code: "
                        + "[500] while fetching appointments list for company 12345678");
    }

    @Test
    void shouldReturnOfficerListWithTotalCountZeroWhenNotFound() throws Exception {
        // given
        when(privateCompanyAppointmentsList.execute())
                .thenThrow(new ApiErrorResponseException(
                        new HttpResponseException
                                .Builder(NOT_FOUND.value(),
                                "Not found", new HttpHeaders()
                                .setContentLength(0L))));

        OfficerList officerList = client.getAppointmentsList(COMPANY_NUMBER, REQUEST_ID);

        verify(resourceHandler).getCompanyAppointmentsList(PATH);
        verify(privateCompanyAppointmentsList).execute();
        assertThat(officerList).isNotNull();
        assertThat(officerList.getTotalResults()).isZero();
    }

    @Test
    void shouldThrowRetryableErrorExceptionWhenAppointmentsApiIsDown() throws Exception {
        // given
        when(privateCompanyAppointmentsList.execute())
                .thenThrow(new ApiErrorResponseException(
                        new HttpResponseException
                                .Builder(NOT_FOUND.value(),
                                "Not found", new HttpHeaders()
                                .setContentLength(2L))
                                .setContent("{}")));

        Executable actual = () -> client.getAppointmentsList(COMPANY_NUMBER, REQUEST_ID);

        Exception ex = assertThrows(RetryableErrorException.class, actual);
        assertThat(ex.getMessage())
                .isEqualTo("Company-appointments service is not available");
    }

    @Test
    void shouldThrowApiErrorResponseExceptionWhenClientError() throws Exception {
        // given
        when(privateCompanyAppointmentsList.execute())
                .thenThrow(new ApiErrorResponseException(
                        new HttpResponseException
                                .Builder(BAD_REQUEST.value(),
                                "Bad request", new HttpHeaders())));

        Executable actual = () -> client.getAppointmentsList(COMPANY_NUMBER, REQUEST_ID);

        Exception ex = assertThrows(NonRetryableErrorException.class, actual);
        assertThat(ex.getMessage())
                .isEqualTo("Client error status code: [400] "
                        + "while fetching appointments list");
    }

    @Test
    void shouldThrowRetryableErrorExceptionWhenIllegalArgumentThrownByApi() throws Exception {
        // given
        when(privateCompanyAppointmentsList.execute())
                .thenThrow(new IllegalArgumentException());

        Executable actual = () -> client.getAppointmentsList(COMPANY_NUMBER, REQUEST_ID);

        Exception ex = assertThrows(RetryableErrorException.class, actual);
        assertThat(ex.getMessage())
                .isEqualTo("Error returned when fetching appointments");
    }

    @Test
    void shouldThrowNonRetryableErrorExceptionWhenURIValidationExceptionThrownByApi() throws Exception {
        // given
        when(privateCompanyAppointmentsList.execute())
                .thenThrow(new URIValidationException("URI wrong"));

        Executable actual = () -> client.getAppointmentsList(COMPANY_NUMBER, REQUEST_ID);

        Exception ex = assertThrows(NonRetryableErrorException.class, actual);
        assertThat(ex.getMessage())
                .isEqualTo("Invalid companyNumber [12345678] when handling API request");
    }
}