package uk.gov.companieshouse.company.links.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class AddExemptionsServiceTest {

    @Mock
    private AddExemptionsClient client;

    @Mock
    private Logger logger;

    @InjectMocks
    private AddExemptionsService service;

    @Test
    @DisplayName("The service should call add exemptions client with the correct company number")
    void process() {
        // given

        // when
        service.process("company/12345678/exemptions");

        // then
        verify(client).addExemptionsLink("/company/12345678/links/exemptions");
    }

    @Test
    @DisplayName("The service should throw a non retryable exception when it cannot extract a company number")
    void processPatternDoesNotMatch() {
        // given

        // when
        Executable executable = () -> service.process("company-exemptions");

        // then
        Exception exception = assertThrows(NonRetryableErrorException.class, executable);
        assertEquals("Could not extract company number from resource URI: company-exemptions", exception.getMessage());
        verifyNoInteractions(client);
    }

    @Test
    @DisplayName("The service should throw a non retryable exception when it cannot extract an empty company number")
    void processBlankCompanyNumber() {
        // given

        // when
        Executable executable = () -> service.process("company//exemptions");

        // then
        Exception exception = assertThrows(NonRetryableErrorException.class, executable);
        assertEquals("Could not extract company number from resource URI: company//exemptions", exception.getMessage());
        verifyNoInteractions(client);
    }

    @Test
    @DisplayName("The service should throw a non retryable exception when it cannot extract a company number from an empty uri")
    void processEmptyStringUri() {
        // given

        // when
        Executable executable = () -> service.process("");

        // then
        Exception exception = assertThrows(NonRetryableErrorException.class, executable);
        assertEquals("Could not extract company number from empty or null resource uri", exception.getMessage());
        verifyNoInteractions(client);
    }

    @Test
    @DisplayName("The service should throw a non retryable exception when it cannot extract a company number from a null uri")
    void processNullUri() {
        // given

        // when
        Executable executable = () -> service.process(null);

        // then
        Exception exception = assertThrows(NonRetryableErrorException.class, executable);
        assertEquals("Could not extract company number from empty or null resource uri", exception.getMessage());
        verifyNoInteractions(client);
    }
}
