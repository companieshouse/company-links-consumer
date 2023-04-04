package uk.gov.companieshouse.company.links.service;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

@ExtendWith(MockitoExtension.class)
class NullLinkClientTest {

    private static final String COMPANY_NUMBER = "12345678";

    @Mock
    private Logger logger;

    @InjectMocks
    private NullLinkClient client;

    private final PatchLinkRequest linkRequest = new PatchLinkRequest(COMPANY_NUMBER);

    @Test
    void shouldThrowNonRetryableErrorExceptionWhenPatchLinkInvoked() {
        Executable actual = () -> client.patchLink(linkRequest);

        assertThrows(NonRetryableErrorException.class, actual);
    }

}