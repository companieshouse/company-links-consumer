package uk.gov.companieshouse.company.links.service;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith(MockitoExtension.class)
class CompanyNumberExtractorTest {

    private static final String REQUEST_ID = "request_id";

    @Mock
    private Logger logger;

    @InjectMocks
    private PatchLinkRequestExtractor extractor;

    @Test
    @DisplayName("The extractor should get the correct company number back")
    void process() {
        // given
        // when
        PatchLinkRequest actual = extractor.extractPatchLinkRequest("company/OC305127/appointments/-0YatipCW4ZL295N9UVFo1TGyW8", REQUEST_ID);

        // then
        assertEquals("OC305127", actual.getCompanyNumber());
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("extractorFixtures")
    void processPatternDoesNotMatch(String displayName, String uri, String expected) {
        // given

        // when
        Executable executable = () -> extractor.extractPatchLinkRequest(uri, REQUEST_ID);

        // then
        Exception exception = assertThrows(NonRetryableErrorException.class, executable);
        assertEquals(expected, exception.getMessage());
    }

    private static Stream<Arguments> extractorFixtures() {
        return Stream.of(
                arguments("The extractor should throw a non retryable exception when it cannot extract a company number",
                        "company-exemptions",
                        "Could not extract company number from resource URI: company-exemptions"),
                arguments("The extractor should throw a non retryable exception when it cannot extract an empty company number",
                        "company//exemptions",
                        "Could not extract company number from resource URI: company//exemptions"),
                arguments("The extractor should throw a non retryable exception when it cannot extract a company number from an empty uri",
                        "",
                        "Could not extract company number from empty or null resource uri"),
                arguments("The extractor should throw a non retryable exception when it cannot extract a company number from a null uri",
                        null,
                        "Could not extract company number from empty or null resource uri"));
    }
}
