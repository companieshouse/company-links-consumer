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
import uk.gov.companieshouse.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith(MockitoExtension.class)
class CompanyNumberExtractorTest {

    @Mock
    private Logger logger;

    @InjectMocks
    private CompanyNumberExtractor extractor;

    @Test
    @DisplayName("The extractor should get the correct company number back")
    void process() {
        // given
        // when
        String actual = extractor.extractCompanyNumber("company/12345678/5exemptions/1234/officers");

        // then
        assertEquals("12345678", actual);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("extractorFixtures")
    void processPatternDoesNotMatch(String displayName, String uri, String expected) {
        // given

        // when
        Executable executable = () -> extractor.extractCompanyNumber("company-exemptions");

        // then
        Exception exception = assertThrows(NonRetryableErrorException.class, executable);
        assertEquals("Could not extract company number from resource URI: company-exemptions", exception.getMessage());
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
