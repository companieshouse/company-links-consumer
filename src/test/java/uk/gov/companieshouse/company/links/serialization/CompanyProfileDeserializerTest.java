package uk.gov.companieshouse.company.links.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.companieshouse.api.company.Data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CompanyProfileDeserializerTest {

    public static final String COMPANY_DATA = "company profile data json string";
    @InjectMocks
    private CompanyProfileDeserializer deserialiser;
    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private Data expected;
    @Mock
    private Logger logger;

    @Test
    void shouldDeserialiseOfficerData() throws JsonProcessingException {
        // given
        when(objectMapper.readValue(anyString(), eq(Data.class))).thenReturn(expected);

        // when
        Data actual = deserialiser.deserialiseCompanyData(COMPANY_DATA);

        // then
        assertEquals(expected, actual);
        verify(objectMapper).readValue(COMPANY_DATA, Data.class);
    }

    @Test
    void shouldThrowNonRetryableExceptionWhenJsonProcessingExceptionThrown() throws JsonProcessingException {
        // given
        when(objectMapper.readValue(anyString(), eq(Data.class))).thenThrow(JsonProcessingException.class);

        // when
        Executable executable = () -> deserialiser.deserialiseCompanyData(COMPANY_DATA);

        // then
        NonRetryableErrorException actual = assertThrows(NonRetryableErrorException.class, executable);
        assertEquals("Unable to parse message payload data", actual.getMessage());
        verify(objectMapper).readValue(COMPANY_DATA, Data.class);
    }

}
