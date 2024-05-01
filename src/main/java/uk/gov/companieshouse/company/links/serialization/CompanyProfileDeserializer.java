package uk.gov.companieshouse.company.links.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;

@Component
public class CompanyProfileDeserializer {

    private final Logger logger;

    private final ObjectMapper objectMapper;


    CompanyProfileDeserializer(ObjectMapper objectMapper, Logger logger) {
        this.objectMapper = objectMapper;
        this.logger = logger;
    }

    /**
     * deserialize.
     */
    public Data deserialiseCompanyData(String data) {
        try {
            return objectMapper.readValue(data, Data.class);
        } catch (JsonProcessingException exception) {
            logger.errorContext("Unable to parse message payload data", exception,
                    DataMapHolder.getLogMap());
            throw new NonRetryableErrorException("Unable to parse message payload data", exception);
        }
    }
}
