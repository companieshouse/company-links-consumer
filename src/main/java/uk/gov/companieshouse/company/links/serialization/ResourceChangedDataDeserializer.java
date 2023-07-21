package uk.gov.companieshouse.company.links.serialization;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class ResourceChangedDataDeserializer implements Deserializer<ResourceChangedData> {

    private final Logger logger;

    @Autowired
    public ResourceChangedDataDeserializer(Logger logger) {
        this.logger = logger;
    }

    /**
     * deserialize.
     */
    @Override
    public ResourceChangedData deserialize(String topic, byte[] data) {
        try {
            logger.trace(String.format("DSND-374 and DSND-604: Message picked up from topic "
                    + "with data: %s", new String(data)), DataMapHolder.getLogMap());

            Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            DatumReader<ResourceChangedData> reader =
                    new ReflectDatumReader<>(ResourceChangedData.class);
            return reader.read(null, decoder);
        } catch (Exception ex) {
            logger.error("Serialization exception while converting to Avro schema object", ex,
                    DataMapHolder.getLogMap());
            throw new NonRetryableErrorException("De-Serialization exception "
                    + "while converting to Avro schema object", ex);
        }
    }

}
