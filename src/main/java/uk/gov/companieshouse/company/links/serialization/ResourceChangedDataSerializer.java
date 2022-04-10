package uk.gov.companieshouse.company.links.serialization;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.kafka.exceptions.SerializationException;
import uk.gov.companieshouse.kafka.serialization.AvroSerializer;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class ResourceChangedDataSerializer implements Serializer<ResourceChangedData> {

    private final Logger logger;

    @Autowired
    public ResourceChangedDataSerializer(Logger logger) {
        this.logger = logger;
    }

    @Override
    public byte[] serialize(String var1, ResourceChangedData resourceChangedData) {
        try {
            if (resourceChangedData == null) {
                return null;
            }

            DatumWriter<ResourceChangedData> writer = new SpecificDatumWriter<>();
            var encoderFactory = EncoderFactory.get();

            AvroSerializer<ResourceChangedData> avroSerializer = new AvroSerializer<>(writer,
                    encoderFactory);

            return avroSerializer.toBinary(resourceChangedData);
        } catch (SerializationException ex) {
            throw new RuntimeException(ex);
        }
    }
}
