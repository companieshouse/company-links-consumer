package uk.gov.companieshouse.company.links.serialization;

import java.util.Arrays;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.stream.ResourceChangedData;

//Confirmed that charges and insolvency will have same avro schema
@Component
public class ResourceChangedDataDeserializer implements Deserializer<ResourceChangedData> {

    /**
     * deserialize.
     */
    @Override
    public ResourceChangedData deserialize(String topic, byte[] data) {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            DatumReader<ResourceChangedData> reader =
                    new ReflectDatumReader<>(ResourceChangedData.class);
            return reader.read(null, decoder);
        } catch (Exception ex) {
            throw new SerializationException(
                    "Message data [" + Arrays.toString(data)
                            + "] from topic ["
                            + topic
                            + "] cannot be deserialized", ex);
        }
    }

}
