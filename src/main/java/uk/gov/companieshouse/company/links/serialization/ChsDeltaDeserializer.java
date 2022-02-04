package uk.gov.companieshouse.company.links.serialization;

import java.util.Arrays;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.delta.ChsDelta;

//TODO Do we need separate deserializer one for charges and for insolvancy
@Component
public class ChsDeltaDeserializer implements Deserializer<ChsDelta> {

    /**
     * deserialize.
     */
    @Override
    public ChsDelta deserialize(String topic, byte[] data) {
        try {
            // TODO: Zahid: Use AvroDeserializer after adding util method in ch-kafka
            Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            DatumReader<ChsDelta> reader = new ReflectDatumReader<>(ChsDelta.class);
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
