package uk.gov.companieshouse.company.links.tranformer;

import org.springframework.stereotype.Component;


//TODO check whether we need this transformer or not
@Component
public class InsolvencyApiTransformer {

    /**
     * Transform.
     */
    public String transform(String input) {
        // TODO: Use mapStruct to transform json object to Open API generated object
        // avro to json transformation
        return input;
    }
}
