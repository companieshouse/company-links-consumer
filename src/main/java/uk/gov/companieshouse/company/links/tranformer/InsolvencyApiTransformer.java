package uk.gov.companieshouse.company.links.tranformer;

import org.springframework.stereotype.Component;

//TODO check whether we need this transformer or not
@Component
public class InsolvencyApiTransformer {

    public String transform(String input) {
        // TODO: Use mapStruct to tranform json object to Open API generated object  //avro to json transformation
        return input;
    }
}
