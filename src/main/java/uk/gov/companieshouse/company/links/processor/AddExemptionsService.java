package uk.gov.companieshouse.company.links.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.service.AddExemptionsClient;

@Component
public class AddExemptionsService implements ExemptionsService {

    private final AddExemptionsClient client;

    public AddExemptionsService(AddExemptionsClient client) {
        this.client = client;
    }

    @Override
    public void process(String uri) {

    }
}
