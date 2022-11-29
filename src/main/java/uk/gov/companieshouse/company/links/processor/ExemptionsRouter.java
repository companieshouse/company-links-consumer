package uk.gov.companieshouse.company.links.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.service.ExemptionsService;
import uk.gov.companieshouse.company.links.type.ResourceChange;

@Component
public class ExemptionsRouter implements ExemptionsRoutable {

    private final ExemptionsService addExemptionsService;

    public ExemptionsRouter(ExemptionsService addExemptionsService) {
        this.addExemptionsService = addExemptionsService;
    }

    @Override
    public void route(ResourceChange message) {
        String eventType = message.getData().getEvent().getType();
        if ("changed".equals(eventType)) {
            addExemptionsService.process(message.getData().getResourceUri());
        } else {
            throw new NonRetryableErrorException(String.format("Invalid event type: %s", eventType));
        }
    }
}