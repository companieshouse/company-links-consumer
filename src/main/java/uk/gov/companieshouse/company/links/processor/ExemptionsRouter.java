package uk.gov.companieshouse.company.links.processor;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.service.ExemptionsService;
import uk.gov.companieshouse.company.links.type.ResourceChange;

@Component
public class ExemptionsRouter implements ExemptionsRoutable {

    private final ExemptionsService addExemptionsService;
    private final ExemptionsService deleteExemptionsService;

    public ExemptionsRouter(ExemptionsService addExemptionsService,
                            ExemptionsService deleteExemptionsService) {
        this.addExemptionsService = addExemptionsService;
        this.deleteExemptionsService = deleteExemptionsService;
    }

    @Override
    public void route(ResourceChange message) {
        String eventType = message.getData().getEvent().getType();
        if ("changed".equals(eventType)) {
            addExemptionsService.process(message.getData().getResourceUri());
        } else if ("deleted".equals(eventType)) {
            deleteExemptionsService.process(message.getData().getResourceUri());
        } else {
            throw new NonRetryableErrorException(
                    String.format("Invalid event type: %s", eventType));
        }
    }
}