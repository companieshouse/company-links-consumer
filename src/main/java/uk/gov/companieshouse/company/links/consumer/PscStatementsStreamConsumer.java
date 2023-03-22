package uk.gov.companieshouse.company.links.consumer;

import org.springframework.kafka.annotation.RetryableTopic;

import uk.gov.companieshouse.company.links.processor.LinkRouter;
import uk.gov.companieshouse.logging.Logger;

public class PscStatementsStreamConsumer {

    private final Logger logger;
    private final LinkRouter statementsRouter;

    public PscStatementsStreamConsumer(Logger logger, LinkRouter statementsRouter) {
        this.logger = logger;
        this.statementsRouter = statementsRouter;
    }

    /**
     * Receives Main topic messages.
     */
}
