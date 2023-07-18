package uk.gov.companieshouse.company.links.service;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.psc.StatementList;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

@Component
public class DeleteStatementsClient implements LinkClient {

    private final Logger logger;
    private final StatementsListClient statementsListClient;
    private final DeleteStatementsLinkClient deleteStatementsLinkClient;

    /**
     * Constructs a DeleteStatementsClient.
     *
     * @param logger                     Logger
     * @param statementsListClient       StatementsListClient
     * @param deleteStatementsLinkClient DeleteStatementsLinkClient
     */
    public DeleteStatementsClient(Logger logger,
            StatementsListClient statementsListClient,
            DeleteStatementsLinkClient deleteStatementsLinkClient) {
        this.logger = logger;
        this.statementsListClient = statementsListClient;
        this.deleteStatementsLinkClient = deleteStatementsLinkClient;
    }

    /**
     * Sends a patch request to the remove officers link endpoint in the company profile api and
     * handles any error responses.
     *
     * @param linkRequest PatchLinkRequest
     */
    @Override
    public void patchLink(PatchLinkRequest linkRequest) {
        StatementList statementList = statementsListClient.getStatementsList(
                linkRequest.getCompanyNumber());
        if (statementList.getItems().isEmpty()) {
            deleteStatementsLinkClient.patchLink(linkRequest);
        } else {
            if (statementList.getItems().stream()
                    .anyMatch(officerSummary -> officerSummary.getLinks().getSelf()
                            .endsWith(linkRequest.getResourceId()))) {
                throw new RetryableErrorException(String.format("Statement with id: %s not "
                        + "deleted", linkRequest.getResourceId()));
            } else {
                logger.debug(String.format("Statements for company number [%s] still exist",
                        linkRequest.getCompanyNumber()), DataMapHolder.getLogMap());
            }
        }
    }
}

