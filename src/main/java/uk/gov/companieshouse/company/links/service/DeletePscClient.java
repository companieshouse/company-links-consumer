package uk.gov.companieshouse.company.links.service;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

@Component("deletePscClient")
public class DeletePscClient implements LinkClient {

    private final Logger logger;
    private final PscListClient pscListClient;
    private final DeletePscLinkClient deletePscLinkClient;

    /**
     * Constructs a DeletePscClient.
     *
     * @param logger                   Logger
     * @param pscListClient   PscListClient
     * @param deletePscLinkClient DeletePscLinkClient
     */
    public DeletePscClient(Logger logger, PscListClient pscListClient,
                           DeletePscLinkClient deletePscLinkClient) {
        this.logger = logger;
        this.pscListClient = pscListClient;
        this.deletePscLinkClient = deletePscLinkClient;
    }

    /**
     * Sends a patch request to the remove psc link endpoint in the company profile api and
     * handles any error responses.
     *
     * @param linkRequest PatchLinkRequest
     */
    @Override
    public void patchLink(PatchLinkRequest linkRequest) {
        PscList pscList = pscListClient.getPscs(linkRequest);
        if (pscList.getTotalResults() == 0) {
            deletePscLinkClient.patchLink(linkRequest);
        } else {
            if (pscList.getItems().stream()
                    .anyMatch(officerSummary -> officerSummary.getLinks().toString()
                            .endsWith(linkRequest.getResourceId()))) {
                throw new RetryableErrorException(String.format("Psc with id: %s not "
                        + "deleted", linkRequest.getResourceId()));
            } else {
                logger.debug(String.format("Psc for company number [%s] still exist",
                        linkRequest.getCompanyNumber()));
            }
        }
    }
}
