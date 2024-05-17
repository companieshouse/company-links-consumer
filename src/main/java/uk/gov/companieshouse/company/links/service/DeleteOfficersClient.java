package uk.gov.companieshouse.company.links.service;

import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.appointment.OfficerList;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;

@Component
public class DeleteOfficersClient implements LinkClient {

    private final Logger logger;
    private final OfficerListClient officerListClient;
    private final DeleteOfficersLinkClient deleteOfficersLinkClient;

    /**
     * Constructs a RemoveOfficersClient.
     *
     * @param logger                    Logger
     * @param officerListClient    OfficerListClient
     * @param deleteOfficersLinkClient  DeleteOfficersLinkClient

     */
    public DeleteOfficersClient(Logger logger,
                                OfficerListClient officerListClient,
                                DeleteOfficersLinkClient deleteOfficersLinkClient) {
        this.logger = logger;
        this.officerListClient = officerListClient;
        this.deleteOfficersLinkClient = deleteOfficersLinkClient;
    }

    /**
     * Sends a patch request to the remove officers link endpoint in the company profile api and
     * handles any error responses.
     *
     * @param linkRequest PatchLinkRequest
     */
    @Override
    public void patchLink(PatchLinkRequest linkRequest) {
        OfficerList officerList = officerListClient.getOfficers(linkRequest);
        if (officerList.getTotalResults() == 0) {
            deleteOfficersLinkClient.patchLink(linkRequest);
        } else {
            if (officerList.getItems().stream()
                    .anyMatch(officerSummary -> officerSummary.getLinks().getSelf()
                            .endsWith(linkRequest.getResourceId()))) {
                throw new RetryableErrorException(String.format("Officer with id: %s is still not "
                        + "deleted", linkRequest.getResourceId()));
            } else {
                logger.debug(String.format("Officers for company number [%s] still exist",
                        linkRequest.getCompanyNumber()), DataMapHolder.getLogMap());
            }
        }
    }
}
