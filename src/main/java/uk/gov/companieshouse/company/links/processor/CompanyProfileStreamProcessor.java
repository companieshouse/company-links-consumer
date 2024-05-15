package uk.gov.companieshouse.company.links.processor;

import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.api.psc.StatementList;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.serialization.CompanyProfileDeserializer;
import uk.gov.companieshouse.company.links.service.AddChargesClient;
import uk.gov.companieshouse.company.links.service.AddPscClient;
import uk.gov.companieshouse.company.links.service.AddStatementsClient;
import uk.gov.companieshouse.company.links.service.ChargesService;
import uk.gov.companieshouse.company.links.service.LinkClient;
import uk.gov.companieshouse.company.links.service.PscListClient;
import uk.gov.companieshouse.company.links.service.StatementsListClient;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class CompanyProfileStreamProcessor extends StreamResponseProcessor {

    private final CompanyProfileDeserializer companyProfileDeserializer;
    private final ChargesService chargesService;
    private final PscListClient pscListClient;
    private final StatementsListClient statementsListClient;
    private final AddChargesClient addChargesClient;
    private final AddPscClient addPscClient;
    private final AddStatementsClient addStatementsClient;

    /**
     * Construct a Company Profile stream processor.
     */
    @Autowired
    public CompanyProfileStreamProcessor(
            Logger logger, CompanyProfileDeserializer companyProfileDeserializer,
            ChargesService chargesService, PscListClient pscListClient,
            AddChargesClient addChargesClient, AddPscClient addPscClient,
            StatementsListClient statementsListClient, AddStatementsClient addStatementsClient) {
        super(logger);
        this.companyProfileDeserializer = companyProfileDeserializer;
        this.chargesService = chargesService;
        this.pscListClient = pscListClient;
        this.addChargesClient = addChargesClient;
        this.addPscClient = addPscClient;
        this.statementsListClient = statementsListClient;
        this.addStatementsClient = addStatementsClient;
    }

    /**
     * Process a ResourceChangedData message.
     */
    public void processDelta(Message<ResourceChangedData> resourceChangedMessage) {
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String contextId = payload.getContextId();
        final String companyNumber = payload.getResourceId();
        DataMapHolder.get()
                .companyNumber(companyNumber);
        Data companyProfileData =
                companyProfileDeserializer.deserialiseCompanyData(payload.getData());

        processChargesLink(contextId, companyNumber, companyProfileData);

        processPscLink(contextId, companyNumber, companyProfileData);
    }

    /**
     * Process the PSCs link for a Company Profile ResourceChanged message.
     * If there is no PSCs link in the ResourceChanged and PSCs exist then add the link
     */
    private void processPscLink(String contextId, String companyNumber, Data data) {
        Optional<String> pscLink = Optional.ofNullable(data)
                .map(Data::getLinks)
                .map(Links::getPersonsWithSignificantControl);

        if (pscLink.isEmpty()) {
            PatchLinkRequest patchLinkRequest = new PatchLinkRequest(companyNumber, contextId);
            PscList pscList;
            try {
                pscList = pscListClient.getPscs(patchLinkRequest);
            } catch (Exception exception) {
                throw new RetryableErrorException(String.format(
                        "Error retrieving PSCs for company number %s", companyNumber),
                        exception);
            }

            if (pscList != null
                    && !pscList.getItems().isEmpty()) {
                addCompanyLink(addPscClient, "PSC", contextId, companyNumber);
            }
        }
    }

    /**
     * Process the PSC Statements link for a Company Profile ResourceChanged message.
     * If there is no PSC Statements link in the ResourceChanged and a PSC statement exists
     * then add the link.
     */
    private void processPscStatementsLink(String contextId, String companyNumber, Data data) {
        Optional<String> pscStatementsLink = Optional.ofNullable(data)
                .map(Data::getLinks)
                .map(Links::getPersonsWithSignificantControlStatements);

        if (pscStatementsLink.isEmpty()) {
            StatementList statementList;
            try {
                statementList = statementsListClient.getStatementsList(companyNumber, contextId);
            } catch (Exception exception) {
                throw new RetryableErrorException(String.format(
                        "Error retrieving Statements for company number %s", companyNumber),
                        exception);
            }

            if (statementList != null && !statementList.getItems().isEmpty()) {
                addStatementsLink(addStatementsClient, "PSC Statements", contextId, companyNumber);
            }
        }
    }

    /**
     * Process the Charges link for a Company Profile ResourceChanged message.
     * If there is no Charges link in the ResourceChanged and Charges exist then add the link
     */
    private void processChargesLink(String contextId, String companyNumber, Data data) {
        Optional<String> chargesLink = Optional.ofNullable(data)
                .map(Data::getLinks)
                .map(Links::getCharges);

        if (chargesLink.isEmpty()) {
            ApiResponse<ChargesApi> chargesResponse;
            try {
                chargesResponse = chargesService.getCharges(contextId, companyNumber);
            } catch (Exception exception) {
                throw new RetryableErrorException(String.format(
                        "Error retrieving Charges for company number %s", companyNumber),
                        exception);
            }

            if (chargesResponse.getData() != null
                    && !chargesResponse.getData().getItems().isEmpty()) {
                addCompanyLink(addChargesClient, "Charges", contextId, companyNumber);
            }
        }
    }

    private void addCompanyLink(LinkClient linkClient, String linkType, String contextId,
                                String companyNumber) {
        try {
            logger.trace(String.format("Message with contextId %s and company number %s -"
                                    + "company profile does not contain %s link, attaching link",
                            contextId, companyNumber, linkType), DataMapHolder.getLogMap());

            PatchLinkRequest linkRequest = new PatchLinkRequest(companyNumber, contextId);

            linkClient.patchLink(linkRequest);
        } catch (Exception exception) {
            throw new RetryableErrorException(String.format(
                    "Error updating %s link for company number %s",
                    linkType, companyNumber), exception);
        }
    }

    private void addStatementsLink(LinkClient linkClient, String linkType, String contextId,
                                   String companyNumber) {
        try {
            logger.trace(String.format("Message with contextId %s and company number %s "
                    + "company profile does not contain %s link, attaching link",
                    contextId, companyNumber, linkType), DataMapHolder.getLogMap());

            PatchLinkRequest linkRequest = new PatchLinkRequest(companyNumber, contextId);

            linkClient.patchLink(linkRequest);
        } catch (Exception exception) {
            throw new RetryableErrorException(String.format(
                    "Error updating %s link for company number %s",
                    linkType, companyNumber), exception);
        }
    }
}
