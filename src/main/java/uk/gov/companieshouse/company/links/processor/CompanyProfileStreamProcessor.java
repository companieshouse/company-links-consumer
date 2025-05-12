package uk.gov.companieshouse.company.links.processor;

import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import uk.gov.companieshouse.api.appointment.OfficerList;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.exemptions.CompanyExemptions;
import uk.gov.companieshouse.api.exemptions.Exemptions;
import uk.gov.companieshouse.api.filinghistory.FilingHistoryList;
import uk.gov.companieshouse.api.insolvency.CompanyInsolvency;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.model.filinghistory.FilingHistoryApi;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.api.psc.StatementList;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.serialization.CompanyProfileDeserializer;
import uk.gov.companieshouse.company.links.service.AddExemptionsClient;
import uk.gov.companieshouse.company.links.service.AddFilingHistoryClient;
import uk.gov.companieshouse.company.links.service.AddOfficersClient;
import uk.gov.companieshouse.company.links.service.AddPscClient;
import uk.gov.companieshouse.company.links.service.AddStatementsClient;
import uk.gov.companieshouse.company.links.service.ChargesService;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.company.links.service.ExemptionsListClient;
import uk.gov.companieshouse.company.links.service.FilingHistoryService;
import uk.gov.companieshouse.company.links.service.InsolvencyService;
import uk.gov.companieshouse.company.links.service.LinkClient;
import uk.gov.companieshouse.company.links.service.OfficerListClient;
import uk.gov.companieshouse.company.links.service.PscListClient;
import uk.gov.companieshouse.company.links.service.StatementsListClient;
import uk.gov.companieshouse.company.links.type.ApiType;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class CompanyProfileStreamProcessor extends StreamResponseProcessor {

    private final CompanyProfileDeserializer companyProfileDeserializer;
    private final ChargesService chargesService;
    private final CompanyProfileService companyProfileService;
    private final ExemptionsListClient exemptionsListClient;
    private final AddExemptionsClient addExemptionsClient;
    private final FilingHistoryService filingHistoryService;
    private final AddFilingHistoryClient addFilingHistoryClient;
    private final InsolvencyService insolvencyService;
    private final OfficerListClient officerListClient;
    private final AddOfficersClient addOfficersClient;
    private final PscListClient pscListClient;
    private final AddPscClient addPscClient;
    private final StatementsListClient statementsListClient;
    private final AddStatementsClient addStatementsClient;

    /**
     * Construct a Company Profile stream processor.
     */
    @Autowired
    public CompanyProfileStreamProcessor(
            Logger logger, CompanyProfileDeserializer companyProfileDeserializer,
            ChargesService chargesService, CompanyProfileService companyProfileService,
            ExemptionsListClient exemptionsListClient, AddExemptionsClient addExemptionsClient,
            FilingHistoryService filingHistoryService,
                AddFilingHistoryClient addFilingHistoryClient,
            InsolvencyService insolvencyService,
            OfficerListClient officerListClient, AddOfficersClient addOfficersClient,
            PscListClient pscListClient, AddPscClient addPscClient,
            StatementsListClient statementsListClient, AddStatementsClient addStatementsClient) {
        super(logger);
        this.companyProfileDeserializer = companyProfileDeserializer;
        this.chargesService = chargesService;
        this.companyProfileService = companyProfileService;
        this.exemptionsListClient = exemptionsListClient;
        this.addExemptionsClient = addExemptionsClient;
        this.filingHistoryService = filingHistoryService;
        this.addFilingHistoryClient = addFilingHistoryClient;
        this.insolvencyService = insolvencyService;
        this.officerListClient = officerListClient;
        this.addOfficersClient = addOfficersClient;
        this.pscListClient = pscListClient;
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

        int something = 0;
        for (int count = 0; count < 100; count++) {
            something++;
        }

        DataMapHolder.get()
                .companyNumber(companyNumber);
        Data companyProfileData =
                companyProfileDeserializer.deserialiseCompanyData(payload.getData());

        RetryableErrorException retryableLinkException = null;
        NonRetryableErrorException nonRetryableLinkException = null;

        try {
            processChargesLink(contextId, companyNumber, companyProfileData);
        } catch (HttpClientErrorException.Conflict conflictException) {
            nonRetryableLinkException = new NonRetryableErrorException(String.format(
                    "Error retrieving Charges for company number %s", companyNumber),
                    conflictException);
        } catch (Exception exception) {
            retryableLinkException = new RetryableErrorException(String.format(
                    "Error retrieving Charges for company number %s", companyNumber),
                    exception);
        }
        try {
            processExemptionsLink(contextId, companyNumber, companyProfileData);
        } catch (NonRetryableErrorException nonRetryableErrorException) {
            nonRetryableLinkException = new NonRetryableErrorException(String.format(
                    "Error retrieving Exemptions for company number %s", companyNumber),
                    nonRetryableErrorException);
        } catch (Exception exception) {
            retryableLinkException = new RetryableErrorException(String.format(
                    "Error retrieving Exemptions for company number %s", companyNumber),
                    exception);
        }
        try {
            processFilingHistoryLink(contextId, companyNumber, companyProfileData);
        } catch (NonRetryableErrorException nonRetryableErrorException) {
            nonRetryableLinkException = new NonRetryableErrorException(String.format(
                    "Error retrieving Filing History for company number %s", companyNumber),
                    nonRetryableErrorException);
        } catch (Exception exception) {
            retryableLinkException = new RetryableErrorException(String.format(
                    "Error retrieving Filing History for company number %s", companyNumber),
                    exception);
        }
        try {
            processInsolvencyLink(contextId, companyNumber, companyProfileData);
        } catch (HttpClientErrorException.Conflict conflictException) {
            nonRetryableLinkException = new NonRetryableErrorException(String.format(
                    "Error retrieving Insolvency for company number %s", companyNumber),
                    conflictException);
        } catch (Exception exception) {
            retryableLinkException = new RetryableErrorException(String.format(
                    "Error retrieving Insolvency for company number %s", companyNumber),
                    exception);
        }
        try {
            processOfficerLink(contextId, companyNumber, companyProfileData);
        } catch (NonRetryableErrorException nonRetryableErrorException) {
            nonRetryableLinkException = new NonRetryableErrorException(String.format(
                    "Error retrieving Officers for company number %s", companyNumber),
                    nonRetryableErrorException);
        } catch (Exception exception) {
            retryableLinkException = new RetryableErrorException(String.format(
                    "Error retrieving Officers for company number %s", companyNumber),
                    exception);
        }
        try {
            processPscLink(contextId, companyNumber, companyProfileData);
        } catch (NonRetryableErrorException nonRetryableErrorException) {
            nonRetryableLinkException = new NonRetryableErrorException(String.format(
                    "Error retrieving Psc for company number %s", companyNumber),
                    nonRetryableErrorException);
        } catch (Exception exception) {
            retryableLinkException = new RetryableErrorException(String.format(
                    "Error retrieving Psc for company number %s", companyNumber),
                    exception);
        }
        try {
            processPscStatementsLink(contextId, companyNumber, companyProfileData);
        } catch (NonRetryableErrorException nonRetryableErrorException) {
            nonRetryableLinkException = new NonRetryableErrorException(String.format(
                    "Error retrieving Psc Statement for company number %s", companyNumber),
                    nonRetryableErrorException);
        } catch (Exception exception) {
            retryableLinkException = new RetryableErrorException(String.format(
                    "Error retrieving Psc Statement for company number %s", companyNumber),
                    exception);
        }

        if (retryableLinkException != null) {
            throw retryableLinkException;
        } else if (nonRetryableLinkException != null) {
            throw nonRetryableLinkException;
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

                Links links;
                if (data != null) {
                    if (data.getLinks() != null) {
                        links = data.getLinks();
                    } else {
                        links = new Links();
                    }
                } else {
                    data = new Data();
                    links = new Links();
                }
                links.setCharges(String.format("/company/%s/charges", companyNumber));
                data.setLinks(links);
                data.setHasCharges(true);
                var companyProfile = new CompanyProfile();
                companyProfile.setData(data);

                //Note: There is an issue where the Patch request sent by the AddChargesClient to
                // the '/company/*/links/charges' endpoint is being picked up by another service
                // in Cidev. The same happens for Insolvency. Therefore, we are using the
                // old endpoint '/company/*/links'.
                final ApiResponse<Void> patchResponse = companyProfileService.patchCompanyProfile(
                        contextId, companyNumber, companyProfile);
                handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), contextId,
                        "PATCH", ApiType.COMPANY_PROFILE, companyNumber);
            }
        }
    }

    private void processExemptionsLink(String contextId, String companyNumber, Data data) {
        Optional<String> exemptionsLink = Optional.ofNullable(data)
                .map(Data::getLinks)
                .map(Links::getExemptions);

        if (exemptionsLink.isEmpty()) {
            CompanyExemptions companyExemptions;
            try {
                companyExemptions = exemptionsListClient
                        .getExemptionsList(contextId, companyNumber);
            } catch (Exception exception) {
                throw new RetryableErrorException(String.format(
                        "Error retrieving Exemptions for company number %s", companyNumber),
                        exception);
            }

            if (companyExemptions != null && companyExemptions.getExemptions() != null) {
                Exemptions exemptions = companyExemptions.getExemptions();
                if (exemptions.getPscExemptAsTradingOnRegulatedMarket() != null
                        || exemptions.getPscExemptAsSharesAdmittedOnMarket() != null
                        || exemptions.getPscExemptAsTradingOnUkRegulatedMarket() != null
                        || exemptions.getPscExemptAsTradingOnEuRegulatedMarket() != null
                        || exemptions.getDisclosureTransparencyRulesChapterFiveApplies() != null) {
                    addCompanyLink(addExemptionsClient, "Company Exemptions",
                            contextId, companyNumber);
                }
            }
        }
    }

    /**
     * Process the Filing History link for a Company Profile ResourceChanged message.
     * If there is no Filing History link in the ResourceChanged and Filing History records exist
     * then add the link
     */
    private void processFilingHistoryLink(String contextId, String companyNumber, Data data) {
        Optional<String> filingHistoryLink = Optional.ofNullable(data)
                .map(Data::getLinks)
                .map(Links::getFilingHistory);

        if (filingHistoryLink.isEmpty()) {
            FilingHistoryApi filingHistoryResponse;
            try {
                filingHistoryResponse = filingHistoryService
                        .getFilingHistory(contextId, companyNumber);
            } catch (Exception exception) {
                throw new RetryableErrorException(String.format(
                        "Error retrieving Filing History for company number %s", companyNumber),
                        exception);
            }

            if (filingHistoryResponse.getItems() != null
                    && !filingHistoryResponse.getItems().isEmpty()) {
                addCompanyLink(addFilingHistoryClient, "filing history", contextId, companyNumber);
            }
        }
    }

    /**
     * Process the Insolvency link for a Company Profile ResourceChanged message.
     * If there is no Insolvency link in the ResourceChanged and Insolvencies exist
     * then add the link
     */
    private void processInsolvencyLink(String contextId, String companyNumber, Data data) {
        Optional<String> insolvencyLink = Optional.ofNullable(data)
                .map(Data::getLinks)
                .map(Links::getInsolvency);

        if (insolvencyLink.isEmpty()) {
            ApiResponse<CompanyInsolvency> insolvencyResponse;
            try {
                insolvencyResponse = insolvencyService.getInsolvency(contextId, companyNumber);
            } catch (Exception exception) {
                throw new RetryableErrorException(String.format(
                        "Error retrieving Insolvency for company number %s", companyNumber),
                        exception);
            }

            if (insolvencyResponse.getData() != null
                    && !insolvencyResponse.getData().getCases().isEmpty()) {

                Links links;
                if (data != null) {
                    if (data.getLinks() != null) {
                        links = data.getLinks();
                    } else {
                        links = new Links();
                    }
                } else {
                    data = new Data();
                    links = new Links();
                }
                links.setInsolvency(String.format("/company/%s/insolvency", companyNumber));
                data.setLinks(links);
                data.setHasInsolvencyHistory(true);
                var companyProfile = new CompanyProfile();
                companyProfile.setData(data);

                //Note: There is an issue where the Patch request sent by the AddInsolvencyClient to
                // the '/company/*/links/insolvency' endpoint is being picked up by another service
                // in Cidev. The same happens for Charges. Therefore, we are using the
                // old endpoint '/company/*/links'.
                final ApiResponse<Void> patchResponse = companyProfileService.patchCompanyProfile(
                        contextId, companyNumber, companyProfile);
                handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), contextId,
                        "PATCH", ApiType.COMPANY_PROFILE, companyNumber);
            }
        }
    }

    /**
     * Process the Officers link for a Company Profile ResourceChanged message.
     * If there is no Officers link in the ResourceChanged and Officers exist then add the link
     */
    private void processOfficerLink(String contextId, String companyNumber, Data data) {
        Optional<String> officerLink = Optional.ofNullable(data)
                .map(Data::getLinks)
                .map(Links::getOfficers);

        if (officerLink.isEmpty()) {
            PatchLinkRequest patchLinkRequest = new PatchLinkRequest(companyNumber, contextId);
            OfficerList officersList;
            try {
                officersList = officerListClient
                        .getOfficers(patchLinkRequest);

            } catch (Exception exception) {
                throw new RetryableErrorException(String.format(
                        "Error retrieving Officers for company number %s", companyNumber),
                        exception);
            }
            if (officersList != null
                    && !officersList.getItems().isEmpty()) {
                addCompanyLink(addOfficersClient, "officers", contextId, companyNumber);
            }
        }
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
     * If there is no PSC Statements link in the ResourceChanged and PSC statements exist
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
                addCompanyLink(addStatementsClient, "PSC Statements", contextId, companyNumber);
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
        } catch (HttpClientErrorException.Conflict conflictException) {
            throw new NonRetryableErrorException(String.format(
                    "Error updating %s link for company number %s", linkType, companyNumber),
                    conflictException);
        } catch (Exception exception) {
            throw new RetryableErrorException(String.format(
                    "Error updating %s link for company number %s",
                    linkType, companyNumber), exception);
        }
    }
}
