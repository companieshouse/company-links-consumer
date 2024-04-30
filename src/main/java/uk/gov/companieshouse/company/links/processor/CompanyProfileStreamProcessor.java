package uk.gov.companieshouse.company.links.processor;

import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.company.links.exception.RetryableErrorException;
import uk.gov.companieshouse.company.links.logging.DataMapHolder;
import uk.gov.companieshouse.company.links.serialization.CompanyProfileDeserializer;
import uk.gov.companieshouse.company.links.service.AddPscClient;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.company.links.service.PscService;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class CompanyProfileStreamProcessor extends StreamResponseProcessor {

    private final CompanyProfileService companyProfileService;
    private final PscService pscService;
    private final CompanyProfileDeserializer companyProfileDeserializer;
    private final AddPscClient addPscClient;

    /**
     * Construct a Company Profile stream processor.
     */
    @Autowired
    public CompanyProfileStreamProcessor(CompanyProfileService companyProfileService,
                                         PscService pscService, Logger logger,
                                         AddPscClient addPscClient,
                                         CompanyProfileDeserializer companyProfileDeserializer) {
        super(logger);
        this.companyProfileService = companyProfileService;
        this.pscService = pscService;
        this.addPscClient = addPscClient;
        this.companyProfileDeserializer = companyProfileDeserializer;
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
            ApiResponse<PscList> pscApiResponse = pscService
                    .getPscList(contextId, companyNumber);
            HttpStatus httpStatus = HttpStatus.resolve(pscApiResponse.getStatusCode());

            if (httpStatus == null || !httpStatus.is2xxSuccessful()) {
                throw new RetryableErrorException(String.format(
                        "Resource not found for PSCs List for company number %s"
                                + "and contextId %s", companyNumber, contextId));
            }
            if (pscApiResponse.getData() != null
                    && pscApiResponse.getData().getTotalResults() != null
                    && pscApiResponse.getData().getTotalResults() > 0) {
                addCompanyPscsLink(contextId, companyNumber, contextId);
            }
        }
    }

    private void addCompanyPscsLink(String logContext, String companyNumber, String contextId) {
        logger.trace(String.format("Message with contextId %s and company number %s -"
                        + "company profile does not contain PSC link, attaching PSC link",
                logContext, companyNumber), DataMapHolder.getLogMap());

        PatchLinkRequest linkRequest = new PatchLinkRequest(companyNumber, "pscs", contextId);

        addPscClient.patchLink(linkRequest);
    }
}
