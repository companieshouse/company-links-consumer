package uk.gov.companieshouse.company.links.processor;

import static uk.gov.companieshouse.company.links.processor.ResponseHandler.handleResponse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.delta.Charge;
import uk.gov.companieshouse.api.delta.ChargesDelta;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.NonRetryErrorException;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;


@Component
public class ChargesStreamProcessor {

    private final Logger logger;
    private final CompanyProfileService companyProfileService;

    /**
     * Construct an Charges stream processor.
     */
    @Autowired
    public ChargesStreamProcessor(CompanyProfileService companyProfileService,
                                  Logger logger) {
        this.companyProfileService = companyProfileService;
        this.logger = logger;
    }

    /**
     * Process a ResourceChangedData message.
     */
    public void process(Message<ResourceChangedData> resourceChangedMessage)
            throws JsonProcessingException {
        MessageHeaders headers = resourceChangedMessage.getHeaders();

        //TODO need to check where we set this property.
        //TODO We need to create a new one for this processor
        final boolean isRetry = headers.containsKey("CHARGES_DELTA_RETRY_COUNT");
        final ResourceChangedData payload = resourceChangedMessage.getPayload();
        final String logContext = payload.getContextId();
        final Map<String, Object> logMap = new HashMap<>();

        // the resource_id field returned represents the charges record's company number
        final String companyNumber = payload.getResourceId();
        logger.trace(String.format("Resource changed message of kind %s "
                + "for company number %s retrieved", payload.getResourceKind(), companyNumber));

        final ApiResponse<CompanyProfile> response =
                getCompanyProfileApi(logContext, logMap, companyNumber);

        ApiResponse<Void> patchResponse = processCompanyProfileUpdates(logContext, logMap,
                companyNumber, response,
                payload, headers);
        if (patchResponse != null) {
            handleResponse(HttpStatus.valueOf(patchResponse.getStatusCode()), logContext,
                    "Response from PATCH call to company profile api", logMap, logger);
        }
    }

    ApiResponse<Void> processCompanyProfileUpdates(String logContext, Map<String, Object> logMap,
                                      String companyNumber,
                                      ApiResponse<CompanyProfile> response,
                                      ResourceChangedData payload,
                                      MessageHeaders headers)
            throws JsonProcessingException {
        var data = response.getData().getData();
        var links = data.getLinks();

        if (doesCompanyProfileHaveCharges(companyNumber, links)) {
            return null;
        }

        return updateCompanyProfileWithCharges(logContext, logMap, companyNumber, data,
                links, payload, headers);

    }

    ApiResponse<Void> updateCompanyProfileWithCharges(String logContext, Map<String, Object> logMap,
                                         String companyNumber, Data data, Links links,
                                         ResourceChangedData payload,
                                         MessageHeaders headers)
            throws JsonProcessingException {
        logger.trace(String.format("Current company profile with company number %s,"
                        + " does not contain charges link, attaching charges link",
                companyNumber));

        if (links == null) {
            links = new Links();
        }

        links.setCharges(String.format("/company/%s/charges", companyNumber));
        data.setLinks(links);
        var companyProfile = new CompanyProfile();
        companyProfile.setData(data);
        setupHeaders(headers, payload);
        final ApiResponse<Void> patchResponse =
                companyProfileService.patchCompanyProfile(
                        logContext, companyNumber, companyProfile
                );

        logger.trace(String.format("Performing a PATCH with new company profile %s",
                companyProfile));
        return patchResponse;
    }

    boolean doesCompanyProfileHaveCharges(String companyNumber, Links links) {
        if (links != null && links.getCharges() != null) {
            logger.trace(String.format("Company profile with company number %s,"
                            + " already contains charges links, will not perform patch",
                    companyNumber));
            return true;
        }
        return false;
    }

    ApiResponse<CompanyProfile> getCompanyProfileApi(String logContext,
                                                     Map<String, Object> logMap,
                                                     String companyNumber) {
        final ApiResponse<CompanyProfile> response =
                companyProfileService.getCompanyProfile(logContext, companyNumber);
        logger.trace(String.format("Retrieved company profile for company number %s: %s",
                companyNumber, response.getData()));
        handleResponse(HttpStatus.valueOf(response.getStatusCode()), logContext,
                "Response from GET call to company profile api", logMap, logger);
        return response;
    }

    ChargesDelta getChargesRecord(ResourceChangedData payload) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(payload.getData(), ChargesDelta.class);
    }

    String getUpdatedBy(MessageHeaders headers) {
        final String receivedTopic =
                headers.get(KafkaHeaders.RECEIVED_TOPIC).toString();
        final String partition =
                headers.get(KafkaHeaders.RECEIVED_PARTITION_ID).toString();
        final String offset =
                headers.get(KafkaHeaders.OFFSET).toString();

        return String.format("%s-%s-%s", receivedTopic, partition, offset);
    }

    String getDeltaAt(ChargesDelta chargesDelta) {
        if (chargesDelta.getCharges().size() > 0) {
            // assuming we always get only one charge item inside charges delta
            Charge charge = chargesDelta.getCharges().get(0);
            return charge.getDeltaAt();
        } else {
            throw new NonRetryErrorException("No charge item found inside ChargesDelta");
        }

    }

    Map<String, String> setupHeaders(MessageHeaders msgHeaders,
                                     ResourceChangedData payload)
            throws JsonProcessingException {
        Map<String, String> headers = new HashMap<>();
        headers.put("updated_by", getUpdatedBy(msgHeaders));
        String deltaAt = getDeltaAt(getChargesRecord(payload));
        headers.put("delta_at", deltaAt);
        return headers;
    }
}
