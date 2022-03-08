package uk.gov.companieshouse.company.links.processor;

import static uk.gov.companieshouse.company.links.processor.ResponseHandler.handleResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.delta.Insolvency;
import uk.gov.companieshouse.api.delta.InsolvencyDelta;
import uk.gov.companieshouse.api.model.ApiResponse;
import uk.gov.companieshouse.company.links.exception.RetryErrorException;
import uk.gov.companieshouse.company.links.producer.InsolvencyStreamProducer;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.delta.ChsDelta;
import uk.gov.companieshouse.logging.Logger;


@Component
public class InsolvencyStreamProcessor {

    private final Logger logger;
    private final InsolvencyStreamProducer insolvencyStreamProducer;
    private final CompanyProfileService companyProfileService;

    /**
     * Construct an insolvency stream processor.
     */
    @Autowired
    public InsolvencyStreamProcessor(InsolvencyStreamProducer insolvencyStreamProducer,
                                     CompanyProfileService companyProfileService,
                                     Logger logger) {
        this.insolvencyStreamProducer = insolvencyStreamProducer;
        this.companyProfileService = companyProfileService;
        this.logger = logger;
    }

    /**
     * Process CHS Delta message.
     */
    //TODO What model should we use here? Is it a Avro?
    public void process(Message<ChsDelta> chsDelta) {
        try {
            MessageHeaders headers = chsDelta.getHeaders();
            final String receivedTopic =
                    Objects.requireNonNull(headers.get(KafkaHeaders.RECEIVED_TOPIC)).toString();
            //TODO need to check where we set this property.
            //TODO We need to create a new one for this processor
            final boolean isRetry = headers.containsKey("INSOLVENCY_DELTA_RETRY_COUNT");
            final ChsDelta payload = chsDelta.getPayload();
            final String logContext = payload.getContextId();
            final Map<String, Object> logMap = new HashMap<>();

            ObjectMapper mapper = new ObjectMapper();
            InsolvencyDelta insolvencyDelta = mapper.readValue(payload.getData(),
                    InsolvencyDelta.class);
            logger.trace(String.format("InsolvencyDelta extracted "
                    + "from Kafka message: %s", insolvencyDelta));

            // We always receive only one insolvency per delta from CHIPS
            Insolvency insolvency = insolvencyDelta.getInsolvency().get(0);
            final String companyNumber = insolvency.getCompanyNumber();

            final ApiResponse<CompanyProfile> response =
                    companyProfileService.getCompanyProfile(logContext, companyNumber);
            logger.trace(String.format("Retrieved company profile for company number %s: %s",
                    companyNumber, response.getData()));
            handleResponse(null, HttpStatus.valueOf(response.getStatusCode()), logContext,
                    "Response from GET call to company profile api", logMap, logger);

            // TODO check if company needs updating - use response.getData()
            // TODO PATCH company profile
        } catch (RetryErrorException ex) {
            retry(chsDelta);
        } catch (Exception ex) {
            handleErrors(chsDelta);
            // send to error topic
        }
    }

    public void retry(Message<ChsDelta> chsDelta) {

    }

    private void handleErrors(Message<ChsDelta> chsDelta) {

    }
}
