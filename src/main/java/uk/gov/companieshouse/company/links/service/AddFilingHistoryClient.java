package uk.gov.companieshouse.company.links.service;

import java.util.function.Supplier;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.api.InternalApiClient;
import uk.gov.companieshouse.api.error.ApiErrorResponseException;
import uk.gov.companieshouse.api.handler.exception.URIValidationException;
import uk.gov.companieshouse.company.links.type.PatchLinkRequest;
import uk.gov.companieshouse.company.links.util.ResponseHandler;

@Component("addFilingHistoryClient")
public class AddFilingHistoryClient implements LinkClient {

    private final Supplier<InternalApiClient> internalApiClientFactory;
    private final ResponseHandler responseHandler;

    private static final String LINK_TYPE = "filing history";

    public AddFilingHistoryClient(Supplier<InternalApiClient> internalApiClientFactory,
                                  ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
        this.internalApiClientFactory = internalApiClientFactory;
    }

    /**
     * Sends a patch request to the add filing_history link endpoint in the company profile api and
     * handles any error responses.
     *
     * @param linkRequest PatchLinkRequest
     */
    @Override
    public void patchLink(PatchLinkRequest linkRequest) {
        InternalApiClient client = internalApiClientFactory.get();
        client.getHttpClient().setRequestId(linkRequest.getRequestId());
        try {
            client.privateCompanyLinksResourceHandler()
                    .addFilingHistoryLink(
                            String.format("/company/%s/links/filing-history",
                                    linkRequest.getCompanyNumber()))
                    .execute();
        } catch (ApiErrorResponseException ex) {
            responseHandler.handle(ex.getStatusCode(), LINK_TYPE, ex);
        } catch (IllegalArgumentException ex) {
            responseHandler.handle(ex);
        } catch (URIValidationException ex) {
            responseHandler.handle(linkRequest.getCompanyNumber(), ex);
        }
    }
}
