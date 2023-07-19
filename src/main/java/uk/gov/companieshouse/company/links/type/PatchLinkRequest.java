package uk.gov.companieshouse.company.links.type;

public class PatchLinkRequest {

    private final String companyNumber;
    private final String resourceId;
    private final String requestId;

    /**
     * Constructor.
     *
     * @param companyNumber The company to patch
     * @param resourceId The resource ID to patch
     * @param requestId The correlation ID of the request
     */
    public PatchLinkRequest(String companyNumber, String resourceId, String requestId) {
        this.companyNumber = companyNumber;
        this.resourceId = resourceId;
        this.requestId = requestId;
    }

    public PatchLinkRequest(String companyNumber, String requestId) {
        this(companyNumber, requestId, null);
    }

    public String getCompanyNumber() {
        return companyNumber;
    }

    public String getResourceId() {
        return resourceId;
    }

    public String getRequestId() {
        return requestId;
    }
}
