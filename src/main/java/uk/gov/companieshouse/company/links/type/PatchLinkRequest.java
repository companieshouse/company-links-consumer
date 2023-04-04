package uk.gov.companieshouse.company.links.type;

public class PatchLinkRequest {
    private final String companyNumber;
    private final String resourceId;

    public PatchLinkRequest(String companyNumber, String resourceId) {
        this.companyNumber = companyNumber;
        this.resourceId = resourceId;
    }

    public PatchLinkRequest(String companyNumber) {
        this.companyNumber = companyNumber;
        this.resourceId = null;
    }

    public String getCompanyNumber() {
        return companyNumber;
    }

    public String getResourceId() {
        return resourceId;
    }
}
