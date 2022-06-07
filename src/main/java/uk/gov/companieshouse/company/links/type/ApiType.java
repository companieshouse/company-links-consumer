package uk.gov.companieshouse.company.links.type;

public enum ApiType {
    COMPANY_PROFILE("company-profile-api"),
    INSOLVENCY("insolvency-data-api"),
    CHARGES("charges-data-api");

    private final String apiName;

    ApiType(final String apiName) {
        this.apiName = apiName;
    }

    @Override
    public String toString() {
        return apiName;
    }
}