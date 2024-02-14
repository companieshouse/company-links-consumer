package uk.gov.companieshouse.company.links.exception;

public class RetryableErrorException extends RuntimeException {

    public RetryableErrorException(String message) {
        super(message);
    }

    public RetryableErrorException(String message, Exception exception) {
        super(message, exception);
    }

    /**
     * Additional constructor to avoid code duplication.
     *
     * @param companyNumber CompanyNumber
     */
    public RetryableErrorException(String companyNumber, String linkType, Exception exception) {
        super(String.format(
                "Company profile [%s] does not exist when processing add %s link request",
                        companyNumber, linkType),
                exception);
    }
}

