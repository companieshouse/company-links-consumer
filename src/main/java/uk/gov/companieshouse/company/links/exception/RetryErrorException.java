package uk.gov.companieshouse.company.links.exception;

public class RetryErrorException extends RuntimeException {
    public RetryErrorException(String message) {
        super(message);
    }
}

