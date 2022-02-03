package uk.gov.companieshouse.company.links.exception;

public class NonRetryErrorException extends RuntimeException {
    public NonRetryErrorException(String message) {
        super(message);
    }
}

