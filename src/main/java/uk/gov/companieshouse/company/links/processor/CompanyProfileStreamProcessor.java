package uk.gov.companieshouse.company.links.processor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.service.CompanyProfileService;
import uk.gov.companieshouse.logging.Logger;

@Component
public class CompanyProfileStreamProcessor extends StreamResponseProcessor {

    private final CompanyProfileService companyProfileService;

    @Autowired
    public CompanyProfileStreamProcessor(CompanyProfileService companyProfileService,
                                         Logger logger) {
        super(logger);
        this.companyProfileService = companyProfileService;
    }
}
