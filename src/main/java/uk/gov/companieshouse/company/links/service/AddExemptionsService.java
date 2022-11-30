package uk.gov.companieshouse.company.links.service;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;
import uk.gov.companieshouse.logging.Logger;

@Component
public class AddExemptionsService implements ExemptionsService {
    private static final String EXTRACT_COMPANY_NUMBER_PATTERN =
            "(?<=company/)(.*\\d)(?=/exemptions)";

    private final AddExemptionsClient client;

    private final Logger logger;

    public AddExemptionsService(AddExemptionsClient client, Logger logger) {
        this.client = client;
        this.logger = logger;
    }

    @Override
    public void process(String uri) {
        if (StringUtils.isBlank(uri)) {
            logger.error("Could not extract company number from empty or null resource uri");
            throw new NonRetryableErrorException(
                    "Could not extract company number from empty or null resource uri");
        }
        //matches all characters between company/ and /
        Pattern companyNo = Pattern.compile(EXTRACT_COMPANY_NUMBER_PATTERN);
        Matcher matcher = companyNo.matcher(uri);
        if (matcher.find()) {
            client.addExemptionsLink(
                    String.format("/company/%s/links/exemptions", matcher.group()));
        } else {
            logger.error(String.format("Could not extract company number from uri "
                    + "%s ", uri));
            throw new NonRetryableErrorException(
                    String.format("Could not extract company number from resource URI: %s", uri));
        }
    }
}