package uk.gov.companieshouse.company.links.processor;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.FileCopyUtils;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;

public class TestData {

    public static final String RESOURCE_KIND = "company-charges";
    public static final String TOPIC = "test";
    public static final String PARTITION = "partition_1";
    public static final String OFFSET = "offset_1";
    public static final String CONTEXT_ID = "context_id";
    public static final String MOCK_COMPANY_NUMBER = "03105860";
    public static final String COMPANY_CHARGES_LINK = String.format("/company/%s/charges", MOCK_COMPANY_NUMBER);
    public static final String INVALID_COMPANY_CHARGES_LINK = String.format("/company/%s/metrics", MOCK_COMPANY_NUMBER);
    public static final String RESOURCE_ID = "11223344";

    public Message<ResourceChangedData> createResourceChangedMessageWithDelete() throws IOException {
        return createResourceChangedDeleteMessage(COMPANY_CHARGES_LINK);
    }

    public Message<ResourceChangedData> createResourceChangedMessageWithValidResourceUri() throws IOException {
        return createResourceChangedMessage(COMPANY_CHARGES_LINK);
    }

    public Message<ResourceChangedData> createResourceChangedMessageWithInValidResourceUri() throws IOException {
        return createResourceChangedMessage(INVALID_COMPANY_CHARGES_LINK);
    }

    public Message<ResourceChangedData> createResourceChangedMessage(String resourceUri) throws IOException {
        InputStreamReader exampleChargesJsonPayload = new InputStreamReader(
                Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                        .getResourceAsStream("charges-record.json")));
        String chargesRecord = FileCopyUtils.copyToString(exampleChargesJsonPayload);

        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
                .setContextId(CONTEXT_ID)
                .setResourceId(RESOURCE_ID)
                .setResourceKind(RESOURCE_KIND)
                .setResourceUri(resourceUri)
                .setEvent(new EventRecord())
                .setData(chargesRecord)
                .build();

        return MessageBuilder
                .withPayload(resourceChangedData)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, TOPIC)
                .setHeader(KafkaHeaders.RECEIVED_PARTITION_ID, PARTITION)
                .setHeader(KafkaHeaders.OFFSET, OFFSET)
                .build();
    }

    public Message<ResourceChangedData> createResourceChangedDeleteMessage(String resourceUri) throws IOException {
        InputStreamReader exampleChargesJsonPayload = new InputStreamReader(
            Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                .getResourceAsStream("charges-record.json")));
        String chargesRecord = FileCopyUtils.copyToString(exampleChargesJsonPayload);

        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
            .setContextId(CONTEXT_ID)
            .setResourceId(RESOURCE_ID)
            .setResourceKind(RESOURCE_KIND)
            .setResourceUri(resourceUri)
            .setEvent(new EventRecord(null, "deleted", null))
            .setData(chargesRecord)
            .build();

        return MessageBuilder
            .withPayload(resourceChangedData)
            .setHeader(KafkaHeaders.RECEIVED_TOPIC, TOPIC)
            .setHeader(KafkaHeaders.RECEIVED_PARTITION_ID, PARTITION)
            .setHeader(KafkaHeaders.OFFSET, OFFSET)
            .build();
    }

    public CompanyProfile createCompanyProfile() {
        Data companyProfileData = new Data();
        companyProfileData.setCompanyNumber(MOCK_COMPANY_NUMBER);

        CompanyProfile companyProfile = new CompanyProfile();
        companyProfile.setData(companyProfileData);
        return companyProfile;
    }

    public CompanyProfile createCompanyProfileWithChargesLinks() {
        CompanyProfile companyProfile = createCompanyProfile();
        updateWithLinks(companyProfile);
        return companyProfile;
    }

    private void updateWithLinks(CompanyProfile companyProfile) {
        Links links = new Links();
        links.setCharges(COMPANY_CHARGES_LINK);
        companyProfile.getData().setLinks(links);
    }

}
