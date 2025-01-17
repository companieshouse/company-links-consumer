package uk.gov.companieshouse.company.links.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.FileCopyUtils;
import uk.gov.companieshouse.api.appointment.OfficerList;
import uk.gov.companieshouse.api.charges.ChargesApi;
import uk.gov.companieshouse.api.company.CompanyProfile;
import uk.gov.companieshouse.api.company.Data;
import uk.gov.companieshouse.api.company.Links;
import uk.gov.companieshouse.api.exemptions.CompanyExemptions;
import uk.gov.companieshouse.api.insolvency.CompanyInsolvency;
import uk.gov.companieshouse.api.model.filinghistory.FilingHistoryApi;
import uk.gov.companieshouse.api.psc.PscList;
import uk.gov.companieshouse.api.psc.StatementList;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.FileInputStream;
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
    public static final String COMPANY_CHARGES_LINK = String.format("/company/%s/charges/123", MOCK_COMPANY_NUMBER);
    public static final String ALL_COMPANY_CHARGES_LINK = String.format("/company/%s/charges", MOCK_COMPANY_NUMBER);
    public static final String INVALID_COMPANY_CHARGES_LINK = String.format("/company/%s/metrics", MOCK_COMPANY_NUMBER);
    public static final String RESOURCE_ID = "11223344";
    public static final String COMPANY_PROFILE_LINK = String.format("/company/%s", MOCK_COMPANY_NUMBER);


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
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, PARTITION)
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
            .setHeader(KafkaHeaders.RECEIVED_PARTITION, PARTITION)
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

    public Message<ResourceChangedData> createCompanyProfileWithLinksMessageWithValidResourceUri() throws IOException {
        InputStreamReader exampleCompanyProfileJsonPayload = new InputStreamReader(
                Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                        .getResourceAsStream("company-profile-record-with-all-links.json")));
        String companyProfileRecord = FileCopyUtils.copyToString(exampleCompanyProfileJsonPayload);

        EventRecord changedEvent = new EventRecord();
        changedEvent.setType("changed");

        ResourceChangedData resourceChangedData = ResourceChangedData.newBuilder()
                .setContextId(CONTEXT_ID)
                .setResourceId(MOCK_COMPANY_NUMBER)
                .setResourceKind("company-profile")
                .setResourceUri(COMPANY_PROFILE_LINK)
                .setEvent(changedEvent)
                .setData(companyProfileRecord)
                .build();

        return MessageBuilder
                .withPayload(resourceChangedData)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, TOPIC)
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, PARTITION)
                .setHeader(KafkaHeaders.OFFSET, OFFSET)
                .build();
    }

    public Data createCompanyProfileWithLinksFromJson() throws IOException {
        String data = FileCopyUtils.copyToString(new InputStreamReader(
                new FileInputStream("src/test/resources/company-profile-record-with-all-links.json")));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        return objectMapper.readValue(data, Data.class);
    }

    public ChargesApi createCharges() throws IOException {
        String data = FileCopyUtils.copyToString(new InputStreamReader(
                new FileInputStream("src/test/resources/charges-api-record.json")));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        return objectMapper.readValue(data, ChargesApi.class);
    }

    public CompanyExemptions createExemptions() throws IOException {
        String data = FileCopyUtils.copyToString(new InputStreamReader(
                new FileInputStream("src/test/resources/exemptions-list-record.json")));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        return objectMapper.readValue(data, CompanyExemptions.class);
    }

    public FilingHistoryApi createFilingHistoryApi() throws IOException {
        String data = FileCopyUtils.copyToString(new InputStreamReader(
                new FileInputStream("src/test/resources/filing-history-list-record.json")));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        return objectMapper.readValue(data, FilingHistoryApi.class);
    }

    public CompanyInsolvency createInsolvency() throws IOException {
        String data = FileCopyUtils.copyToString(new InputStreamReader(
                new FileInputStream("src/test/resources/insolvency-api-record.json")));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        return objectMapper.readValue(data, CompanyInsolvency.class);
    }

    public OfficerList createOfficers() throws IOException {
        String data = FileCopyUtils.copyToString(new InputStreamReader(
                new FileInputStream("src/test/resources/officers-record.json")));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        return objectMapper.readValue(data, OfficerList.class);
    }

    public PscList createPscList() throws IOException {
        String data = FileCopyUtils.copyToString(new InputStreamReader(
                new FileInputStream("src/test/resources/psc-list-record.json")));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        return objectMapper.readValue(data, PscList.class);
    }

    public StatementList createStatementList() throws IOException {
        String data = FileCopyUtils.copyToString(new InputStreamReader(
                new FileInputStream("src/test/resources/psc-statements-list-record.json")));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        return objectMapper.readValue(data, StatementList.class);
    }
}
