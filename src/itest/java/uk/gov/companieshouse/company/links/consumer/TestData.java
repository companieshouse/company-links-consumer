package uk.gov.companieshouse.company.links.consumer;

import org.springframework.util.FileCopyUtils;
import uk.gov.companieshouse.stream.EventRecord;
import uk.gov.companieshouse.stream.ResourceChangedData;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Objects;

public class TestData {

    public static final String CHANGED = "changed";
    public static final String DELETED = "deleted";
    public static final String CONTEXT_ID = "context_id";
    public static final String RESOURCE_ID = "11223344";
    public static final String RESOURCE_KIND_CHARGES = "company-charges";
    public static final String RESOURCE_KIND_INSOLVENCY = "company-insolvency";
    public static final String CHARGES_RESOURCE_URI = "/company/12345678/charges";
    public static final String INSOLVENCY_RESOURCE_URI = "/company/12345678/insolvency";
    public static final String EVENT_TYPE_DELETE = "deleted";

    public ResourceChangedData getResourceChangedData(String fileName) throws IOException {
        EventRecord event = EventRecord.newBuilder()
                .setType(CHANGED)
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        String chargesData = getChargesData(fileName);

        ResourceChangedData resourceChanged = getChargesData(event, chargesData);
        return resourceChanged;
    }

    public ResourceChangedData getResourceChangedDataForDeletedEvent(String fileName) throws IOException {
        EventRecord event = EventRecord.newBuilder()
                .setType(DELETED)
                .setPublishedAt("2022-02-22T10:51:30")
                .setFieldsChanged(Arrays.asList("foo", "moo"))
                .build();

        String chargesData = getChargesData(fileName);

        ResourceChangedData resourceChanged = getChargesData(event, chargesData);
        return resourceChanged;
    }

    private ResourceChangedData getChargesData(EventRecord event, String chargesData) {
        ResourceChangedData resourceChanged = ResourceChangedData.newBuilder()
                .setContextId(CONTEXT_ID)
                .setResourceId(RESOURCE_ID)
                .setResourceKind(RESOURCE_KIND_CHARGES)
                .setResourceUri(CHARGES_RESOURCE_URI)
                .setData(chargesData)
                .setEvent(event)
                .build();
        return resourceChanged;
    }

    private ResourceChangedData getInsolvencyData(EventRecord event, String chargesData) {
        ResourceChangedData resourceChanged = ResourceChangedData.newBuilder()
                .setContextId(CONTEXT_ID)
                .setResourceId(RESOURCE_ID)
                .setResourceKind(RESOURCE_KIND_INSOLVENCY)
                .setResourceUri(INSOLVENCY_RESOURCE_URI)
                .setData(chargesData)
                .setEvent(event)
                .build();
        return resourceChanged;
    }

    private String getChargesData(String filename) throws IOException {
        InputStreamReader exampleChargesJsonPayload = new InputStreamReader(
                Objects.requireNonNull(ClassLoader.getSystemClassLoader()
                        .getResourceAsStream(filename)));
        return FileCopyUtils.copyToString(exampleChargesJsonPayload);
    }

}
