package uk.gov.companieshouse.company.links.type;

import java.util.Objects;
import uk.gov.companieshouse.stream.ResourceChangedData;

public class ResourceChange {

    private final ResourceChangedData data;

    public ResourceChange(ResourceChangedData data) {
        this.data = data;
    }

    public ResourceChangedData getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceChange that = (ResourceChange) o;
        return Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
