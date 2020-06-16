package org.example;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

public class Brewery implements Serializable {

    @JsonProperty("document_id")
    private String documentId;
    @JsonProperty("brewery_id")
    private String breweryId;
    private String updated;
    private String id;
    private String style;
    private String description;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getStyle() {
        return style;
    }

    public void setStyle(String style) {
        this.style = style;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public String getBreweryId() {
        return breweryId;
    }

    public void setBreweryId(String breweryId) {
        this.breweryId = breweryId;
    }

    public String getUpdated() {
        return updated;
    }

    public void setUpdated(String updated) {
        this.updated = updated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Brewery brewery = (Brewery) o;

        return new EqualsBuilder()
                .append(documentId, brewery.documentId)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(documentId)
                .toHashCode();
    }
}
