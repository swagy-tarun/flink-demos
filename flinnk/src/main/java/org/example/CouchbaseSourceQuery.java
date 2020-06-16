package org.example;

/**
 * MIT License
 * <p>
 * Copyright (c) 2020 Tarun Arora
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.time.ZoneId;

public class CouchbaseSourceQuery implements Serializable {
    final private String bucket;
    final private String dateQueryField;
    final private Class typeToCast;
    final private ObjectMapper objectMapper;
    final private String dateFormatOfQueryField;
    final private ZoneId zoneId;

    public CouchbaseSourceQuery(String bucket, String dateQueryField, final Class typeToCast,
                                final String dateFormatOfQueryField, final ZoneId zoneId) {
        this.bucket = bucket;
        this.dateQueryField = dateQueryField;
        this.typeToCast = typeToCast;
        this.dateFormatOfQueryField = dateFormatOfQueryField;
        this.zoneId = zoneId;

        this.objectMapper = new ObjectMapper();
    }

    public CouchbaseSourceQuery(String bucket, String dateQueryField, Class typeToCast,
                                final String dateFormatOfQueryField, final ZoneId zoneId, ObjectMapper objectMapper) {
        this.bucket = bucket;
        this.dateQueryField = dateQueryField;
        this.typeToCast = typeToCast;
        this.dateFormatOfQueryField = dateFormatOfQueryField;
        this.zoneId = zoneId;
        this.objectMapper = objectMapper;
    }

    public String getBucket() {
        return bucket;
    }

    public String getDateQueryField() {
        return dateQueryField;
    }

    public Class getTypeToCast() {
        return typeToCast;
    }

    public String getDateFormatOfQueryField() {
        return dateFormatOfQueryField;
    }

    public ZoneId getZoneId() {
        return zoneId;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
