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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

public class CouchbaseVersion3Source<T> extends RichSourceFunction<T> implements ListCheckpointed<ZonedDateTime> {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseVersion3Source.class);

    private Cluster cluster;
    private volatile boolean isRunning = true;
    private ZonedDateTime lastWindowTime;
    private Time queryFrequency;
    private final String query;
    private final CouchbaseSourceQuery couchbaseSourceQuery;

    public CouchbaseVersion3Source(final Time queryFrequency, final CouchbaseSourceQuery couchbaseSourceQuery) {
        this.queryFrequency = queryFrequency;
        String queryformat = "select * from `%s` where %s >= $greater and updated < $less";
        this.query = String.format(queryformat, couchbaseSourceQuery.getBucket(), couchbaseSourceQuery.getDateQueryField());
        this.couchbaseSourceQuery = couchbaseSourceQuery;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (cluster == null) {
            cluster = Cluster.connect("localhost", "admin", "password");
        }
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(this.couchbaseSourceQuery.getDateFormatOfQueryField());
        while (isRunning) {
            LOG.info("Couchbase Source running ...{}", lastWindowTime);
            JsonObject parameters = JsonObject.create(2);
            synchronized (ctx.getCheckpointLock()) {
                ZonedDateTime now = ZonedDateTime.now(this.couchbaseSourceQuery.getZoneId());
                ZonedDateTime less;
                if (lastWindowTime == null) {
                    lastWindowTime = now.minusSeconds(10);
                    less = lastWindowTime;
                } else {
                    Duration duration = Duration.between(lastWindowTime, now);
                    if (duration.getSeconds() > 60) {
                        LOG.info("Duration between two fetch: {}", duration.getSeconds());
                        less = lastWindowTime.plusSeconds(Math.min(120, duration.getSeconds()));
                    } else {
                        less = lastWindowTime.plusSeconds(queryFrequency.toMilliseconds() / 1000);
                    }
                }
                String greater = formatter.format(lastWindowTime);
                lastWindowTime = less;
                parameters.put("greater", greater);
                parameters.put("less", formatter.format(less));
                LOG.info("Query parameters: {}", parameters.toString());
            }
            Mono<ReactiveQueryResult> result = cluster.reactive()
                    .query(this.query,
                            QueryOptions.queryOptions().readonly(true).parameters(parameters));


            result.flatMapMany(ReactiveQueryResult::rowsAsObject).
                    subscribe(row -> {
                                synchronized (ctx.getCheckpointLock()) {
                                    JsonObject record = row.getObject("test-inserts");
                                    try {
                                        T value = (T) this.couchbaseSourceQuery.getObjectMapper().readValue(record.toString(),
                                                this.couchbaseSourceQuery.getTypeToCast());
                                        ctx.collect(value);
                                    } catch (JsonProcessingException e) {
                                        LOG.error("Error in reading record from source", e);
                                    }

                                }
                            }
                    );
            Thread.sleep(queryFrequency.toMilliseconds());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        cluster.disconnect();
        cluster = null;
    }

    @Override
    public List<ZonedDateTime> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Collections.singletonList(this.lastWindowTime);
    }

    @Override
    public void restoreState(List<ZonedDateTime> state) throws Exception {
        if (!state.isEmpty()) {
            this.lastWindowTime = state.get(0);
        } else {
            LOG.warn("State is empty");
        }
    }
}
