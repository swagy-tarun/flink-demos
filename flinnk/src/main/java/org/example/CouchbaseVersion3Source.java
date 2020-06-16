package org.example;

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
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

public class CouchbaseVersion3Source extends RichSourceFunction<JsonObject> implements ListCheckpointed<LocalDateTime> {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseVersion3Source.class);

    private Cluster cluster;
    private volatile boolean isRunning = true;
    private LocalDateTime lastWindowTime;
    private Time queryFrequency;

    public CouchbaseVersion3Source(final Time queryFrequency) {
        this.queryFrequency = queryFrequency;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (cluster == null) {
            cluster = Cluster.connect("localhost", "admin", "password");
        }
    }

    @Override
    public void run(SourceContext<JsonObject> ctx) throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        while (isRunning) {
            LOG.info("Couchbase Source running ...{}", lastWindowTime);
            JsonObject parameters = JsonObject.create(2);
            synchronized (ctx.getCheckpointLock()) {
                LocalDateTime now = LocalDateTime.now();
                String less;
                if (lastWindowTime == null) {
                    lastWindowTime = now.minusSeconds(10);
                    less = formatter.format(now);
                } else {
                    Duration duration = Duration.between(lastWindowTime, now);
                    if (duration.getSeconds() > 60) {
                        LOG.info("Duration between two fetch: {}", duration.getSeconds());
                        less = formatter.format(lastWindowTime.plusSeconds(Math.min(120, duration.getSeconds())));
                    } else {
                        less = formatter.format(lastWindowTime.plusSeconds(queryFrequency.toMilliseconds() / 1000));
                    }
                }
                String greater = formatter.format(lastWindowTime);
                lastWindowTime = LocalDateTime.parse(less, formatter);
                parameters.put("greater", greater);
                parameters.put("less", less);
                LOG.info("Query parameters: {}", parameters.toString());
            }
            Mono<ReactiveQueryResult> result = cluster.reactive()
                    .query("select * from `test-inserts` where updated >= $greater and updated < $less",
                            QueryOptions.queryOptions().readonly(true).parameters(parameters));


            result.flatMapMany(ReactiveQueryResult::rowsAsObject).
                    subscribe(row -> {
                                synchronized (ctx.getCheckpointLock()) {
                                    JsonObject record = row.getObject("test-inserts");
                                    /*LOG.info("Id {} of Record Fetched: {}", record.get("document_id"), record.get("brewery_id"));*/
                                    ctx.collect(record);
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
    public List<LocalDateTime> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Collections.singletonList(this.lastWindowTime);
    }

    @Override
    public void restoreState(List<LocalDateTime> state) throws Exception {
        if (!state.isEmpty()) {
            this.lastWindowTime = state.get(0);
        } else {
            LOG.warn("State is empty");
        }

    }
}
