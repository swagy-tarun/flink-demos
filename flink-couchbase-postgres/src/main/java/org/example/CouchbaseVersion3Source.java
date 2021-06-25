package org.example;

/**
 * MIT License
 * <p>
 * Copyright (c) 2020 Tarun Arora
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Couchbase Streaming source works on a simplistic design of continuously querying the database and
 * emitting the elements to next operator in chain.
 * <p>
 * Implements {@link org.apache.flink.streaming.api.checkpoint.CheckpointedFunction} to support
 * failover and recovery by storing the last queried timestamp in State Store.
 * <p>
 * Source can be configured with frequency of querying. Source can also be configured with
 *
 * @param <T> pojo type to which query result should be converted
 * @link QueryCatchupConfig to fast forward querying by x seconds when the job has recovered from a
 * failover scenario or for some reason it has lagged behind the current time.
 */
public class CouchbaseVersion3Source<T> extends RichSourceFunction<T> implements
    ListCheckpointed<ZonedDateTime> {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseVersion3Source.class);

  private Cluster cluster;
  private volatile boolean isRunning = true;
  private ZonedDateTime lastWindowTime;
  private final String query;
  private final CouchbaseSourceQuery couchbaseSourceQuery;
  private final QueryCatchupConfig queryCatchupConfig;
  private final CouchbaseClusterInfo couchbaseClusterInfo;

  public CouchbaseVersion3Source(CouchbaseClusterInfo clusterInfo,
      final CouchbaseSourceQuery couchbaseSourceQuery,
      final QueryCatchupConfig queryCatchupConfig) {
    this.couchbaseClusterInfo = clusterInfo;
    final String queryformat = "select * from `%s` where %s >= $greater and %s < $less";
    this.query = String.format(queryformat, couchbaseSourceQuery.getBucket(),
        couchbaseSourceQuery.getDateQueryField(), couchbaseSourceQuery.getDateQueryField());
    this.couchbaseSourceQuery = couchbaseSourceQuery;
    this.queryCatchupConfig = queryCatchupConfig;
  }

  @Override
  public void open(Configuration parameters) throws Exception {

    super.open(parameters);
    if (cluster == null) {
      cluster = Cluster
          .connect(this.couchbaseClusterInfo.getHost(), this.couchbaseClusterInfo.getUser(),
              this.couchbaseClusterInfo.getPassword());
    }
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    DateTimeFormatter formatter = DateTimeFormatter
        .ofPattern(this.couchbaseSourceQuery.getDateFormatOfQueryField());
    while (isRunning) {
      LOG.info("Couchbase Source running ...{}", lastWindowTime);
      JsonObject parameters = JsonObject.create(2);
      synchronized (ctx.getCheckpointLock()) {
        ZonedDateTime now = ZonedDateTime.now(this.couchbaseSourceQuery.getZoneId());
        ZonedDateTime less;
        if (lastWindowTime == null) {
          // Starting from retrospective when starts for first time. After checkpointing this
          // if block will not be invoked
          if (QueryCatchupConfig.Strategy.ENABLED.equals(queryCatchupConfig.getStrategy())) {
            lastWindowTime = now.minusSeconds(queryCatchupConfig.getRetro().getSeconds());
          }
          less = lastWindowTime;
        } else {
                    /* this is to fast forward the query after recovering from failure since time
                    could have elapsed. Fast forwarding will query more data by providing long windows as
                    configured till the time it catches up with current time. Use with caution to avoid OOM errors.
                     */
          if (QueryCatchupConfig.Strategy.ENABLED.equals(queryCatchupConfig.getStrategy()) &&
              queryCatchupConfig.isFastForwardEnabled()) {
            Duration duration = Duration.between(lastWindowTime, now);
            if (duration.getSeconds() > queryCatchupConfig.getFastForwardThreshold().getSeconds()) {
              LOG.info("Duration between two fetch: {}", duration.getSeconds());
              less = lastWindowTime
                  .plusSeconds(Math.min(queryCatchupConfig.getFastForwardBy().getSeconds(),
                      duration.getSeconds()));
            } else {
              less = lastWindowTime
                  .plusSeconds(this.couchbaseSourceQuery.getQueryFrequency().getSeconds());
            }
          } else {
            less = lastWindowTime
                .plusSeconds(this.couchbaseSourceQuery.getQueryFrequency().getSeconds());
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
                  JsonObject record = row.getObject(this.couchbaseSourceQuery.getBucket());
                  try {
                    //
                    T value = (T) this.couchbaseSourceQuery.getObjectMapper()
                        .readValue(record.toString(),
                            this.couchbaseSourceQuery.getTypeToCast());
                    ctx.collect(value);
                  } catch (JsonProcessingException e) {
                    LOG.error("Error in reading record from source", e);
                  }

                }
              }
          );
      Thread.sleep(this.couchbaseSourceQuery.getQueryFrequency().getSeconds() * 1000);
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
