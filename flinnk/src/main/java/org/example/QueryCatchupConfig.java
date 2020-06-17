package org.example;

import java.io.Serializable;
import java.time.Duration;

public class QueryCatchupConfig implements Serializable {
    private static QueryCatchupConfig DISABLED;

    static {
        DISABLED = new QueryCatchupConfig();
    }

    public enum Strategy {
        ENABLED,
        DISABLED
    }

    private QueryCatchupConfig() {
    }

    private Strategy strategy = Strategy.DISABLED;
    private Duration retro;
    private boolean fastForwardEnabled;
    private Duration fastForwardThreshold;
    private Duration fastForwardBy;

    public static QueryCatchupConfig disabled() {
        return DISABLED;
    }

    public static QueryCatchupConfig build(Duration retro, boolean fastForwardEnabled, Duration fastForwardThreshold, Duration fastForwardBy) {
        QueryCatchupConfig queryCatchupConfig = new QueryCatchupConfig();
        queryCatchupConfig.strategy = Strategy.ENABLED;
        queryCatchupConfig.retro = retro;
        queryCatchupConfig.fastForwardEnabled = fastForwardEnabled;
        queryCatchupConfig.fastForwardThreshold = fastForwardThreshold;
        queryCatchupConfig.fastForwardBy = fastForwardBy;

        return queryCatchupConfig;
    }


    public Strategy getStrategy() {
        return strategy;
    }

    public Duration getRetro() {
        return retro;
    }

    public boolean isFastForwardEnabled() {
        return fastForwardEnabled;
    }

    public Duration getFastForwardThreshold() {
        return fastForwardThreshold;
    }

    public Duration getFastForwardBy() {
        return fastForwardBy;
    }
}
