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
