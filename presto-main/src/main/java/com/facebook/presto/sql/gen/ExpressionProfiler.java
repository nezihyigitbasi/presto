/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.gen;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;

import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ExpressionProfiler
{
    private static final long EXPENSIVE_EXPRESSION_THRESHOLD_MILLIS = 1_000;
    private static final int NOT_INITALIZED = -1;

    private final Ticker ticker;
    private final long expensiveExpressionThresholdMillis;
    private double totalExecutionTime;
    private int samples;
    private long previousTimestamp = NOT_INITALIZED;
    private boolean isExpressionExpensive = true;

    public ExpressionProfiler()
    {
        this.expensiveExpressionThresholdMillis = EXPENSIVE_EXPRESSION_THRESHOLD_MILLIS;
        this.ticker = systemTicker();
    }

    @VisibleForTesting
    public ExpressionProfiler(Ticker ticker, long expensiveExpressionThresholdMillis)
    {
        verify(expensiveExpressionThresholdMillis >= 0);
        requireNonNull(ticker, "ticker is null");
        this.expensiveExpressionThresholdMillis = expensiveExpressionThresholdMillis;
        this.ticker = ticker;
    }

    public void start()
    {
        previousTimestamp = ticker.read();
    }

    public void stop(int batchSize)
    {
        verify(previousTimestamp != NOT_INITALIZED, "start() is not called");
        verify(batchSize > 0, "batchSize must be positive");

        long now = ticker.read();
        long delta = NANOSECONDS.toMillis(now - previousTimestamp);
        totalExecutionTime += delta;
        samples += batchSize;
        if ((totalExecutionTime / samples) < expensiveExpressionThresholdMillis) {
            isExpressionExpensive = false;
        }
        previousTimestamp = NOT_INITALIZED;
    }

    public boolean isExpressionExpensive()
    {
        return isExpressionExpensive;
    }
}
