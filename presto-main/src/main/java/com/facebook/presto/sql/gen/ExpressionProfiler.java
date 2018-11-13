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

import static com.google.common.base.Verify.verify;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ExpressionProfiler
{
    private static final int NUMBER_OF_ROWS_TO_PROFILE = 1024;
    private static final int EXPENSIVE_FUNCTION_THRESHOLD_MILLIS = 1_000;

    private final int rowsToProfile;
    private final int expensiveFunctionThresholdMillis;
    private double meanExecutionTime;
    private int samples;
    private long previousTimestamp = -1;
    private boolean isExpressionExpensive = true;
    private boolean isProfiling = true;

    public ExpressionProfiler()
    {
        this.rowsToProfile = NUMBER_OF_ROWS_TO_PROFILE;
        this.expensiveFunctionThresholdMillis = EXPENSIVE_FUNCTION_THRESHOLD_MILLIS;
    }

    @VisibleForTesting
    public ExpressionProfiler(int rowsToProfile, int expensiveFunctionThresholdMillis)
    {
        verify(rowsToProfile >= 0);
        verify(expensiveFunctionThresholdMillis >= 0);
        this.rowsToProfile = rowsToProfile;
        this.expensiveFunctionThresholdMillis = expensiveFunctionThresholdMillis;
    }

    public void start()
    {
        if (!isProfiling) {
            return;
        }

        previousTimestamp = System.nanoTime();
    }

    public void stop(int batchSize)
    {
        if (!isProfiling) {
            return;
        }

        long now = System.nanoTime();
        long delta = NANOSECONDS.toMillis(now - previousTimestamp);
        meanExecutionTime = (meanExecutionTime * samples + delta) / (samples + 1);
        samples += batchSize;
        if (samples >= rowsToProfile) {
            isProfiling = false;
            if (meanExecutionTime < expensiveFunctionThresholdMillis) {
                isExpressionExpensive = false;
            }
        }
        previousTimestamp = -1;
    }

    public boolean isExpressionExpensive()
    {
        return isExpressionExpensive;
    }
}
