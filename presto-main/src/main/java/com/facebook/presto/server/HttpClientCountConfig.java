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
package com.facebook.presto.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class HttpClientCountConfig
{
    private int schedulerHttpClientCount = 8;
    private int exchangeHttpClientCount = 8;

    public int getSchedulerHttpClientCount()
    {
        return schedulerHttpClientCount;
    }

    @Config("scheduler-http-client-count")
    @ConfigDescription("The number of scheduler HTTP clients to use")
    public HttpClientCountConfig setSchedulerHttpClientCount(int schedulerHttpClientCount)
    {
        this.schedulerHttpClientCount = schedulerHttpClientCount;
        return this;
    }

    public int getExchangeHttpClientCount()
    {
        return exchangeHttpClientCount;
    }

    @Config("exchange-http-client-count")
    @ConfigDescription("The number of exchange HTTP clients to use")
    public HttpClientCountConfig setExchangeHttpClientCount(int exchangeHttpClientCount)
    {
        this.exchangeHttpClientCount = exchangeHttpClientCount;
        return this;
    }
}
