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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.client.HttpClient;
import org.testng.annotations.Test;
import org.weakref.jmx.MBeanExporter;

import javax.inject.Qualifier;
import javax.management.MBeanServer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.management.ManagementFactory;
import java.util.IdentityHashMap;

import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestPrestoHttpClientProvider
{
    @Test
    public void testPrestoHttpClientProvider()
            throws Exception
    {
        int clientCount = 8;
        Injector injector = new Bootstrap(
                binder -> {
                    httpClientBinder(binder)
                            .bindHttpClient(new PrestoHttpClientProvider("foo", TestingHttpClient.class, clientCount), Scopes.NO_SCOPE);
                    binder.bind(MBeanExporter.class).in(Scopes.SINGLETON);
                    binder.bind(MBeanServer.class).toInstance(ManagementFactory.getPlatformMBeanServer());
                })
                .quiet()
                .strictConfig()
                .initialize();

        IdentityHashMap<HttpClient, Integer> clientMap = new IdentityHashMap<>();
        for (int i = 0; i < 10_000; i++) {
            HttpClient httpClient = (HttpClient) injector.getInstance(Key.get(HttpClient.class, TestingHttpClient.class));
            assertNotNull(httpClient, "httpClient is null");
            clientMap.merge(httpClient, 1, Integer::sum);
        }
        assertEquals(clientMap.size(), clientCount);

        // unregister mbeans
        injector.getInstance(LifeCycleManager.class).stop();
    }

    @Retention(RUNTIME)
    @Target(ElementType.PARAMETER)
    @Qualifier
    public @interface TestingHttpClient
    {
    }
}
