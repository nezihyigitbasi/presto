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

import com.google.common.io.Closer;
import io.airlift.http.client.AbstractHttpClientProvider;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.annotation.Annotation;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;

public class PrestoHttpClientProvider
        extends AbstractHttpClientProvider
{
    private final HttpClient[] httpClients;
    private MBeanExporter exporter;

    public PrestoHttpClientProvider(String name, Class<? extends Annotation> annotation, int clientCount)
    {
        super(name, annotation);
        checkArgument(clientCount > 0, "clientCount must be positive");
        this.httpClients = new JettyHttpClient[clientCount];
    }

    @Override
    public void initialize()
    {
        exporter = injector.getInstance(MBeanExporter.class);
        for (int i = 0; i < httpClients.length; i++) {
            httpClients[i] = new JettyHttpClient(name, getHttpClientConfig(), getKerberosConfig(), getHttpRequestFilters());
            String name = ObjectNames.builder(HttpClient.class, annotation).withProperty("id", String.valueOf(i)).build();
            exporter.export(name, httpClients[i]);
        }
    }

    @Override
    public HttpClient get()
    {
        return httpClients[ThreadLocalRandom.current().nextInt(httpClients.length)];
    }

    @Override
    public void close()
    {
        Closer closer = Closer.create();
        for (int i = 0; i < httpClients.length; i++) {
            String mbeanName = ObjectNames.builder(HttpClient.class, annotation).withProperty("id", String.valueOf(i)).build();
            closer.register(httpClients[i]);
            closer.register(() -> exporter.unexport(mbeanName));
        }
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
