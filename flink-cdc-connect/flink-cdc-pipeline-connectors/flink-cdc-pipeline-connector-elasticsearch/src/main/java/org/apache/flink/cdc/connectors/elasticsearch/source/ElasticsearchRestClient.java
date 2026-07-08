/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.elasticsearch.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Minimal Elasticsearch REST client used by the pipeline source. */
class ElasticsearchRestClient implements Closeable, Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ElasticsearchSourceConfig config;
    private final AtomicInteger nextHost = new AtomicInteger();

    private transient CloseableHttpClient client;

    ElasticsearchRestClient(ElasticsearchSourceConfig config) {
        this.config = config;
    }

    JsonNode get(String path) throws IOException {
        HttpGet get = new HttpGet(nextBaseUrl() + path);
        return execute(get);
    }

    JsonNode post(String path, JsonNode body) throws IOException {
        HttpPost post = new HttpPost(nextBaseUrl() + path);
        post.setEntity(
                new StringEntity(
                        OBJECT_MAPPER.writeValueAsString(body), ContentType.APPLICATION_JSON));
        return execute(post);
    }

    JsonNode delete(String path, JsonNode body) throws IOException {
        HttpDeleteWithBody delete = new HttpDeleteWithBody(nextBaseUrl() + path);
        delete.setEntity(
                new StringEntity(
                        OBJECT_MAPPER.writeValueAsString(body), ContentType.APPLICATION_JSON));
        return execute(delete);
    }

    JsonNode openPit(String index) throws IOException {
        String path =
                "/" + encodePath(index) + "/_pit?keep_alive=" + config.getPitKeepAliveString();
        return post(path, OBJECT_MAPPER.createObjectNode());
    }

    void closePit(String pitId) throws IOException {
        if (pitId != null && !pitId.isEmpty()) {
            delete("/_pit", OBJECT_MAPPER.createObjectNode().put("id", pitId));
        }
    }

    static ObjectMapper objectMapper() {
        return OBJECT_MAPPER;
    }

    private JsonNode execute(HttpRequestBase request) throws IOException {
        request.setHeader("Accept", "application/json");
        HttpResponse response = client().execute(request);
        HttpEntity entity = response.getEntity();
        String body = entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode < 200 || statusCode >= 300) {
            throw new IOException(
                    String.format(
                            "Elasticsearch request %s %s failed with status %s: %s",
                            request.getMethod(), request.getURI(), statusCode, body));
        }
        if (body.isEmpty()) {
            return OBJECT_MAPPER.createObjectNode();
        }
        return OBJECT_MAPPER.readTree(body);
    }

    private CloseableHttpClient client() {
        if (client == null) {
            HttpClientBuilder builder = HttpClientBuilder.create();
            if (config.getUsername() != null && config.getPassword() != null) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(
                                config.getUsername(), config.getPassword()));
                builder.setDefaultCredentialsProvider(credentialsProvider);
            }
            int timeoutMillis = Math.toIntExact(config.getRequestTimeout().toMillis());
            org.apache.http.client.config.RequestConfig requestConfig =
                    org.apache.http.client.config.RequestConfig.custom()
                            .setConnectTimeout(timeoutMillis)
                            .setSocketTimeout(timeoutMillis)
                            .setConnectionRequestTimeout(timeoutMillis)
                            .build();
            client = builder.setDefaultRequestConfig(requestConfig).build();
        }
        return client;
    }

    private String nextBaseUrl() {
        List<String> hosts = config.getHosts();
        int idx = Math.floorMod(nextHost.getAndIncrement(), hosts.size());
        String host = hosts.get(idx);
        // Validate eagerly and normalize to avoid producing malformed request URLs later.
        HttpHost parsed = HttpHost.create(host);
        return parsed.toURI();
    }

    private static String encodePath(String path) {
        return URLEncoder.encode(path, StandardCharsets.UTF_8).replace("+", "%20");
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    private static class HttpDeleteWithBody extends HttpPost {
        HttpDeleteWithBody(String uri) {
            super(uri);
        }

        @Override
        public String getMethod() {
            return HttpDelete.METHOD_NAME;
        }
    }
}
