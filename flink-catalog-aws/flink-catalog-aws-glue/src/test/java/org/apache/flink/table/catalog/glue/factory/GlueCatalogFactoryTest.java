/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.glue.factory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.glue.GlueCatalog;
import org.apache.flink.table.factories.CatalogFactory;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link GlueCatalogFactory}. */
class GlueCatalogFactoryTest {

    @Test
    void testFactoryIdentifier() {
        assertThat(new GlueCatalogFactory().factoryIdentifier()).isEqualTo("glue");
    }

    @Test
    void testCreateCatalogWithMinimalOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");

        Catalog catalog = createCatalog("my_glue", options);

        assertThat(catalog).isInstanceOf(GlueCatalog.class);
        assertThat(((GlueCatalog) catalog).getName()).isEqualTo("my_glue");
        assertThat(((GlueCatalog) catalog).getDefaultDatabase()).isEqualTo("default");
    }

    @Test
    void testCreateCatalogWithDefaultDatabase() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("default-database", "analytics");

        Catalog catalog = createCatalog("my_glue", options);

        assertThat(((GlueCatalog) catalog).getDefaultDatabase()).isEqualTo("analytics");
    }

    @Test
    void testMissingRegionIsRejected() {
        assertThatThrownBy(() -> createCatalog("my_glue", new HashMap<>()))
                .hasMessageContaining("region");
    }

    @Test
    void testUnknownOptionIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("unknown-option", "value");

        assertThatThrownBy(() -> createCatalog("my_glue", options))
                .hasMessageContaining("unknown-option");
    }

    @Test
    void testAwsAndHttpClientOptionsArePassedThrough() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("aws.endpoint", "http://localhost:5000");
        options.put("aws.credentials.provider", "BASIC");
        options.put("aws.credentials.basic.accesskeyid", "testAccessKey");
        options.put("aws.credentials.basic.secretkey", "testSecret");
        options.put("http-client.connection-timeout-ms", "5000");

        Catalog catalog = createCatalog("my_glue", options);

        assertThat(catalog).isInstanceOf(GlueCatalog.class);
    }

    @Test
    void testInvalidCredentialConfigurationIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        // BASIC requires access key id and secret key to be configured.
        options.put("aws.credentials.provider", "BASIC");

        assertThatThrownBy(() -> createCatalog("my_glue", options))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInvalidHttpClientOptionIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("http-client.connection-timeout-ms", "not-a-number");

        assertThatThrownBy(() -> createCatalog("my_glue", options))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Catalog createCatalog(String name, Map<String, String> options) {
        return new GlueCatalogFactory()
                .createCatalog(
                        new CatalogFactory.Context() {
                            @Override
                            public String getName() {
                                return name;
                            }

                            @Override
                            public Map<String, String> getOptions() {
                                return options;
                            }

                            @Override
                            public Configuration getConfiguration() {
                                return new Configuration();
                            }

                            @Override
                            public ClassLoader getClassLoader() {
                                return GlueCatalogFactoryTest.class.getClassLoader();
                            }
                        });
    }
}
