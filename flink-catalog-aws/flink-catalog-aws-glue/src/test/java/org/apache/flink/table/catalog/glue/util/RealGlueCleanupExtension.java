/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.glue.util;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.HashSet;
import java.util.Set;

/**
 * Keeps a real AWS Glue account clean when the catalog test suites run against the real service
 * (see {@link GlueTestClientFactory}): before each test it snapshots the account's database names,
 * and after the test it deletes any database (and its tables) that the test created. The
 * fake-backed default mode is a no-op.
 *
 * <p>The delta approach means pre-existing databases in the account are never touched, and tests
 * keep their fixed database names without colliding across tests.
 */
public class RealGlueCleanupExtension implements BeforeEachCallback, AfterEachCallback {

    private GlueClient cleanupClient;
    private Set<String> databasesBeforeTest;

    @Override
    public void beforeEach(ExtensionContext context) {
        if (!GlueTestClientFactory.REAL_GLUE) {
            return;
        }
        if (cleanupClient == null) {
            cleanupClient = GlueTestClientFactory.createClient();
        }
        databasesBeforeTest = listDatabaseNames();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (!GlueTestClientFactory.REAL_GLUE || databasesBeforeTest == null) {
            return;
        }
        Set<String> createdByTest = listDatabaseNames();
        createdByTest.removeAll(databasesBeforeTest);
        for (String database : createdByTest) {
            try {
                for (Table table :
                        cleanupClient.getTables(b -> b.databaseName(database)).tableList()) {
                    cleanupClient.deleteTable(b -> b.databaseName(database).name(table.name()));
                }
                cleanupClient.deleteDatabase(b -> b.name(database));
            } catch (EntityNotFoundException ignored) {
                // already removed by the test itself
            }
        }
    }

    private Set<String> listDatabaseNames() {
        Set<String> names = new HashSet<>();
        for (GetDatabasesResponse page : cleanupClient.getDatabasesPaginator(b -> {})) {
            for (Database database : page.databaseList()) {
                names.add(database.name());
            }
        }
        return names;
    }
}
