/**
 * PermissionsEx
 * Copyright (C) zml and PermissionsEx contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ninja.leaping.permissionsex.backend.sql;

import com.google.common.collect.ImmutableList;
import ninja.leaping.configurate.objectmapping.ObjectMappingException;
import ninja.leaping.permissionsex.PermissionsExTest;
import ninja.leaping.permissionsex.backend.DataStore;
import ninja.leaping.permissionsex.config.PermissionsExConfiguration;
import ninja.leaping.permissionsex.exception.PEBKACException;
import ninja.leaping.permissionsex.exception.PermissionsLoadingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class SqlDaoTest extends PermissionsExTest {
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object> data() {
        String[] propertyTestDbs = System.getProperty("ninja.leaping.permissions.backend.sql.testDatabases", "").split(";", -1);
        if (propertyTestDbs.length == 1 && propertyTestDbs[0].equals("")) {
            propertyTestDbs = new String[0];
        }
        final Object[][] tests = new Object[propertyTestDbs.length + 1][2];
        tests[propertyTestDbs.length] = new Object[] {"h2", "jdbc:h2:{base}/test.db"};
        for (int i = 0; i < propertyTestDbs.length; ++i) {
            tests[i] = propertyTestDbs[i].split("!");
        }
        return Arrays.asList(tests);
    }

    private final SqlDataStore sqlStore = new SqlDataStore();
    private  String databaseName, jdbcUrl;

    public SqlDaoTest(String databaseName, String jdbcUrl) throws IOException {
        this.databaseName = databaseName;
        this.jdbcUrl = jdbcUrl;
        System.out.println("Creating instance");
    }

    @Before
    @Override
    public void setUp() throws IOException, PEBKACException, PermissionsLoadingException, ObjectMappingException {
        System.out.println("Setting up");
        File testDir = tempFolder.newFolder();
        jdbcUrl = jdbcUrl.replaceAll("\\{base\\}", testDir.getCanonicalPath());
        sqlStore.setConnectionUrl(jdbcUrl);
        super.setUp();
    }

    @Override
    protected PermissionsExConfiguration populate() {
        return new PermissionsExConfiguration() {
            @Override
            public DataStore getDataStore(String name) {
                return null;
            }

            @Override
            public DataStore getDefaultDataStore() {
                return sqlStore;
            }

            @Override
            public boolean isDebugEnabled() {
                return false;
            }

            @Override
            public List<String> getServerTags() {
                return ImmutableList.of();
            }

            @Override
            public void validate() throws PEBKACException {
            }

            @Override
            public PermissionsExConfiguration reload() throws IOException {
                return this;
            }
        };
    }

    @Test
    public void testGetSubjectRef() throws SQLException {
        try (SqlDao dao = sqlStore.getDao()) {
            assertFalse(dao.getSubjectRef("group", "admin").isPresent());
            assertFalse(dao.getSubjectRef(1).isPresent());
        }
    }

    @Test
    public void testGetOrCreateSubjectRef() throws SQLException {
        try (SqlDao dao = sqlStore.getDao()) {
            assertFalse(dao.getSubjectRef("group", "admin").isPresent());
            SubjectRef created = dao.getOrCreateSubjectRef("group", "admin");
            SubjectRef fetched = dao.getSubjectRef("group", "admin").get();
            assertEquals(created.getId(), fetched.getId());

            SubjectRef couldBeCreated = dao.getOrCreateSubjectRef("group", "admin");
            assertEquals(created.getId(), couldBeCreated.getId());

            SubjectRef gottenById = dao.getSubjectRef(created.getId()).get();
            assertEquals(created.getId(), gottenById.getId());
            assertEquals(created, gottenById);
        }
    }

    @Test
    public void removeSubject() throws SQLException {
        try (SqlDao dao = sqlStore.getDao()) {
            SubjectRef first = dao.getOrCreateSubjectRef("group", "one");
            SubjectRef second = dao.getOrCreateSubjectRef("group", "two");

            assertTrue(dao.removeSubject("group", "one"));
            assertFalse(dao.getSubjectRef(first.getType(), first.getIdentifier()).isPresent());

            assertTrue(dao.removeSubject(second));
            assertFalse(dao.getSubjectRef(second.getId()).isPresent());
        }
    }

    @Test
    public void getRegisteredTypes() throws SQLException {
        try (SqlDao dao = sqlStore.getDao()) {

        }

    }

    /*@Test


    @Test
    public void getSegments() throws SQLException {

    }

    @Test
    public void addSegment() throws SQLException {

    }

    @Test
    public void removeSegment() throws SQLException {

    }

    @Test
    public void getAllIdentifiers() throws SQLException {

    }


    @Test
    public void initializeTables() throws SQLException {

    }

    @Test
    public void clearOption() throws SQLException {

    }

    @Test
    public void setOptions() throws SQLException {

    }

    @Test
    public void setOption() throws SQLException {

    }

    @Test
    public void setPermission() throws SQLException {

    }

    @Test
    public void clearPermission() throws SQLException {

    }

    @Test
    public void setPermissions() throws SQLException {

    }

    @Test
    public void setDefaultValue() throws SQLException {

    }

    @Test
    public void addParent() throws SQLException {

    }

    @Test
    public void removeParent() throws SQLException {

    }

    @Test
    public void setParents() throws SQLException {

    }*/
}
