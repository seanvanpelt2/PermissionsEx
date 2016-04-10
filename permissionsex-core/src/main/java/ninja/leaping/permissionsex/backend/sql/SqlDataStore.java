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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import ninja.leaping.configurate.objectmapping.Setting;
import ninja.leaping.permissionsex.backend.AbstractDataStore;
import ninja.leaping.permissionsex.backend.ConversionUtils;
import ninja.leaping.permissionsex.backend.DataStore;
import ninja.leaping.permissionsex.backend.sql.dao.H2SqlDao;
import ninja.leaping.permissionsex.backend.sql.dao.MySqlDao;
import ninja.leaping.permissionsex.data.ContextInheritance;
import ninja.leaping.permissionsex.data.ImmutableSubjectData;
import ninja.leaping.permissionsex.exception.PermissionsLoadingException;
import ninja.leaping.permissionsex.rank.RankLadder;
import ninja.leaping.permissionsex.util.ThrowingFunction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.regex.Pattern;

import static ninja.leaping.permissionsex.util.Translations.t;

/**
 * DataSource for SQL data
 */
public final class SqlDataStore extends AbstractDataStore {
    public static final Factory FACTORY = new Factory("sql", SqlDataStore.class);
    private static final Pattern BRACES_PATTERN = Pattern.compile("\\{\\}");

    protected SqlDataStore() {
        super(FACTORY);
    }

    @Setting("url")
    private String connectionUrl;
    @Setting("prefix")
    private String prefix = "pex";
    private String realPrefix;
    @Setting("aliases")
    private Map<String, String> legacyAliases;

    private final ConcurrentMap<String, String> queryPrefixCache = new ConcurrentHashMap<>();
    private final ThreadLocal<SqlDao> heldDao = new ThreadLocal<>();
    private final Map<String, ThrowingFunction<SqlDataStore, SqlDao, SQLException>> daoImplementations = ImmutableMap.of("mysql", MySqlDao::new, "h2", H2SqlDao::new);
    private ThrowingFunction<SqlDataStore, SqlDao, SQLException> daoFactory;
    private DataSource sql;

    SqlDao getDao() throws SQLException {
        SqlDao dao = heldDao.get();
        if (dao != null) {
            return dao;
        }
        return daoFactory.apply(this);
    }

    @Override
    protected void initializeInternal() throws PermissionsLoadingException {
        sql = getManager().getDataSourceForURL(connectionUrl);
        if (this.prefix != null && !this.prefix.isEmpty() && !this.prefix.endsWith("_")) {
            this.realPrefix = this.prefix + "_";
        } else if (this.prefix == null) {
            this.realPrefix = "";
        } else {
            this.realPrefix = this.prefix;
        }

        // Provide database-implementation specific DAO
        try (Connection conn = sql.getConnection()) {
            final String database = conn.getMetaData().getDatabaseProductName().toLowerCase();
            this.daoFactory = daoImplementations.get(database);
            if (this.daoFactory == null) {
                throw new PermissionsLoadingException(t("Database implementation %s is not supported!", database));
            }
        } catch (SQLException e) {
            throw new PermissionsLoadingException(t("Could not connect to SQL database!"), e);
        }

        // Initialize data
        try (SqlDao dao = getDao()) {
            System.out.println("Initializing tables");
            dao.initializeTables();
        } catch (SQLException e) {
            throw new PermissionsLoadingException(t("Error interacting with SQL database"), e);
        }
    }

    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    DataSource getDataSource() {
        return this.sql;
    }

    String insertPrefix(String query) {
        return queryPrefixCache.computeIfAbsent(query, qu -> BRACES_PATTERN.matcher(qu).replaceAll(this.realPrefix));
    }

    // TODO Make getting data asynchronous
    @Override
    protected SqlSubjectData getDataInternal(String type, String identifier) throws PermissionsLoadingException {
        try (SqlDao dao = getDao()) {
            SubjectRef ref = dao.getOrCreateSubjectRef(type, identifier);
            List<Segment> segments = dao.getSegments(ref);
            Map<Set<Map.Entry<String, String>>, Segment> contexts = new HashMap<>();
            for (Segment segment : segments) {
                contexts.put(segment.getContexts(), segment);
            }

            return new SqlSubjectData(ref, contexts, null);
        } catch (SQLException e) {
            throw new PermissionsLoadingException(t("Error loading permissions for %s %s", type, identifier), e);
        }
    }

    @Override
    protected CompletableFuture<ImmutableSubjectData> setDataInternal(String type, String identifier, ImmutableSubjectData data) {
        // Cases: update data for sql (easy), update of another type (get SQL data, do update)
        SqlSubjectData sqlData;
        if (data instanceof SqlSubjectData) {
            sqlData = (SqlSubjectData) data;
        } else {
            return runAsync(() -> {
                SqlSubjectData newData = getDataInternal(type, identifier);
                ConversionUtils.transfer(data, newData);

                return null;
            });
        }
        return runAsync(() -> {
            try (SqlDao dao = getDao()) {
                sqlData.doUpdates(dao);
                return sqlData;
            }
        });
    }

    @Override
    protected RankLadder getRankLadderInternal(String ladder) {
        return null;
    }

    @Override
    protected CompletableFuture<RankLadder> setRankLadderInternal(String ladder, RankLadder newLadder) {
        return null;
    }

    @Override
    public boolean isRegistered(String type, String identifier) {
        try (SqlDao dao = getDao()) {
            return dao.getSubjectRef(type, identifier).isPresent();
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public Set<String> getAllIdentifiers(String type) {
        try (SqlDao dao = getDao()) {
            return dao.getAllIdentifiers(type);
        } catch (SQLException e) {
            return ImmutableSet.of();
        }
    }

    @Override
    public Set<String> getRegisteredTypes() {
        try (SqlDao dao = getDao()) {
            return dao.getRegisteredTypes();
        } catch (SQLException e) {
            return ImmutableSet.of();
        }

    }

    @Override
    public Iterable<Map.Entry<Map.Entry<String, String>, ImmutableSubjectData>> getAll() {
        return ImmutableSet.of();
    }

    @Override
    public Iterable<String> getAllRankLadders() {
        return ImmutableSet.of();
    }

    @Override
    public boolean hasRankLadder(String ladder) {
        return false;
    }

    @Override
    public ContextInheritance getContextInheritanceInternal() {
        return null;
    }

    @Override
    public CompletableFuture<ContextInheritance> setContextInheritanceInternal(ContextInheritance inheritance) {
        return null;
    }

    @Override
    protected <T> T performBulkOperationSync(final Function<DataStore, T> function) throws Exception {
        SqlDao dao = null;
        try {
            dao = getDao();
            heldDao.set(dao);
            dao.holdOpen++;
            T res = function.apply(this);
            return res;
        } finally {
            if (dao != null) {
                if (--dao.holdOpen == 0) {
                    heldDao.set(null);
                }
                try {
                    dao.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }

    @Override
    public void close() {
        this.queryPrefixCache.clear();
    }
}
