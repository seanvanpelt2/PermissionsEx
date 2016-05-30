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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import ninja.leaping.permissionsex.rank.RankLadder;
import ninja.leaping.permissionsex.util.ThrowingSupplier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/**
 * Abstraction to communicate with the SQL database. Instances are not thread-safe -- it's best to create a new one for each operation on a single thread
 */
public abstract class SqlDao implements AutoCloseable {
    private final Connection conn;
    private final SqlDataStore ds;
    int holdOpen, transactionLevel;

    public SqlDao(SqlDataStore ds) throws SQLException {
        this.ds = ds;
        this.conn = ds.getDataSource().getConnection();
    }

    // -- Queries

    protected String getSelectGlobalParameterQuery() {
        return "SELECT (`value`) FROM {}global WHERE `key`=?";
    }

    protected abstract String getInsertGlobalParameterQueryUpdating();

    protected String getDeleteGlobalParameterQuery() {
        return "DELETE FROM {}global WHERE `key`=?";
    }

    protected String getGetSubjectRefIdQuery() {
        return "SELECT type, identifier FROM {}subjects WHERE id=?";
    }

    protected String getGetSubjectRefTypeNameQuery() {
        return "SELECT id FROM {}subjects WHERE type=? AND identifier=?";
    }

    protected String getDeleteSubjectIdQuery() {
        return "DELETE FROM {}subjects WHERE id=?";
    }

    protected String getDeleteSubjectTypeNameQuery() {
        return "DELETE FROM {}subjects WHERE type=? AND identifier=?";
    }

    protected String getInsertSubjectTypeNameQuery() {
        return "INSERT INTO {}subjects (type, identifier) VALUES (?, ?)";
    }

    protected String getSelectContextsSegmentQuery() {
        return "SELECT `key`, `value` FROM {}contexts WHERE segment=?";
    }

    protected String getSelectSegmentsSubjectQuery() {
        return "SELECT id, perm_default FROM {}segments WHERE subject=?";
    }

    protected String getSelectPermissionsSegmentQuery() {
        return "SELECT `key`, `value` FROM {}permissions WHERE segment=?";
    }

    protected String getSelectOptionsSegmentQuery() {
        return "SELECT `key`, `value` FROM {}options WHERE segment=?";
    }

    protected String getSelectInheritanceSegmentQuery() {
        return "SELECT * FROM {}inheritance LEFT JOIN ({}subjects) on ({}inheritance.parent={}subjects.id) WHERE segment=?";
    }

    protected String getInsertSegmentQuery() {
        return "INSERT INTO {}segments (subject, perm_default) VALUES (?, ?)";
    }

    protected String getDeleteSegmentIdQuery() {
        return "DELETE FROM {}segments WHERE id=?";
    }

    protected String getSelectSubjectIdentifiersQuery() {
        return "SELECT identifier FROM {}subjects WHERE type=?";
    }

    protected String getSelectSubjectTypesQuery() {
        return "SELECT DISTINCT type FROM {}subjects";
    }

    protected String getDeleteOptionKeyQuery() {
        return "DELETE FROM {}options WHERE segment=? AND `key`=?";
    }

    protected String getDeleteOptionsQuery() {
        return "DELETE FROM {}options WHERE segment=?";
    }

    protected abstract String getInsertOptionUpdatingQuery();

    protected abstract String getInsertPermissionUpdatingQuery();

    protected String getDeletePermissionKeyQuery() {
        return "DELETE FROM {}permissions WHERE segment=? AND `key`=?";
    }

    protected String getDeletePermissionsQuery() {
        return "DELETE FROM {}permissions WHERE segment=?";
    }

    protected String getUpdatePermissionDefaultQuery() {
        return "UPDATE {}segments SET perm_default=? WHERE id=?";
    }

    protected String getInsertInheritanceQuery() {
        return "INSERT INTO {}inheritance (`segment`, `parent`) VALUES (?, ?)";
    }

    protected String getDeleteInheritanceParentQuery() {
        return "DELETE FROM {}inheritance WHERE segment=? AND parent=?";
    }

    protected String getDeleteInheritanceQuery() {
        return "DELETE FROM {}inheritance WHERE segment=?";
    }

    protected String getInsertContextQuery() {
        return "INSERT INTO {}contexts (segment, `key`, `value`) VALUES (?, ?, ?)";
    }

    protected String getDeleteContextQuery() {
        return "DELETE FROM {}contexts WHERE segment=?";
    }

    protected String getSelectContextInheritanceQuery() {
        return "SELECT `child_key`, `child_value`, `parent_key`, `parent_value` FROM {}context_inheritance GROUP BY `child_key`, `child_value`, `parent_key`, `parent_value` ORDER BY `id` ASC";
    }

    protected String getInsertContextInheritanceQuery() {
        return "INSERT INTO {}context_inheritance (child_key, child_value, parent_key, parent_value) VALUES (?, ?, ?, ?)";
    }

    protected String getDeleteContextInheritanceQuery() {
        return "DELETE FROM {}context_inheritance WHERE child_key=? AND child_value=?";
    }

    protected String getSelectRankLadderQuery() {
        return "SELECT `{}rank_ladders`.`id`, `subject`, `type`, `identifier` FROM {}rank_ladders LEFT JOIN (`{}subjects`) ON (`{}rank_ladders`.`subject`=`{}subjects`.`id`) WHERE `name`=? ORDER BY `{}rank_ladders`.`id` ASC";
    }

    protected String getTestRankLadderExistsQuery() {
        return "SELECT `id` FROM {}rank_ladders WHERE `name`=? LIMIT 1";
    }

    protected String getInsertRankLadderQuery() {
        return "INSERT INTO {}rank_ladders (`name`, `subject`) VALUES (?, ?)";
    }

    protected String getDeleteRankLadderQuery() {
        return "DELETE FROM {}rank_ladders WHERE `name`=?";
    }

    protected String getSelectAllRankLadderNamesQuery() {
        return "SELECT DISTINCT `name` FROM {}rank_ladders";
    }

    protected String getSelectAllSubjectsQuery() {
        return "SELECT `id`, `type`, `identifier` FROM {}subjects";
    }

    protected PreparedStatement prepareStatement(String query) throws SQLException {
        return conn.prepareStatement(this.ds.insertPrefix(query));
    }

    protected PreparedStatement prepareStatement(String query, int params) throws SQLException {
        return conn.prepareStatement(this.ds.insertPrefix(query), params);
    }

    protected <T> T executeInTransaction(ThrowingSupplier<T, SQLException> func) throws SQLException {
        transactionLevel++;
        conn.setAutoCommit(false);
        try {
            T ret = func.supply();
            if (--transactionLevel <= 0) {
                conn.commit();
            }
            return ret;
        } finally {
            if (transactionLevel <= 0) {
                conn.setAutoCommit(true);
            }
        }
    }

    // -- Operations

    public Optional<String> getGlobalParameter(String key) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getSelectGlobalParameterQuery())) {
            stmt.setString(1, key);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return Optional.of(rs.getString(1));
            } else {
                return Optional.empty();
            }
        }

    }

    public void setGlobalParameter(String key, String value) throws SQLException {
        if (value == null) {
            try (PreparedStatement stmt = prepareStatement(getDeleteGlobalParameterQuery())) {
                stmt.setString(1, key);
                stmt.executeUpdate();
            }
        } else {
            try (PreparedStatement stmt = prepareStatement(getInsertGlobalParameterQueryUpdating())) {
                stmt.setString(1, key);
                stmt.setString(2, value);
                stmt.executeUpdate();
            }
        }
    }

    public Optional<SubjectRef> getSubjectRef(int id) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getGetSubjectRefIdQuery())) {
            stmt.setInt(1, id);
            ResultSet res = stmt.executeQuery();

            if (!res.next()) {
                return Optional.empty();
            }
            return Optional.of(new SubjectRef(id, res.getString(1), res.getString(2)));
        }
    }

    public Optional<SubjectRef> getSubjectRef(String type, String name) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getGetSubjectRefTypeNameQuery())) {
            stmt.setString(1, type);
            stmt.setString(2, name);
            ResultSet res = stmt.executeQuery();

            if (!res.next()) {
                return Optional.empty();
            }
            return Optional.of(new SubjectRef(res.getInt(1), type, name));
        }
    }

    public boolean removeSubject(SubjectRef ref) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getDeleteSubjectIdQuery())) {
            stmt.setInt(1, ref.getId());
            return stmt.executeUpdate() > 0;
        }
    }

    public boolean removeSubject(String type, String name) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getDeleteSubjectTypeNameQuery())) {
            stmt.setString(1, type);
            stmt.setString(2, name);
            return stmt.executeUpdate() > 0;
        }
    }

    public SubjectRef getOrCreateSubjectRef(String type, String name) throws SQLException {
        final SubjectRef ret = SubjectRef.unresolved(type, name);
        allocateSubjectRef(ret);
        return ret;
    }

    public void allocateSubjectRef(SubjectRef ref) throws SQLException {
        executeInTransaction(() -> {
            try (PreparedStatement stmt = prepareStatement(getGetSubjectRefTypeNameQuery())) {
                stmt.setString(1, ref.getType());
                stmt.setString(2, ref.getIdentifier());
                ResultSet res = stmt.executeQuery();
                if (res.next()) {
                    ref.setId(res.getInt(1));
                } else {
                    try (PreparedStatement addStatement = prepareStatement(getInsertSubjectTypeNameQuery(), Statement.RETURN_GENERATED_KEYS)) {
                        addStatement.setString(1, ref.getType());
                        addStatement.setString(2, ref.getIdentifier());
                        addStatement.executeUpdate();
                        res = addStatement.getGeneratedKeys();
                        res.next();
                        ref.setId(res.getInt(1));
                    }
                }
            }
            return ref;
        });
    }

    public int getIdAllocating(SubjectRef ref) throws SQLException {
        if (ref.isUnallocated()) {
            allocateSubjectRef(ref);
        }
        return ref.getId();
    }


    private Set<Entry<String, String>> getSegmentContexts(int segmentId) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getSelectContextsSegmentQuery())) {
            stmt.setInt(1, segmentId);
            ImmutableSet.Builder<Entry<String, String>> res = ImmutableSet.builder();

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                res.add(Maps.immutableEntry(rs.getString(1), rs.getString(2)));
            }
            return res.build();
        }
    }

    public List<Segment> getSegments(SubjectRef ref) throws SQLException {
        ImmutableList.Builder<Segment> result = ImmutableList.builder();
        try (PreparedStatement stmt = prepareStatement(getSelectSegmentsSubjectQuery())) {
            stmt.setInt(1, getIdAllocating(ref));
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                final int id = rs.getInt(1);
                Number permDef = (Number) rs.getObject(2);
                Set<Entry<String, String>> contexts = getSegmentContexts(id);

                ImmutableMap.Builder<String, Integer> permValues = ImmutableMap.builder();
                ImmutableMap.Builder<String, String> optionValues = ImmutableMap.builder();
                ImmutableList.Builder<SubjectRef> inheritanceValues = ImmutableList.builder();

                try (PreparedStatement permStmt = prepareStatement(getSelectPermissionsSegmentQuery())) {
                    permStmt.setInt(1, id);

                    ResultSet segmentRs = permStmt.executeQuery();
                    while (segmentRs.next()) {
                        permValues.put(segmentRs.getString(1), segmentRs.getInt(2));
                    }
                }

                try (PreparedStatement optStmt = prepareStatement(getSelectOptionsSegmentQuery())) {
                    optStmt.setInt(1, id);

                    ResultSet segmentRs = optStmt.executeQuery();
                    while (segmentRs.next()) {
                        optionValues.put(segmentRs.getString(1), segmentRs.getString(2));
                    }
                }

                try (PreparedStatement inheritStmt = prepareStatement(getSelectInheritanceSegmentQuery())) {
                    inheritStmt.setInt(1, id);

                    ResultSet segmentRs = inheritStmt.executeQuery();
                    while (segmentRs.next()) {
                        inheritanceValues.add(new SubjectRef(segmentRs.getInt(3), segmentRs.getString(4), segmentRs.getString(5)));
                    }
                }

                result.add(new Segment(id, contexts, permValues.build(), optionValues.build(), inheritanceValues.build(), permDef == null ? null : permDef.intValue(), null));

            }
        }
        return result.build();
    }

    public Segment addSegment(SubjectRef ref) throws SQLException { // TODO: Is this method useful?
        Segment segment = Segment.unallocated();
        allocateSegment(ref, segment);
        return segment;
    }

    public void updateFullSegment(SubjectRef ref, Segment segment) throws SQLException {
        executeInTransaction(() -> {
            allocateSegment(ref, segment);
            setContexts(segment, segment.getContexts());
            setOptions(segment, segment.getOptions());
            setParents(segment, segment.getParents());
            setPermissions(segment, segment.getPermissions());
            setDefaultValue(segment, segment.getPermissionDefault());
            return null;
        });
    }

    public void setContexts(Segment seg, Set<Entry<String, String>> contexts) throws SQLException {
        // Update contexts
        executeInTransaction(() -> {
            try (PreparedStatement delete = prepareStatement(getDeleteContextQuery());
                 PreparedStatement insert = prepareStatement(getInsertContextQuery())) {
                delete.setInt(1, seg.getId());
                delete.executeUpdate();

                insert.setInt(1, seg.getId());

                for (Map.Entry<String, String> context : contexts) {
                    insert.setString(2, context.getKey());
                    insert.setString(3, context.getValue());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            return null;
        });

    }

    public void allocateSegment(SubjectRef subject, Segment val) throws SQLException {
        if (!val.isUnallocated()) {
            return;
        }

        try (PreparedStatement stmt = prepareStatement(getInsertSegmentQuery(), Statement.RETURN_GENERATED_KEYS)) {
            stmt.setInt(1, getIdAllocating(subject));
            if (val.getPermissionDefault() == null) {
                stmt.setNull(2, Types.INTEGER);
            } else {
                stmt.setInt(2, val.getPermissionDefault());
            }

            stmt.executeUpdate();
            ResultSet res = stmt.getGeneratedKeys();
            res.next();
            val.setId(res.getInt(1));
        }
        setContexts(val, val.getContexts());
    }

    public boolean removeSegment(Segment segment) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getDeleteSegmentIdQuery())) {
            stmt.setInt(1, segment.getId());
            return stmt.executeUpdate() > 0;
        }
    }

    public Set<String> getAllIdentifiers(String type) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getSelectSubjectIdentifiersQuery())) {
            stmt.setString(1, type);

            ResultSet rs = stmt.executeQuery();
            ImmutableSet.Builder<String> ret = ImmutableSet.builder();

            while (rs.next()) {
                ret.add(rs.getString(1));
            }


            return ret.build();
        }
    }

    public Set<String> getRegisteredTypes() throws SQLException {
        try (ResultSet rs = prepareStatement(getSelectSubjectTypesQuery()).executeQuery()) {
            ImmutableSet.Builder<String> ret = ImmutableSet.builder();

            while (rs.next()) {
                ret.add(rs.getString(1));
            }

            return ret.build();
        }
    }

    public void initializeTables() throws SQLException {
        if (hasTable("{}permissions")) {
            return;
        }
        String database = conn.getMetaData().getDatabaseProductName().toLowerCase();
        try (Statement stmt = conn.createStatement()) {
            try (InputStream res = SqlDao.class.getResourceAsStream("deploy/" + database + ".sql")) {
                if (res == null) {
                    throw new SQLException("No initial schema available for " + database + " databases!");
                }
                try (BufferedReader read = new BufferedReader(new InputStreamReader(res, StandardCharsets.UTF_8))) {
                    StringBuilder currentQuery = new StringBuilder();
                    String line;
                    while ((line = read.readLine()) != null) {
                        if (line.startsWith("--")) {
                            continue;
                        }

                        currentQuery.append(line);
                        if (line.endsWith(";")) {
                            currentQuery.deleteCharAt(currentQuery.length() - 1);
                            String queryLine = currentQuery.toString().trim();
                            currentQuery = new StringBuilder();
                            if (!queryLine.isEmpty()) {
                                stmt.addBatch(ds.insertPrefix(queryLine));
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new SQLException(e);
            }
            stmt.executeBatch();
        }
    }

    private boolean hasTable(String table) throws SQLException {
        return conn.getMetaData().getTables(null, null, this.ds.insertPrefix(table).toUpperCase(), null).next(); // Upper-case for H2
    }

    public void clearOption(Segment segment, String option) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getDeleteOptionKeyQuery())) {
            stmt.setInt(1, segment.getId());
            stmt.setString(2, option);
            stmt.executeUpdate();
        }
    }

    public void setOptions(Segment seg, Map<String, String> options) throws SQLException {
        executeInTransaction(() -> {
            try (PreparedStatement del = prepareStatement(getDeleteOptionsQuery());
                 PreparedStatement ins = prepareStatement(getInsertOptionUpdatingQuery())) {
                del.setInt(1, seg.getId());
                del.executeUpdate();

                if (options != null) {
                    ins.setInt(1, seg.getId());
                    for (Map.Entry<String, String> ent : options.entrySet()) {
                        ins.setString(2, ent.getKey());
                        ins.setString(3, ent.getValue());
                        ins.addBatch();
                    }
                    ins.executeBatch();
                }
            }
            return null;
        });
    }

    public void setOption(Segment segment, String key, String value) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getInsertOptionUpdatingQuery())) {
            stmt.setInt(1, segment.getId());
            stmt.setString(2, key);
            stmt.setString(3, value);
            stmt.executeUpdate();
        }
    }

    public void setPermission(Segment segment, String permission, int value) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getInsertPermissionUpdatingQuery())) {
            stmt.setInt(1, segment.getId());
            stmt.setString(2, permission);
            stmt.setInt(3, value);
            stmt.executeUpdate();
        }

    }

    public void clearPermission(Segment segment, String permission) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getDeletePermissionKeyQuery())) {
            stmt.setInt(1, segment.getId());
            stmt.setString(2, permission);
            stmt.executeUpdate();
        }
    }

    public void setPermissions(Segment segment, Map<String, Integer> permissions) throws SQLException {
        executeInTransaction(() -> {
            try (PreparedStatement del = prepareStatement(getDeletePermissionsQuery());
                 PreparedStatement ins = prepareStatement(getInsertPermissionUpdatingQuery())) {
                del.setInt(1, segment.getId());
                del.executeUpdate();

                if (permissions != null) {
                    ins.setInt(1, segment.getId());
                    for (Map.Entry<String, Integer> ent : permissions.entrySet()) {
                        ins.setString(2, ent.getKey());
                        ins.setInt(3, ent.getValue());
                        ins.addBatch();
                    }
                    ins.executeBatch();
                }
            }
            return null;
        });
    }

    public void setDefaultValue(Segment segment, Integer permissionDefault) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getUpdatePermissionDefaultQuery())) {
            if (permissionDefault == null) {
                stmt.setNull(1, Types.INTEGER);
            } else {
                stmt.setInt(1, permissionDefault);
            }
            stmt.setInt(2, segment.getId());
            stmt.executeUpdate();
        }
    }

    public void addParent(Segment seg, SubjectRef parent) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getInsertInheritanceQuery())) {
            stmt.setInt(1, seg.getId());
            stmt.setInt(2, getIdAllocating(parent));
            stmt.executeUpdate();
        }
    }

    public void removeParent(Segment segment, SubjectRef parent) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getDeleteInheritanceParentQuery())) {
            stmt.setInt(1, segment.getId());
            stmt.setInt(2, getIdAllocating(parent));
            stmt.executeUpdate();
        }
    }

    public void setParents(Segment segment, List<SubjectRef> parents) throws SQLException {
        executeInTransaction(() -> {
            try (PreparedStatement del = prepareStatement(getDeleteInheritanceQuery());
                 PreparedStatement ins = prepareStatement(getInsertInheritanceQuery())) {
                del.setInt(1, segment.getId());
                del.executeUpdate();

                if (parents != null) {
                    ins.setInt(1, segment.getId());
                    for (SubjectRef ent : parents) {
                        ins.setInt(2, getIdAllocating(ent));
                        ins.addBatch();
                    }
                    ins.executeBatch();
                }
            }
            return null;
        });
    }

    public SqlContextInheritance getContextInheritance() throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getSelectContextInheritanceQuery())) {
            ImmutableMap.Builder<Entry<String, String>, List<Entry<String, String>>> ret = ImmutableMap.builder();
            Entry<String, String> current = null;
            ImmutableList.Builder<Entry<String, String>> builder = null;
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                final String childKey = rs.getString(1),
                        childValue = rs.getString(2),
                        parentKey = rs.getString(3),
                        parentValue = rs.getString(4);
                if (current == null || !childKey.equals(current.getKey()) || !childValue.equals(current.getValue())) {
                    if (current != null && builder != null) {
                        ret.put(current, builder.build());
                    }
                    current = Maps.immutableEntry(childKey, childValue);
                    builder = ImmutableList.builder();
                }
                builder.add(Maps.immutableEntry(parentKey, parentValue));
            }

            if (current != null) {
                ret.put(current, builder.build());
            }

            return new SqlContextInheritance(ret.build(), null);
        }

    }

    public void setContextInheritance(Entry<String, String> child, List<Entry<String, String>> parents) throws SQLException {
        executeInTransaction(() -> {
            try (PreparedStatement delete = prepareStatement(getDeleteContextInheritanceQuery());
            PreparedStatement insert = prepareStatement(getInsertContextInheritanceQuery())) {
                delete.setString(1, child.getKey());
                delete.setString(2, child.getValue());
                delete.executeUpdate();

                if (parents != null && parents.size() > 0) {
                    insert.setString(1, child.getKey());
                    insert.setString(2, child.getValue());
                    for (Entry<String, String> parent : parents) {
                        insert.setString(3, parent.getKey());
                        insert.setString(4, parent.getValue());
                        insert.addBatch();
                    }
                    insert.executeBatch();
                }
            }
            return null;
        });
    }

    public RankLadder getRankLadder(String name) throws SQLException {
        ImmutableList.Builder<SubjectRef> elements = ImmutableList.builder();
        try (PreparedStatement stmt = prepareStatement(getSelectRankLadderQuery())) {
            stmt.setString(1, name);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                elements.add(new SubjectRef(rs.getInt(2), rs.getString(3), rs.getString(4)));
            }
        }
        return new SqlRankLadder(name, elements.build());
    }

    public boolean hasEntriesForRankLadder(String name) throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getTestRankLadderExistsQuery())) {
            stmt.setString(1, name);
            return stmt.executeQuery().next();
        }
    }

    public void setRankLadder(String name, RankLadder ladder) throws SQLException {
        executeInTransaction(() -> {
            List<SubjectRef> ranks;
            if (ladder == null) {
                ranks = ImmutableList.of();
            } else if (ladder instanceof SqlRankLadder) {
                ranks = ((SqlRankLadder) ladder).getRanks();
            } else {
                ranks = Lists.transform(ladder.getRanks(), rank -> rank instanceof SubjectRef ? (SubjectRef) rank : SubjectRef.unresolved(rank.getKey(), rank.getValue()));
            }

            try (PreparedStatement delete = prepareStatement(getDeleteRankLadderQuery());
                PreparedStatement insert = prepareStatement(getInsertRankLadderQuery())) {
                delete.setString(1, name);
                delete.executeUpdate();

                if (ladder != null) {
                    insert.setString(1, name);
                    for (SubjectRef ref : ranks) {
                        insert.setInt(2, getIdAllocating(ref));
                        insert.addBatch();
                    }
                    insert.executeBatch();
                }
            }
            return null;
        });
    }

    public Iterable<String> getAllRankLadderNames() throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getSelectAllRankLadderNamesQuery())) {
            ImmutableSet.Builder<String> ret = ImmutableSet.builder();
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                ret.add(rs.getString(1));
            }
            return ret.build();
        }
    }

    public Iterable<SubjectRef> getAllSubjectRefs() throws SQLException {
        try (PreparedStatement stmt = prepareStatement(getSelectAllSubjectsQuery())) {
            ImmutableSet.Builder<SubjectRef> ret = ImmutableSet.builder();
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                ret.add(new SubjectRef(rs.getInt(1), rs.getString(2), rs.getString(3)));
            }
            return ret.build();
        }
    }

    @Override
    public void close() throws SQLException {
        if (this.holdOpen <= 0) {
            this.conn.close();
        }
    }
}
