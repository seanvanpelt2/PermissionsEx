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

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;

public class SubjectRef {
    private volatile int id;
    private final String type, identifier;

    SubjectRef(int id, String type, String identifier) {
        this.id = id;
        this.type = type;
        this.identifier = identifier;
    }

    public static SubjectRef unresolved(String type, String name) {
        return new SubjectRef(SqlConstants.UNALLOCATED, type, name);
    }

    public int getId() {
        if (id == SqlConstants.UNALLOCATED) {
            throw new IllegalStateException("Unallocated SubjectRef tried to be used!");
        }
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    boolean isUnallocated() {
        return id == SqlConstants.UNALLOCATED;
    }

    public String getType() {
        return type;
    }

    public String getIdentifier() {
        return identifier;
    }

    public Map.Entry<String, String> toEntry() {
        return Maps.immutableEntry(type, identifier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SubjectRef)) return false;
        SubjectRef that = (SubjectRef) o;
        return Objects.equals(type, that.type) &&
                Objects.equals(identifier, that.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, identifier);
    }
}
