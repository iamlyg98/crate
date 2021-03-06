/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport;

import io.crate.Constants;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.collect.MapBuilder;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlterTableOperationTest extends CrateUnitTest {


    @Test
    public void testPrepareAlterTableMappingRequest() throws Exception {
        Map<String, Object> oldMapping = MapBuilder.<String, Object>newMapBuilder()
            .put("properties", MapBuilder.<String, String>newMapBuilder().put("foo", "foo").map())
            .put("_meta", MapBuilder.<String, String>newMapBuilder().put("meta1", "val1").map())
            .map();

        Map<String, Object> newMapping = MapBuilder.<String, Object>newMapBuilder()
            .put("properties", MapBuilder.<String, String>newMapBuilder().put("foo", "bar").map())
            .put("_meta", MapBuilder.<String, String>newMapBuilder()
                .put("meta1", "v1")
                .put("meta2", "v2")
                .map())
            .map();

        PutMappingRequest request = AlterTableOperation.preparePutMappingRequest(oldMapping, newMapping);

        assertThat(request.type(), is(Constants.DEFAULT_MAPPING_TYPE));
        assertThat(request.source(), is("{\"_meta\":{\"meta2\":\"v2\",\"meta1\":\"v1\"},\"properties\":{\"foo\":\"bar\"}}"));
    }
}
