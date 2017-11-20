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

package io.crate.operation.user;

import io.crate.user.SecureHash;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AlterUserRequest extends AcknowledgedRequest<AlterUserRequest> {
    private String userName;
    private SecureHash secureHash;

    public AlterUserRequest() {
    }

    public AlterUserRequest(String userName, @Nullable SecureHash secureHash) {

        this.userName = userName;
        this.secureHash = secureHash;
    }

    public String userName() {
        return userName;
    }

    @Nullable
    public SecureHash secureHash() {
        return secureHash;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (userName == null) {
            return ValidateActions.addValidationError("userName is missing", null);
        }
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        super.readFrom(in);
        userName = in.readString();
        secureHash = in.readOptionalWriteable(SecureHash::readFrom);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(userName);
        out.writeOptionalWriteable(secureHash);
    }
}
