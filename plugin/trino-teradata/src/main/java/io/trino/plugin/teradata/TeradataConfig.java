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
package io.trino.plugin.teradata;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class TeradataConfig
{
    private String oidcJWTToken;
    private String oidcClientSecret;
    private String oidcJWSCertificate;
    private String oidcJWSPrivateKey;
    private String oidcClientId;
    private String logMech = "TD2";
    private TeradataCaseSensitivity teradataCaseSensitivity = TeradataCaseSensitivity.CASE_SENSITIVE;
    private String viewMetadataSchema = "trino_metadata";

    public String getOidcClientId()
    {
        return oidcClientId;
    }

    @Config("oidc.client-id")
    public TeradataConfig setOidcClientId(String clientId)
    {
        this.oidcClientId = clientId;
        return this;
    }

    public String getOidcJWSPrivateKey()
    {
        return oidcJWSPrivateKey;
    }

    @Config("oidc.jws-private-key")
    public TeradataConfig setOidcJWSPrivateKey(String privateKey)
    {
        this.oidcJWSPrivateKey = privateKey;
        return this;
    }

    public String getOidcJWSCertificate()
    {
        return oidcJWSCertificate;
    }

    @Config("oidc.jws-certificate")
    public TeradataConfig setOidcJWSCertificate(String certificate)
    {
        this.oidcJWSCertificate = certificate;
        return this;
    }

    public String getOidcClientSecret()
    {
        return oidcClientSecret;
    }

    @Config("oidc.client-secret")
    public TeradataConfig setOidcClientSecret(String clientSecret)
    {
        this.oidcClientSecret = clientSecret;
        return this;
    }

    public String getOidcJwtToken()
    {
        return oidcJWTToken;
    }

    @Config("jwt.token")
    public TeradataConfig setOidcJwtToken(String jwtToken)
    {
        this.oidcJWTToken = jwtToken;
        return this;
    }

    public String getLogMech()
    {
        return logMech;
    }

    @Config("logon-mechanism")
    @ConfigDescription("Specifies the logon mechanism for Teradata (default: TD2). Use 'TD2' for TD2 authentication.")
    public TeradataConfig setLogMech(String logMech)
    {
        this.logMech = logMech;
        return this;
    }

    @NotNull
    public TeradataCaseSensitivity getTeradataCaseSensitivity()
    {
        return teradataCaseSensitivity;
    }

    @Config("teradata.case-sensitivity")
    @ConfigDescription("How char/varchar columns' case sensitivity will be exposed to Trino (default: CASE_SENSITIVE). Possible values: CASE_INSENSITIVE, CASE_SENSITIVE, AS_DEFINED.")
    public TeradataConfig setTeradataCaseSensitivity(TeradataCaseSensitivity teradataCaseSensitivity)
    {
        this.teradataCaseSensitivity = teradataCaseSensitivity;
        return this;
    }

    @NotBlank
    public String getViewMetadataSchema()
    {
        return viewMetadataSchema;
    }

    @Config("teradata.view-metadata-schema")
    @ConfigDescription("Schema name in Teradata used to store Trino view definitions (auto-created on first use)")
    public TeradataConfig setViewMetadataSchema(String viewMetadataSchema)
    {
        this.viewMetadataSchema = viewMetadataSchema;
        return this;
    }

    public enum TeradataCaseSensitivity
    {
        CASE_INSENSITIVE, CASE_SENSITIVE, AS_DEFINED
    }
}
