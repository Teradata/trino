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
package io.trino.plugin.teradata.integration;

import java.util.Map;

public record BearerAuth(String jwsPrivateKey, String jwsCertificate, String clientId)
        implements AuthConfig
{
    @Override
    public void populateProps(Map<String, String> props)
    {
        props.put("jws_private_key", jwsPrivateKey);
        props.put("jws_cert", jwsCertificate);
        props.put("oidc_clientid", clientId);
    }
}
