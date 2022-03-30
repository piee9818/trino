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

package io.trino.plugin.kafka;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import javax.inject.Inject;

import java.util.Properties;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;

public class PlainTextKafkaAdminFactory
        implements KafkaAdminFactory
{
    private final Set<HostAddress> nodes;
    private final SecurityProtocol securityProtocol;
    private String ncpSecurityProtocol;
    private String ncpSaslMechanism;
    private String ncpSaslJaasConfig;

    @Inject
    public PlainTextKafkaAdminFactory(
            KafkaConfig kafkaConfig,
            KafkaSecurityConfig securityConfig)
    {
        requireNonNull(kafkaConfig, "kafkaConfig is null");
        requireNonNull(securityConfig, "securityConfig is null");

        nodes = kafkaConfig.getNodes();
        securityProtocol = securityConfig.getSecurityProtocol();
        ncpSecurityProtocol = kafkaConfig.getNcpSecurityProtocol();
        ncpSaslMechanism = kafkaConfig.getNcpSaslMechanism();
        ncpSaslJaasConfig = kafkaConfig.getNcpSaslJaasConfig();
    }

    @Override
    public Properties configure(ConnectorSession session)
    {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, nodes.stream()
                .map(HostAddress::toString)
                .collect(joining(",")));
        properties.setProperty(SECURITY_PROTOCOL_CONFIG, securityProtocol.name());
        if (ncpSecurityProtocol != null) {
            properties.setProperty(SECURITY_PROTOCOL_CONFIG, ncpSecurityProtocol);
        }
        if (ncpSaslMechanism != null) {
            properties.setProperty(SASL_MECHANISM, ncpSaslMechanism);
        }
        if (ncpSaslJaasConfig != null) {
            properties.setProperty(SASL_JAAS_CONFIG, ncpSaslJaasConfig);
        }

        return properties;
    }
}
