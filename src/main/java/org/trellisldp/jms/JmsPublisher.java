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
package org.trellisldp.jms;

import static java.lang.System.getProperty;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.spi.EventService.serialize;

import java.io.IOException;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.trellisldp.spi.Event;
import org.trellisldp.spi.EventService;

/**
 * A JMS message producer capable of publishing messages to a JMS broker such as ActiveMQ.
 *
 * @author acoburn
 */
public class JmsPublisher implements EventService, AutoCloseable {

    private static final Logger LOGGER = getLogger(JmsPublisher.class);

    private static final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();

    private final Connection conn;

    private final MessageProducer producer;

    private final Session session;

    /**
     * Create a new JMS Publisher
     * @throws IOException when there is an error connecting to the JMS broker
     * @throws JMSException when there is a JMS error
     */
    public JmsPublisher() throws IOException, JMSException {
        this(getProperty("trellis.jms.uri"), "event", null, null);
    }

    /**
     * Create a new JMS Publisher
     * @param uri the connection URI
     * @param queueName the name of the queue
     * @param username the username
     * @param password the password
     * @throws IOException when there is an error connecting to the JMS broker
     * @throws JMSException when there is a JMS error
     */
    public JmsPublisher(final String uri, final String queueName, final String username, final String password)
            throws IOException, JMSException {
        requireNonNull(uri);
        requireNonNull(queueName);

        factory.setBrokerURL(uri);
        if (nonNull(username) && nonNull(password)) {
            factory.setUserName(username);
            factory.setPassword(password);
        }

        conn = factory.createConnection();
        conn.start();
        session = conn.createSession(false, AUTO_ACKNOWLEDGE);
        producer = session.createProducer(session.createQueue(queueName));
    }

    @Override
    public void emit(final Event event) {
        requireNonNull(event, "Cannot emit a null event!");

        serialize(event).ifPresent(json -> {
            try {
                final Message message = session.createTextMessage(json);
                message.setStringProperty("Content-Type", "application/ld+json");
                producer.send(message);
            } catch (final JMSException ex) {
                LOGGER.error("Error writing to broker: {}", ex.getMessage());
            }
        });
    }

    @Override
    public void close() throws JMSException {
        conn.close();
    }
}
