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

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static java.util.Collections.singleton;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import javax.jms.JMSException;
import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.trellisldp.spi.Event;
import org.trellisldp.spi.EventService;
import org.trellisldp.vocabulary.AS;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.Trellis;

/**
 * @author acoburn
 */
@RunWith(MockitoJUnitRunner.class)
public class JmsPublisherTest {

    private static final RDF rdf = new SimpleRDF();

    private final String queueName = "queue";

    @Mock
    private Connection mockConnection;

    @Mock
    private Session mockSession;

    @Mock
    private Event mockEvent;

    @Mock
    private Queue mockQueue;

    @Mock
    private TextMessage mockMessage;

    @Mock
    private MessageProducer mockProducer;

    @Before
    public void setUp() throws JMSException {
        when(mockEvent.getTarget()).thenReturn(of(rdf.createIRI("trellis:repository/resource")));
        when(mockEvent.getAgents()).thenReturn(singleton(Trellis.RepositoryAdministrator));
        when(mockEvent.getIdentifier()).thenReturn(rdf.createIRI("urn:test"));
        when(mockEvent.getTypes()).thenReturn(singleton(AS.Update));
        when(mockEvent.getTargetTypes()).thenReturn(singleton(LDP.RDFSource));
        when(mockEvent.getInbox()).thenReturn(empty());

        when(mockConnection.createSession(anyBoolean(), eq(AUTO_ACKNOWLEDGE))).thenReturn(mockSession);
        when(mockSession.createQueue(eq(queueName))).thenReturn(mockQueue);
        when(mockSession.createTextMessage(anyString())).thenReturn(mockMessage);
        when(mockSession.createProducer(any(Queue.class))).thenReturn(mockProducer);

        doNothing().when(mockProducer).send(any(TextMessage.class));
    }

    @Test
    public void testJms() throws JMSException {
        final EventService svc = new JmsPublisher(mockConnection, queueName);
        svc.emit(mockEvent);

        verify(mockProducer).send(eq(mockMessage));
    }

    @Test
    public void testError() throws JMSException {
        doThrow(JMSException.class).when(mockProducer).send(eq(mockMessage));

        final EventService svc = new JmsPublisher(mockSession, queueName);
        svc.emit(mockEvent);

        verify(mockProducer).send(eq(mockMessage));
    }
}
