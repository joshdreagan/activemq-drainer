/*
 * Copyright (C) Red Hat, Inc.
 * http://www.redhat.com
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
package org.jboss.examples.activemq;

import java.io.File;
import java.io.IOException;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageDrainer {

  private static final Logger log = LoggerFactory.getLogger(MessageDrainer.class);

  private static class Options {

    @Option(name = "--kahaDB", usage = "Sets the location of the KahaDB directory.", required = true)
    String kahaDB;

    @Option(name = "--username", usage = "Sets the username for the target broker.")
    String username;

    @Option(name = "--password", usage = "Sets the password for the target broker.")
    String password = "";

    @Option(name = "--brokerURL", usage = "Sets the URL for the target broker.", required = true)
    String brokerURL;
  }

  public static void main(String[] args) throws Throwable {

    Options cmdLineOptions = new Options();
    CmdLineParser cmdLineParser = new CmdLineParser(cmdLineOptions);
    try {
      cmdLineParser.parseArgument(args);
    } catch (CmdLineException e) {
      System.out.println(String.format("Error parsing command line arguments: %s", e.getMessage()));
      cmdLineParser.printUsage(System.out);
    }
    
    File kahaDB = new File(cmdLineOptions.kahaDB);
    if (!kahaDB.exists() || !kahaDB.isDirectory()) {
      throw new IOException("The --kahaDB option must point to a valid KahaDB directory.");
    }

    BrokerService broker = new BrokerService();
    PersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
    persistenceAdapter.setDirectory(kahaDB);
    broker.setPersistenceAdapter(persistenceAdapter);
    broker.start();

    ConnectionFactory localConnectionFactory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
    Connection localConnection = localConnectionFactory.createConnection();
    Session localSession = localConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

    ConnectionFactory remoteConnectionFactory = new ActiveMQConnectionFactory(cmdLineOptions.brokerURL);
    Connection remoteConnection = null;
    if (cmdLineOptions.username != null && !cmdLineOptions.username.trim().isEmpty()) {
      remoteConnection = remoteConnectionFactory.createConnection(cmdLineOptions.username, cmdLineOptions.password);
    } else {
      remoteConnection = remoteConnectionFactory.createConnection();
    }
    Session remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    localConnection.start();
    for (Destination destination : broker.getBroker().getDestinations()) {
      if (destination instanceof Queue && !broker.checkQueueSize(((Queue) destination).getQueueName())) {
        log.info(String.format("Migrating messages for '%s' to '%s'...", destination.toString(), cmdLineOptions.brokerURL));
        long migratedMessageCount = 0;
        MessageConsumer localConsumer = localSession.createConsumer(destination);
        MessageProducer remoteProducer = remoteSession.createProducer(destination);
        Message message = null;
        do {
          message = localConsumer.receive(1000L);
          if (message != null) {
            remoteProducer.send(message);
            message.acknowledge();
            ++migratedMessageCount;
          }
        } while (message != null || !broker.checkQueueSize(((Queue) destination).getQueueName()));
        remoteProducer.close();
        localConsumer.close();
        log.info(String.format("Finished migrating %s messages for '%s' to '%s'.", migratedMessageCount, destination.toString(), cmdLineOptions.brokerURL));
      }
    }

    remoteSession.close();
    remoteConnection.close();

    localSession.close();
    localConnection.close();

    broker.stop();
  }
}
