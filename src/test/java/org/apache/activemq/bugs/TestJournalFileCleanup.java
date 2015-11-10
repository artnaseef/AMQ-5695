package org.apache.activemq.bugs;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by art on 6/4/15.
 */
public class TestJournalFileCleanup {
  private static final Logger log = LoggerFactory.getLogger(TestJournalFileCleanup.class);

  private static final int KB = 1024;
  private static File dataDirectory;
  private static File kahaDbDirectory;

  @BeforeClass
  public static void configure() {
    String dataParentDir = System.getProperty("org.apache.activemq.bugs.data_parent_dir");
    assertNotNull(dataParentDir);

    dataDirectory = new File(dataParentDir, "testKahaDBFileCleanupFail");
    kahaDbDirectory = new File(dataDirectory, "kahadb");
  }

  @Test(timeout = 60000)
  public void testJournalFileCleanup() throws Exception {
    //
    // Start the broker once with an empty directory.
    //

    FileUtils.deleteDirectory(dataDirectory);

    BrokerService brokerService;

    brokerService = this.setupBrokerService();

    brokerService.start();
    brokerService.waitUntilStarted();

    this.sendMessages(brokerService, "test.queue.00", 1);
    this.sendMessages(brokerService, "test.queue.01", 50);

    assertEquals(51, brokerService.getAdminView().getTotalMessageCount());


    // Acknowledge them all
    this.consumeMessages(brokerService, "test.queue.01", 49);
    assertEquals(2, brokerService.getAdminView().getTotalMessageCount());

    // Produce one more, and consume the last of the initial 50
    this.sendMessages(brokerService, "test.queue.01", 1);
    this.consumeMessages(brokerService, "test.queue.01", 1);
    assertEquals(2, brokerService.getAdminView().getTotalMessageCount());

    Thread.sleep(1000);

    brokerService.stop();
    brokerService.waitUntilStopped();



    //
    // PART 2 - reload the same broker (using a newly-created broker service) and validate the
    //          counts.
    //

//    brokerService = this.setupBrokerService();
//
//    brokerService.start();
//    brokerService.waitUntilStarted();
//
//    long count = brokerService.getAdminView().getTotalMessageCount();
//    assertEquals(0, count);
  }

  /**
   * Prepare the broker service for testing.
   * @return
   * @throws Exception
   */
  protected BrokerService setupBrokerService() throws Exception {
    BrokerService brokerService = new BrokerService();
    brokerService.setDataDirectory(dataDirectory.getPath());
    brokerService.setPersistent(true);
    brokerService.setUseJmx(true);

    KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
    persistenceAdapter.setDirectory(kahaDbDirectory);
    persistenceAdapter.setJournalMaxFileLength(5 * KB);
    persistenceAdapter.setCleanupInterval(1000);
    brokerService.setPersistenceAdapter(persistenceAdapter);

    return brokerService;
  }

  protected void sendMessages(BrokerService brokerService, String queueName, int count)
      throws Exception {

    ActiveMQConnection connection = ActiveMQConnection
        .makeConnection(brokerService.getVmConnectorURI().toString());

    try {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(new ActiveMQQueue(queueName));

      int cur;
      cur = 0;

      while (cur < count) {
        TextMessage textMessage = session.createTextMessage(String.format("message #%05d", cur));
        producer.send(textMessage);
        cur++;
      }
    } finally {
      connection.close();
    }
  }

  protected void consumeMessages(BrokerService brokerService, String queueName, int max)
      throws Exception {

    ActiveMQConnection connection = ActiveMQConnection
        .makeConnection(brokerService.getVmConnectorURI().toString());

    try {
      connection.start();
      Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(new ActiveMQQueue(queueName));

      int cur;
      cur = 0;

      log.info("Starting to consume up to {} messages on queue '{}'", max, queueName);

      Message msg = consumer.receive(100);
      while ((cur < max) && (msg != null)) {
        msg.acknowledge();

        msg = consumer.receive(100);
        cur++;
      }

      log.info("Finsihed consuming {} messages with max {} on queue '{}'", cur, max, queueName);
    } finally {
      connection.close();
    }
  }
}
