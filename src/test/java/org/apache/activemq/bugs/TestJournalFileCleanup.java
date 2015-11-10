package org.apache.activemq.bugs;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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

  private KahaDBPersistenceAdapter kahaDBPersistenceAdapter;

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

//    this.sendMessages(brokerService, "test.queue.00", 1);
    this.sendMessages(brokerService, "test.queue.01", 10);

    Thread.sleep(25);
    this.logJournalFileInfo("HAVE {} log files (#{} ... #{}) after producing 10 messages");

    this.performKahaDBCleanup(brokerService);
    this.logJournalFileInfo(
        "HAVE {} log files (#{} ... #{}) after producing 10 messages and kahadb cleanup");


    assertEquals(10, brokerService.getAdminView().getTotalMessageCount());


    // Consume all but acknowledge none; do so more than once.
    int totalUnackCycles = 3;
    int cur = 0;
    while (cur < totalUnackCycles) {
      cur++;

      this.consumeMessages(brokerService, "test.queue.01", 10, false);
      assertEquals(10, brokerService.getAdminView().getTotalMessageCount());
      this.logJournalFileInfo("HAVE {} log files (#{} ... #{}) after producing 10 messages and kahadb cleanup; iter={}", cur);
    }

    // Acknowledge them all
    this.consumeMessages(brokerService, "test.queue.01", 10, true);
    assertEquals(0, brokerService.getAdminView().getTotalMessageCount());
    this.logJournalFileInfo("HAVE {} log files (#{} ... #{}) after consuming 10 messages");

    this.performKahaDBCleanup(brokerService);
    this.logJournalFileInfo("HAVE {} log files (#{} ... #{}) after consuming 10 messages and kahaDB cleanup");

    // Produce one more, and consume the last of the initial 50
    this.sendMessages(brokerService, "test.queue.01", 1);
    this.consumeMessages(brokerService, "test.queue.01", 1, true);
    assertEquals(0, brokerService.getAdminView().getTotalMessageCount());


    this.performKahaDBCleanup(brokerService);
    this.logJournalFileInfo("FINALLY HAVE {} log files (#{} ... #{})");


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

//  @Ignore
//  @Test(timeout = 60000)
//  public void testDeleteFileAndRestart() throws Exception {
//    //
//    // Start the broker once with an empty directory.
//    //
//
//    FileUtils.deleteDirectory(dataDirectory);
//
//    BrokerService brokerService;
//
//    brokerService = this.setupBrokerService();
//
//    brokerService.start();
//    brokerService.waitUntilStarted();
//
////    this.sendMessages(brokerService, "test.queue.00", 1);
//    this.sendMessages(brokerService, "test.queue.01", 10);
//
//    File Db1File = new File(kahaDbDirectory, "db-1.log");
//    Db1File.delete();
//
//
//    this.restartBrokerService(brokerService);
//
//
//    assertEquals(10, brokerService.getAdminView().getTotalMessageCount());
//
//
//    // Acknowledge them all
//    this.consumeMessages(brokerService, "test.queue.01", 10, true);
//    assertEquals(0, brokerService.getAdminView().getTotalMessageCount());
//
//    // Produce one more, and consume the last of the initial 50
//    this.sendMessages(brokerService, "test.queue.01", 1);
//    this.consumeMessages(brokerService, "test.queue.01", 1, true);
//    assertEquals(0, brokerService.getAdminView().getTotalMessageCount());
//
//    Thread.sleep(1000);
//
//    brokerService.stop();
//    brokerService.waitUntilStopped();
//
//
//
//    //
//    // PART 2 - reload the same broker (using a newly-created broker service) and validate the
//    //          counts.
//    //
//
////    brokerService = this.setupBrokerService();
////
////    brokerService.start();
////    brokerService.waitUntilStarted();
////
////    long count = brokerService.getAdminView().getTotalMessageCount();
////    assertEquals(0, count);
//  }

  protected void logJournalFileInfo(String pattern, Object... additionalDetails) {
    List<Object> details = new LinkedList<Object>();

    File[] journalFiles = this.findJournalFiles(kahaDbDirectory);
    int[] lowHighFileNums = this.getJournalFileLowHigh(journalFiles);

    details.add(journalFiles.length);
    details.add(lowHighFileNums[0]);
    details.add(lowHighFileNums[1]);
    details.addAll(Arrays.asList(additionalDetails));

    log.info(pattern, details.toArray(new Object[details.size()]));
  }

  protected File[] findJournalFiles(File kahaDirectory) {
    File [] journalFiles = kahaDirectory.listFiles(new FileFilter() {
      public boolean accept(File pathname) {
        return pathname.getName().matches("db-[0-9]+\\.log");
      }
    });

    return journalFiles;
  }

  protected int[] getJournalFileLowHigh (File[] journalFiles) {
    int[] result = new int[2];

    if (journalFiles.length > 0) {
      result[0] = extractFileNumber(journalFiles[0]);
      result[1] = result[0];

      for (File oneFile : journalFiles) {
        int num = extractFileNumber(oneFile);
        if (num < result[0]) {
          result[0] = num;
        }
        if (num > result[1]) {
          result[1] = num;
        }
      }
    } else {
      result[0] = -1;
      result[1] = -1;
    }

    return result;
  }

  protected int extractFileNumber(File file) {
    String numPart = file.getName().replace("db-", "").replace(".log", "");

    return Integer.parseInt(numPart);
  }

  protected void restartBrokerService (BrokerService brokerService) throws Exception {
    log.info("Shutting down broker service with {} messages",
             brokerService.getAdminView().getTotalMessageCount());

    brokerService.stop();
    brokerService.waitUntilStopped();
    brokerService.start();
    brokerService.waitUntilStarted();

    log.info("Restarted broker service with {} messages",
             brokerService.getAdminView().getTotalMessageCount());
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
    persistenceAdapter.setJournalMaxFileLength(512);
    persistenceAdapter.setCleanupInterval(999999999L); // try to disable so we control when cleanup happens
    brokerService.setPersistenceAdapter(persistenceAdapter);

//    persistenceAdapter.getStore().checkpoint(false);

    PolicyMap policies = new PolicyMap();

    PolicyEntry queuePolicy;
    queuePolicy = new PolicyEntry();
    queuePolicy.setQueue(">");
    queuePolicy.setPersistJMSRedelivered(true);

    policies.setPolicyEntries(Arrays.asList(queuePolicy));

    brokerService.setDestinationPolicy(policies);

    return brokerService;
  }

  protected void performKahaDBCleanup(BrokerService brokerService) throws IOException {
    ((KahaDBPersistenceAdapter) brokerService.getPersistenceAdapter()).getStore().checkpoint(true);
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

  protected void consumeMessages(BrokerService brokerService, String queueName, int max, boolean ackNormally)
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
        if (ackNormally) {
          msg.acknowledge();
        }

        msg = consumer.receive(100);
        cur++;
      }

      log.info("Finsihed consuming {} messages with max {} on queue '{}'", cur, max, queueName);
    } finally {
      connection.close();
    }
  }
}
