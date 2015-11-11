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
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

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

  private BrokerService brokerService;
  private ActiveMQConnection consumerConnection;
  private ConsumerThread consumerThread;

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

    brokerService = this.setupBrokerService();

    brokerService.start();
    brokerService.waitUntilStarted();

    this.startConsumer("test.queue.01");
    this.sendMessages(brokerService, "test.queue.01", 10000);

    Thread.sleep(1000);
    this.logJournalFileInfo("HAVE {} log files (#{}) after producing messages");

    this.performKahaDBCleanup(brokerService);
    this.logJournalFileInfo(
        "HAVE {} log files (#{}) after producing 10 messages and kahadb cleanup");
  }

  protected void logJournalFileInfo(String pattern, Object... additionalDetails) {
    List<Object> details = new LinkedList<Object>();

    File[] journalFiles = this.findJournalFiles(kahaDbDirectory);
    String journaleFileNums = this.getJournalFileNumbers(journalFiles);


    details.add(journalFiles.length);
    details.add(journaleFileNums);
    details.addAll(Arrays.asList(additionalDetails));

    log.info(pattern, details.toArray(new Object[details.size()]));
  }

  protected File[] findJournalFiles(File kahaDirectory) {
    File[] journalFiles = kahaDirectory.listFiles(new FileFilter() {
      public boolean accept(File pathname) {
        return pathname.getName().matches("db-[0-9]+\\.log");
      }
    });

    return journalFiles;
  }

  protected String getJournalFileNumbers(File[] journalFiles) {
    TreeSet<Integer> numberSet = new TreeSet<>();
    StringBuilder result = new StringBuilder();

    if (journalFiles.length > 0) {
      for (File oneFile : journalFiles) {
        int num = this.extractFileNumber(oneFile);
        numberSet.add(num);
      }

      boolean isFirst = true;
      int numRange = 0;
      int start = 0;
      int last = 0;

      for (Integer oneNum : numberSet) {
        if (isFirst) {
          start = oneNum;
          last = oneNum;
          isFirst = false;
        } else {
          if (oneNum == last + 1) {
            last = oneNum;
          } else {
            if (numRange != 0) {
              result.append(",");
            }

            result.append(Integer.toString(start));

            if (start != last) {
              result.append("-");
              result.append(Integer.toString(last));
            }

            start = oneNum;
            last = oneNum;
            numRange++;
          }
        }
      }

      // Last range
      if (numRange != 0) {
        result.append(",");
      }
      result.append(Integer.toString(start));
      if (start != last) {
        result.append("-");
        result.append(Integer.toString(last));
      }
    } else {
      result.append("none");
    }

    return result.toString();
  }

  protected int extractFileNumber(File file) {
    String numPart = file.getName().replace("db-", "").replace(".log", "");

    return Integer.parseInt(numPart);
  }

  protected void restartBrokerService() throws Exception {
    log.info("Shutting down broker service with {} messages",
             brokerService.getAdminView().getTotalMessageCount());

    this.brokerService.stop();
    this.brokerService.waitUntilStopped();

    //
    // Prepare a fresh broker service object
    //
    this.brokerService = this.setupBrokerService();

    this.brokerService.start();
    this.brokerService.waitUntilStarted();

    log.info("Restarted broker service with {} messages",
             this.brokerService.getAdminView().getTotalMessageCount());
  }

  /**
   * Prepare the broker service for testing.
   */
  protected BrokerService setupBrokerService() throws Exception {
    BrokerService brokerService = new BrokerService();
    brokerService.setDataDirectory(dataDirectory.getPath());
    brokerService.setPersistent(true);
    brokerService.setUseJmx(true);

    KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
    persistenceAdapter.setDirectory(kahaDbDirectory);
//    persistenceAdapter.setJournalMaxFileLength(512);
    persistenceAdapter.setJournalMaxFileLength(10 * KB);
    persistenceAdapter
        .setCleanupInterval(999999999L); // try to disable so we control when cleanup happens
    persistenceAdapter.setConcurrentStoreAndDispatchQueues(true);
    brokerService.setPersistenceAdapter(persistenceAdapter);

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

  protected void consumeMessages(BrokerService brokerService, String queueName, int max,
                                 boolean ackNormally)
      throws Exception {

    ActiveMQConnection connection = ActiveMQConnection
        .makeConnection(brokerService.getVmConnectorURI().toString());

    try {
      connection.start();
      Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(new ActiveMQQueue(queueName));

      int cur;
      cur = 0;

      log.info("Starting to consume up to {} messages on queue '{}' ackNormally={}", max, queueName,
               ackNormally);

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

  protected void startConsumer(final String queueName) {
    this.consumerThread = new ConsumerThread(queueName);
    this.consumerThread.start();
  }

  protected void stopConsumer() {
    this.consumerThread.shutdown();
  }

  protected class ConsumerThread extends Thread {
    private final String queueName;
    private AtomicBoolean shutdownInd = new AtomicBoolean(false);

    public ConsumerThread(String queueName) {
      this.queueName = queueName;
    }

    public void shutdown() {
      this.shutdownInd.set(true);
    }

    @Override
    public void run() {
      int numConsumed = 0;

      try {
        ActiveMQConnection connection = ActiveMQConnection
            .makeConnection(brokerService.getVmConnectorURI().toString());

        try {
          connection.start();
          Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
          MessageConsumer consumer = session.createConsumer(new ActiveMQQueue(queueName));

          log.info("Starting consumer on queue {}", queueName);

          while (! shutdownInd.get()) {
            Message msg = consumer.receive();
            msg.acknowledge();
            numConsumed++;
          }
        } finally {
          connection.close();
        }

      } catch (Exception exc) {
        if (!shutdownInd.get()) {
          log.error("consumer terminated with exception", exc);
        }
      }
      log.info("Consumer read {} messages", numConsumed);
    }
  }
}
