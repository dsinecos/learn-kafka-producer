const Kafka = require('node-rdkafka');
const debug = require('debug')('kafka:producer');

// To list the features supported by node-rdkafka
debug(`Supported features ${Kafka.features}`);
// To retrieve the version of librdkafka that node-rdkafka is based on
debug(`librdkafka version ${Kafka.librdkafkaVersion}`);

// Configure a Producer
const producer = new Kafka.Producer({
  // Allows to correlate requests on the broker with the respective Producer
  'client.id': "demo-producer",
  // Bootstrap server is used to fetch the full set of brokers from the cluster &
  // relevant metadata
  'bootstrap.servers': 'localhost:9092', // OR 'metadata.broker.list': 'localhost:9092'
  // Enable to receive delivery reports for messages
  'dr_cb': true,
  // Enable to receive message payload in delivery reports
  'dr_msg_cb': true,
  // Enable to receive events from `librdkafka`
  'event_cb': false,
  // Enable to receive logs from `librdkafka`
  'debug': ['all'],
  // Configure the max number of retries for temporary errors
  'message.send.max.retries': 100000,
  // Configure backoff time in ms before retrying a message
  'retry.backoff.ms': 1000,
},
{
  // Set the acknowledgement level for Kafka Producer
  'acks': 'all',
}
);

// Topic has been already created using Kafka CLI
// Create Topic on Kafka Cluster
const topicName = 'replicated_topic';

// Setup listener to receive logs
producer.on('event.log', (log) => {
  debug('Log received');
  debug(log)
})

// Setup listener to receive errors
producer.on('event.error', (err) => {
  debug('Error');
  debug(err);
})

// Setup listener to receive delivery-reports
producer.on('delivery-report', (err, report) => {
  if (err) {
    debug('Error delivering messaage');
    debug(err)
    return;
  }

  debug(`Delivery-report: ${JSON.stringify(report, null, '  ')}`);
})

// To receive delivery reports the producer needs to be polled at regular intervals
// Configures polling the producer for delivery reports every 1000 ms
producer.setPollInterval(100);
// producer.setPollInterval(0) to disable polling

// The 'ready' event is emitted when the Producer is ready to send messages
producer.on('ready', function (arg) {

  debug('Producer ready. ' + JSON.stringify(arg, null, '  '));

  // Log Metadata once Producer connects to Kafka Cluster
  const opts = {
    // Topic for which metadata is to be retrieved
    topic: 'replicated_topic',
    // Max time, in ms, to try to fetch metadata before timing out. Defaults to 3000
    timeout: 10000
  };

  producer.getMetadata(opts, function (err, metadata) {
    if (err) {
      debug('Error fetching metadata');
      debug(err);
      return;
    }
    debug('Received metadata');
    debug(metadata);
  });

  let maxMessages = 5;

  // Iterate and Publish 10 Messages to the Kafka Topic
  for (let i = 1; i <= maxMessages; i++) {

    // Message to be sent must be a Buffer
    let value = Buffer.from('value-' + i);

    // The partitioners shipped with Kafka guarantee that all messages with the same non-empty
    // key will be sent to the same partition. If no key is provided, then the partition is 
    // selected in a round-robin fashion to ensure an even distribution across the topic 
    // partitions
    let key = "key-" + i;

    // If a partition is set, the messages will be routed to the defined Topic-Partition
    // If partition is set to -1, librdkafka will use the default partitioner
    let partition = -1;

    // If the Broker version supports adding a timestamp, it'll be added
    let timestamp = Date.now();

    // Opaque token gets passed to the delivery reports and can be used to
    // correlate messages against their respective delivery reports
    let opaqueToken = `opaque::${i}`;

    producer.produce(
      topicName,
      null, // Partition is set to null, 
      value,
      null, // Key is set to null resulting in a Round-Robin distribution of messages
      timestamp,
      opaqueToken
    );
  }
});

// Connecting the producer to the Kafka Cluster
producer.connect({}, (err) => {
  if (err) {
    debug('Error connecting to Broker');
    debug(err);
    return;
  }
  debug('Connected to broker');
});