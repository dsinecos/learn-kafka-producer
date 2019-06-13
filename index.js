const Kafka = require('node-rdkafka');
const debug = require('debug')('kafka:producer');

// To list the features supported by node-rdkafka
debug(`Supported features ${Kafka.features}`);
// To retrieve the version of librdkafka that node-rdkafka is based on
debug(`librdkafka version ${Kafka.librdkafkaVersion}`);

// Configure a Producer
const producer = new Kafka.HighLevelProducer({
  // Allows to correlate requests on the broker with the respective Producer
  'client.id': "demo-producer",
  // Bootstrap server is used to fetch the full set of brokers from the cluster &
  // relevant metadata
  'bootstrap.servers': 'localhost:9092', // OR 'metadata.broker.list': 'localhost:9092'
});

// Topic has been already created using Kafka CLI
// Create Topic on Kafka Cluster
const topicName = 'first_topic';

// The 'ready' event is emitted when the Producer is ready to send messages
producer.on('ready', function (arg) {

  debug('Producer ready. ' + JSON.stringify(arg, null, '  '));

  // Log Metadata once Producer connects to Kafka Cluster
  const opts = {
    // Topic for which metadata is to be retrieved
    topic: 'first_topic',
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

  let maxMessages = 5

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

    producer.produce(
      topicName, 
      null, // Partition is set to null, 
      value, 
      null, // Key is set to null resulting in a Round-Robin distribution of messages
      timestamp, 
      (err, offset) => { // Callback to receive delivery reports for messages
      if (err) {
        debug('Error producing message');
        debug(err)
      }

      debug(`Offset: \n ${offset}`) // Offset of the committed message is logged
    });
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