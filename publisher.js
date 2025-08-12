// 1. Import the mqtt library
const mqtt_publisher = require('mqtt');

// 2. Define the connection URL
// THIS IS THE MODIFIED LINE: It now points to your public AWS IP address.
const pubConnectUrl = 'mqtt://13.201.98.218:1883';

// 3. Create a client instance
const pubClient = mqtt_publisher.connect(pubConnectUrl);

// 4. Define the topic and the message to publish
const pubTopic = 'ebhoomPub';
const pubMessage = 'Hello from an external device!'; // Changed message for clarity

// This function runs when the client successfully connects
pubClient.on('connect', () => {
  console.log('Connected to the public MQTT broker to publish a message.');

  // Publish the message to the topic
  pubClient.publish(pubTopic, pubMessage, (error) => {
    if (error) {
      console.error('Publish error:', error);
    } else {
      console.log(`Message published successfully to topic: ${pubTopic}`);
      console.log(`Message: "${pubMessage}"`);
    }

    // After publishing, close the connection
    pubClient.end();
  });
});

// Handle connection errors
pubClient.on('error', (error) => {
  console.error('Publisher connection error:', error);
  pubClient.end();
});
