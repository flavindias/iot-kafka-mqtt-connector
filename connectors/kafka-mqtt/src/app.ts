import dotenv from 'dotenv';
import mqtt from 'mqtt';
import { Kafka } from 'kafkajs';
dotenv.config();

const { MQTT_PUB_URL, MQTT_PUB_PORT, MQTT_PUB_USER, KAFKA_BROKERS, MQTT_PUB_PASS, KAFKA_MQTT_PUB_TOPICS } = process.env;
const mqttClient  = mqtt.connect(MQTT_PUB_URL, {
	clientId: 'MQTT Bess Publisher Dev',
	host: MQTT_PUB_URL,
	port: parseInt(MQTT_PUB_PORT),
	username: MQTT_PUB_USER,
	password: MQTT_PUB_PASS,
	clean: true,
	protocol: 'ssl',
});

// Mqtt error calback
mqttClient.on('error', (err) => {
	console.log(err);
	mqttClient.end();
	console.error('MQTT disconnected by error');
});

// Connection callback
mqttClient.on('connect', () => {
	console.log('mqtt client connected');
});

// mqttClient.subscribe('bess/#', { qos: 0 });

const kafka = new Kafka({
	clientId: 'kafka-mongo-connector-dev',
	brokers: KAFKA_BROKERS.split(','),
	connectionTimeout: 3000,
	retry: {
		initialRetryTime: 100,
		retries: 8
	},
	// ssl: true,
	// sasl: {
	// 	mechanism: 'plain',
	// 	username: KAFKA_PUB_USER,
	// 	password: KAFKA_PUB_PASS
	// }
});
const consumer = kafka.consumer({ groupId: 'kafka-mqtt-connector' });
const publishMqtt = async (message, topic) => {
	try{
		await mqttClient.publish(`bess/${message.key.toString()}/${topic}`, message.value.toString());
		console.log(new Date().toISOString(), message.key.toString(), topic);
	}
	catch(error){
		console.error(error);
	}
};
const run = async () => {
	try {
		await consumer.connect();
		KAFKA_MQTT_PUB_TOPICS.split(',').map(async(topic) => {
			await consumer.subscribe({ topic, fromBeginning: true });
		});

		await consumer.run({
			eachMessage: async ({ topic, message }) => {
				// convert kafka format ( topic_name )to mqtt format (topic/name)
				const topicMqtt = topic.split('_').join('/');
				publishMqtt(message, topicMqtt);
			},
		});

	}
	catch(error){
		console.error(`ERROR: ${error}`);
	}
};
run();