import mqtt from 'mqtt';
import dotenv from 'dotenv';
import { v4 as uuidv4 } from 'uuid';
import { Kafka, CompressionTypes } from 'kafkajs';
dotenv.config();

const { MQTT_SUB_URL, MQTT_SUB_PORT, MQTT_SUB_USER, MQTT_SUB_PASS, KAFKA_BROKERS, KAFKA_MQTT_SUB_TOPICS } = process.env;

const mqttClient  = mqtt.connect(MQTT_SUB_URL, {
	clientId: 'MQTT Client',
	host: MQTT_SUB_URL,
	port: parseInt(MQTT_SUB_PORT),
	username: MQTT_SUB_USER,
	password: MQTT_SUB_PASS,
	clean: true,
	protocol: 'ssl'
});

// Mqtt error calback
mqttClient.on('error', (err) => {
	console.log(err);
	mqttClient.end();
});

// Connection callback
mqttClient.on('connect', () => {
	console.log('mqtt client connected');
});
mqttClient.subscribe('bess/#', { qos: 0 });

const kafka = new Kafka({
	clientId: 'mqtt-kafka',
	brokers: KAFKA_BROKERS.split(','),
	connectionTimeout: 3000,
	retry: {
		initialRetryTime: 100,
		retries: 8
	},
	// ssl: false,
	// sasl: {
	// 	mechanism: 'plain',
	// 	username: KAFKA_SUB_USER,
	// 	password: KAFKA_SUB_PASS
	// }
});
const producer = kafka.producer();
		
const publishKafka = async (topic, message, key) => {
	
	try{
		JSON.parse(message);
	}
	catch(error){
		console.error(error, message);
	}
	finally{
		await producer.connect();
		if(message.length < 4){
			console.log(typeof message, message);
		}
		else{
			let payload = { 
				key, 
				value: message, 
				headers: {
					'correlation-id': `mqtt-kafka-connector-${uuidv4()}`,
					'system-id': 'mqtt-kafka-connector'
				} 
			};
			const response = await producer.send({
				topic,
				compression: CompressionTypes.GZIP,
				messages: [payload],
			});
			if(response[0].errorCode === 0){
				console.log(new Date().toISOString(),key, topic);
			}
		}
		
	}
	
	// await producer.disconnect();
};
const main = async () => {
	try{
		const admin = kafka.admin();
		await admin.connect();
		
		const topics = KAFKA_MQTT_SUB_TOPICS.split(',').map(topic => {
			return {
				topic,
				numPartitions: 2,
				replicationFactor: 1
			};
		});
		await admin.createTopics({
			topics,
		});
		
		mqttClient.on('message', async (topicName, message) => {
			// message is Buffer
			const [, key, ...topic] = topicName.split('/');
			await publishKafka(topic.join('_'), message.toString(), key);
		});
		mqttClient.on('close', () => {
			console.log('MQTT client disconnected');
		});
	}
	catch(error){
		console.error(error);
	}
};
main().then(() => console.log('done'));