import dotenv from 'dotenv';
import { Kafka } from 'kafkajs';
import {MongoClient, ObjectId} from 'mongodb';
dotenv.config();

const { KAFKA_BROKERS, KAFKA_TOPICS, MONGO_URL, DB_NAME } = process.env;
const dbClient = new MongoClient(MONGO_URL, {
	useNewUrlParser: true,
	useUnifiedTopology: true,
});

dbClient.connect(async (err) => {
	if (err) throw err;
	console.log('MongoDB has been connected');
});
const kafka = new Kafka({
	clientId: 'kafka-mongo-connector',
	brokers: KAFKA_BROKERS.split(',')
});

const consumer = kafka.consumer({ groupId: 'kafka-mongo-connector' });

const run = async () => {
	try {
		await consumer.connect();
		KAFKA_TOPICS.split(',').map(async(topic) => {
			await consumer.subscribe({ topic, fromBeginning: true });
		});

		await consumer.run({
			eachMessage: async ({ topic, partition, message }) => {
				try{
					JSON.parse(message.value.toString());	
				}
				catch(error){
					console.error(error);
					console.error('ERROR', new Date().toISOString(), partition, message.key.toString(), `message: ${message.value.toString()}`, topic);
				}
				finally{
					if(message.value.toString() !== 'message'){
						const data = JSON.parse(message.value.toString()).Data;
						const tzCloud = JSON.parse(message.value.toString()).TimestampCloud;
						const tz = `${JSON.parse(message.value.toString()).Timestamp}`.length === 19 ? 
							`${JSON.parse(message.value.toString()).Timestamp}.000Z` : 
							JSON.parse(message.value.toString()).Timestamp;

						const result = await dbClient
							.db(DB_NAME)
							.collection(topic)
							.insertOne({
								...data,
								bess: new ObjectId(message.key.toString()),
								createdAt: tzCloud ?? tz,
								updatedAt: tzCloud ?? tz
							});
						if(result.insertedCount === 1){
							console.log(new Date().toISOString(), partition, message.key.toString(), topic);
						}
						else{
							console.error('ERROR: ', new Date().toISOString(), partition, message.key.toString(), topic);
						}
					}
				}
			},
		});
	}
	catch(error){
		console.error(error);
	}
};
run();