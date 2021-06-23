require('dotenv').config();
const Kafka = require('kafkajs').Kafka;
const CronJob = require("cron").CronJob;
const { request } = require('graphql-request')

const kafkaHost = process.env.KAFKA_HOST;
const kafkaClientId = process.env.APP_ID;
const cronExpression = process.env.CRON_EXPRESSION;
const topic = process.env.PRODUCE_TO_TOPIC

const kafka = new Kafka({
	clientId: kafkaClientId,
	brokers: [].concat(kafkaHost)
})

function executeByCron(cronTimeExpression, process) {
	var job = new CronJob(cronTimeExpression, function () {
		process()
	}, null, true);

	job.start();
}


const produceToTopic = async (messages, topic) => {
	console.log(messages)
	const producer = await kafka.producer()

	const formattedMessages = messages.map((message) => {
		const mes = {
			key: message.id,
			value: JSON.stringify(message)
		}
		return mes
	});

	await producer.connect()
	await producer.send({
		topic: topic,
		messages: formattedMessages,
	})
		.then(() => {
			console.log(`Produced messages: ${JSON.stringify(messages)} \n`);
		})
		.catch(err => console.error(`Error when producing: ${err} \n`))

	await producer.disconnect()
}


const query = `{
  jobs{
    id
    title
    description
    applyUrl
    company {
      name,
      logoUrl
    }
    cities {
      name
      country {
        name
      }
    }
    tags {
      name
    },
    isPublished,
    createdAt,
    updatedAt,
    postedAt
  }
}`

// https://api.graphql.jobs/
const fs = require('fs');

async function fetchAndProduceData() {
	request("https://api.graphql.jobs", query)
		.then((data) => {
			const graphqlJobs = data.jobs;
			produceToTopic(graphqlJobs, topic);
		})
}


/**
 * Start process
 */
fetchAndProduceData()
executeByCron(cronExpression, fetchAndProduceData);
