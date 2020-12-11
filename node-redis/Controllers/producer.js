const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

// exports.kafkaProduce = function (data) {
//     const producer = kafka.producer({
//         maxInFlightRequests: 1,
//         idempotent: true,
//         transactionalId: "uniqueProducerId",
//     });

//     async function sendPayload() {
//         try {
//             await producer.send({
//                 topic: 'test-topic',
//                 messages: [
//                     { value: 'Hello KafkaJS user!' },
//                 ],
//             })
//         } catch (e) {
//             console.error("Caught Error while sending:", e);
//         }
//     }

//         await producer.connect();
//         while (true) {
//             let input = await reader.question("Data: ");
//             if (input === "exit") {
//                 process.exit(0);
//             }
//             try {
//                 await sendPayload(input);
//             } catch (e) {
//                 console.error(e);
//             }
//         }

// }

exports.kafkaProduce = function (data) {
    const producer = kafka.producer();
    const run = async () => {
        // Producing
        await producer.connect()
        await producer.send({
            topic: 'test-topic',
            messages: [
                { value: 'Hello KafkaJS user!' },
            ],
        })
        await producer.disconnect()

    }
    run()
};
var out = "";
exports.kafkaConsume = async () => {
    
    const consumer = kafka.consumer({ groupId: 'test-group' })
    // const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            out = message.value.toString();

            return out;
        },
    })

    // const errorTypes = ['unhandledRejection', 'uncaughtException']
    // const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

    // errorTypes.map(type => {
    //     process.on(type, async e => {
    //         try {
    //             console.log(`process.on ${type}`)
    //             console.error(e)
    //             await consumer.disconnect()
    //             process.exit(0)
    //         } catch (_) {
    //             process.exit(1)
    //         }
    //     })
    // })

    // signalTraps.map(type => {
    //     process.once(type, async () => {
    //         try {
    //             await consumer.disconnect()
    //         } finally {
    //             process.kill(process.pid, type)
    //         }
    //     })
    // })

};

