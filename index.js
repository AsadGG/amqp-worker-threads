const { Worker } = require("worker_threads");
const amqplib = require("amqplib");

function createQueueWorker(queueName, handlers) {
  const worker = new Worker(
    `const { parentPort, workerData } = require("worker_threads");
    const amqplib = require("amqplib");
    
    async function connectAndConsume(queueName) {
      try {
        const connection = await amqplib.connect(
          "amqp://guest:guest@localhost:5672"
        );
        const channel = await connection.createChannel();
        await channel.assertQueue(queueName, { durable: true });
    
        console.log(\`Worker: Listening for messages on queue '${queueName}'\`);
    
        channel.consume(
          queueName,
          (msg) => {
            if (msg !== null) {
              parentPort.postMessage({
                queueName,
                message: msg.content.toString(),
                deliveryTag: msg.fields.deliveryTag,
              });
            }
          },
          { noAck: false }
        );
    
        parentPort.on("message", (messageFromMain) => {
          if (messageFromMain.nack && messageFromMain.deliveryTag) {
            channel.nack(
              { fields: { deliveryTag: messageFromMain.deliveryTag } },
              false,
              true
            );
          } else if (messageFromMain.ack && messageFromMain.deliveryTag) {
            channel.ack({ fields: { deliveryTag: messageFromMain.deliveryTag } });
          }
        });
    
        process.on("SIGINT", async () => {
          console.log("Worker: Received SIGINT. Closing connection...");
          await channel.close();
          await connection.close();
          process.exit(0);
        });
      } catch (error) {
        console.error(
          \`Worker: Error connecting or consuming from queue '${queueName}':\`,
          error
        );
        parentPort.postMessage({ error: error.message, queueName });
      }
    }
    
    connectAndConsume(workerData.queueName);
    `,
    { eval: true, workerData: { queueName } }
  );

  console.log("handlers :>> ", handlers);

  const onMessage =
    handlers?.onMessage ??
    ((message) => {
      if (message.error) {
        // ... (error handling)
      } else {
        console.log(
          `Main thread: Received message from queue '${message.queueName}':`,
          message.message
        );
      }
    });

  worker.on("message", onMessage);

  const onError =
    handlers?.onError ??
    ((err) => {
      console.error(
        `Main thread: Worker for queue ${queueName} encountered an error:`,
        err
      );
    });

  worker.on("error", onError);

  const onExit =
    handlers?.onExit ??
    ((code) => {
      if (code !== 0) {
        console.error(
          `Main thread: Worker for queue ${queueName} exited with code ${code}`
        );
      } else {
        console.log(
          `Main thread: Worker for queue ${queueName} exited normally.`
        );
      }
    });

  worker.on("exit", onExit);

  return worker;
}

async function sendToQueue(queueName, data) {
  const connection = await amqplib.connect("amqp://guest:guest@localhost:5672");

  const channel = await connection.createChannel();

  await channel.assertQueue(queueName, { durable: true });

  channel.sendToQueue(queueName, Buffer.from(JSON.stringify(data)));

  await channel.close();

  await connection.close();
}

async function main() {
  const queues = ["my_queue_1", "my_queue_2", "my_queue_3", "my_queue_4"];

  const workers = queues.map((queue) => {
    const worker = createQueueWorker(queue, {
      onMessage: (message) => {
        console.log("message :>> ", message);
        const isAck = Math.random() > 0.75;
        console.log(`workers -> isAck:`, isAck);

        if (isAck) {
          setTimeout(() => {
            worker.postMessage({
              ack: true,
              deliveryTag: message.deliveryTag,
            });
          }, 3000);
        } else {
          setTimeout(() => {
            worker.postMessage({
              nack: true,
              deliveryTag: message.deliveryTag,
            });
          }, 3000);
        }
      },
    });
    return worker;
  });

  process.on("SIGINT", async () => {
    await Promise.all(workers.map((worker) => worker.terminate()));

    console.log("Main thread: Received SIGINT. Terminating worker...");

    process.exit(0);
  });

  setTimeout(async () => {
    await sendToQueue("my_queue_1", { data_1: "data_1" });

    await sendToQueue("my_queue_2", { data_2: "data_2" });

    await sendToQueue("my_queue_3", { data_3: "data_3" });

    await sendToQueue("my_queue_4", { data_4: "data_4" });
  }, 3000);
}

main();
