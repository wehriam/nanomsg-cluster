
const uuid = require('uuid');
const { getNode } = require('../tests/lib/node');

const HOST = '127.0.0.1';
const NANOMSG_PUBSUB_PORT_A = 7789;
const NANOMSG_PUBSUB_PORT_B = 7889;
const NANOMSG_PIPELINE_PORT_A = 8789;
const NANOMSG_PIPELINE_PORT_B = 8889;

const nameA = uuid.v4();
const nameB = uuid.v4();
const topic1 = uuid.v4();
const topic2 = uuid.v4();

const addressA = {
  host: HOST,
  pubsubPort: NANOMSG_PUBSUB_PORT_A,
  pipelinePort: NANOMSG_PIPELINE_PORT_A,
};

const addressB = {
  host: HOST,
  pubsubPort: NANOMSG_PUBSUB_PORT_B,
  pipelinePort: NANOMSG_PIPELINE_PORT_B,
};

const ITERATIONS = 10000;

const run = async () => {
  const uuids = [];
  for (let i = 0; i < ITERATIONS; i += 1) {
    uuids.push(Math.random().toString());
  }
  const nodeA = await getNode(nameA, addressA, []);
  const nodeB = await getNode(nameB, addressB, [addressA]);
  let messageA = uuids.pop();
  let messageB = uuids.pop();
  const start = Date.now();
  const stop = async () => {
    const time = Date.now() - start;
    console.log(`\n\n${ITERATIONS} iterations in ${time}ms\n\n${Math.round(100 * ITERATIONS / time) * 10} iterations per second\n\n`);
    process.exit(0);
  };
  nodeA.subscribe(topic1, (message) => {
    if (uuids.length === 0) {
      stop();
      return;
    }
    if (message === messageB) {
      messageB = uuids.pop();
      nodeA.sendToAll(topic2, messageA);
    }
  });
  nodeB.subscribe(topic2, (message) => {
    if (uuids.length === 0) {
      stop();
      return;
    }
    if (message === messageA) {
      messageA = uuids.pop();
      nodeB.sendToAll(topic1, messageB);
    }
  });
  nodeA.sendToAll(topic2, messageA);
};

run();
