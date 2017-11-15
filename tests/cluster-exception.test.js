// @flow

const uuid = require('uuid');
const { getNode } = require('./lib/node');

const HOST = '127.0.0.1';
const NANOMSG_PUBSUB_PORT_A = 4789;
const NANOMSG_PUBSUB_PORT_B = 4889;
const NANOMSG_PIPELINE_PORT_A = 5789;
const NANOMSG_PIPELINE_PORT_B = 5889;

const addressA = {
  host: HOST,
  pubsubPort: NANOMSG_PUBSUB_PORT_A,
  pipelinePort: NANOMSG_PIPELINE_PORT_A,
};

const addressB = {
  host: '1.1.1.1',
  pubsubPort: NANOMSG_PUBSUB_PORT_B,
  pipelinePort: NANOMSG_PIPELINE_PORT_B,
};

let nodeA;
// let nodeB;

const nameA = uuid.v4();
// const nameB = uuid.v4();

const topic1 = uuid.v4();
const message1 = { [uuid.v4()]: uuid.v4() };

beforeAll(async () => {
  nodeA = await getNode(nameA, addressA, []);
});

afterAll(async () => {
  await nodeA.close();
});

test('Connects to a non-existent node', async () => {
  nodeA.addPeer(addressB);
  nodeA.sendToAll(topic1, message1);
});
