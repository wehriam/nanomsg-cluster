// @flow

const { messageTimeout, getNode } = require('./lib/node');

const uuid = require('uuid');

const HOST = '127.0.0.1';
let port = 7000;
const NANOMSG_PUBSUB_PORT_A = port++;
const NANOMSG_PUBSUB_PORT_B = port++;
const NANOMSG_PUBSUB_PORT_C = port++;
const NANOMSG_PIPELINE_PORT_A = port++;
const NANOMSG_PIPELINE_PORT_B = port++;
const NANOMSG_PIPELINE_PORT_C = port++;
const NANOMSG_TOPIC_PORT_A = port++;
const NANOMSG_TOPIC_PORT_B = port++;
const NANOMSG_TOPIC_PORT_C = port++;

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

const addressC = {
  host: HOST,
  pubsubPort: NANOMSG_PUBSUB_PORT_C,
  pipelinePort: NANOMSG_PIPELINE_PORT_C,
};

let nodeA;
let nodeA2;
let nodeB;
let nodeC;

const topic = uuid.v4();

const nodeNames = [
  uuid.v4(),
  uuid.v4(),
  uuid.v4(),
  uuid.v4(),
];

nodeNames.sort();

const [nameA, nameB, nameC, nameA2] = nodeNames;

describe('Pipeline Reconnect', () => {
  beforeAll(async () => {
    await messageTimeout();
  });


  test('Node A, B, and C reconnect and send pipeline events', async () => {
    nodeA = await getNode(nameA, addressA, [], 50);
    nodeB = await getNode(nameB, addressB, [], 50);
    nodeC = await getNode(nameC, addressC, [], 50);
    nodeB.addPeer(addressA);
    nodeC.addPeer(addressA);
    await messageTimeout();
    nodeA.providePipeline(topic);
    nodeB.providePipeline(topic);
    nodeC.providePipeline(topic);
    await messageTimeout();
    expect(nodeA.pipelineConsumers(topic)).toEqual([]);
    expect(nodeB.pipelineConsumers(topic)).toEqual([]);
    expect(nodeC.pipelineConsumers(topic)).toEqual([]);
    nodeA.consumePipeline(NANOMSG_TOPIC_PORT_A, topic);
    nodeB.consumePipeline(NANOMSG_TOPIC_PORT_B, topic);
    nodeC.consumePipeline(NANOMSG_TOPIC_PORT_C, topic);
    await messageTimeout();
    expect(new Set(nodeA.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    expect(new Set(nodeB.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    expect(new Set(nodeC.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    expect(nodeA.isLeader()).toEqual(true);
    expect(nodeB.isLeader()).toEqual(false);
    expect(nodeC.isLeader()).toEqual(false);
    expect(nodeA.isLeader(nameA)).toEqual(true);
    expect(nodeB.isLeader(nameA)).toEqual(true);
    expect(nodeC.isLeader(nameA)).toEqual(true);
    expect(nodeA.isLeader(nameB)).toEqual(false);
    expect(nodeB.isLeader(nameB)).toEqual(false);
    expect(nodeC.isLeader(nameB)).toEqual(false);
    expect(nodeA.isLeader(nameC)).toEqual(false);
    expect(nodeB.isLeader(nameC)).toEqual(false);
    expect(nodeC.isLeader(nameC)).toEqual(false);
    nodeB.removePeer(addressA);
    nodeC.removePeer(addressA);
    await messageTimeout();
    expect(new Set(nodeA.pipelineConsumers(topic))).toEqual(new Set([nameA]));
    expect(new Set(nodeB.pipelineConsumers(topic))).toEqual(new Set([nameB, nameC]));
    expect(new Set(nodeC.pipelineConsumers(topic))).toEqual(new Set([nameB, nameC]));
    // Node A is leader of itself
    expect(nodeA.isLeader()).toEqual(true);
    expect(nodeB.isLeader()).toEqual(true);
    expect(nodeC.isLeader()).toEqual(false);
    expect(nodeA.isLeader(nameA)).toEqual(true);
    expect(nodeB.isLeader(nameA)).toEqual(false);
    expect(nodeC.isLeader(nameA)).toEqual(false);
    expect(nodeA.isLeader(nameB)).toEqual(false);
    expect(nodeB.isLeader(nameB)).toEqual(true);
    expect(nodeC.isLeader(nameB)).toEqual(true);
    expect(nodeA.isLeader(nameC)).toEqual(false);
    expect(nodeB.isLeader(nameC)).toEqual(false);
    expect(nodeC.isLeader(nameC)).toEqual(false);

    expect(new Set(Object.keys(nodeA.namedPushSockets))).toEqual(new Set([]));
    expect(new Set(Object.keys(nodeB.namedPushSockets))).toEqual(new Set([nameC]));
    expect(new Set(Object.keys(nodeC.namedPushSockets))).toEqual(new Set([nameB]));

    expect(new Set(Object.keys(nodeB.peerSocketHashes))).toEqual(new Set([nodeC.socketHash]));
    expect(new Set(Object.keys(nodeC.peerSocketHashes))).toEqual(new Set([nodeB.socketHash]));

    nodeB.addPeer(addressA);
    await messageTimeout();
    expect(nodeA.isLeader()).toEqual(true);
    expect(nodeB.isLeader()).toEqual(false);
    expect(nodeC.isLeader()).toEqual(false);
    expect(nodeA.isLeader(nameA)).toEqual(true);
    expect(nodeB.isLeader(nameA)).toEqual(true);
    expect(nodeC.isLeader(nameA)).toEqual(true);
    expect(nodeA.isLeader(nameB)).toEqual(false);
    expect(nodeB.isLeader(nameB)).toEqual(false);
    expect(nodeC.isLeader(nameB)).toEqual(false);
    expect(nodeA.isLeader(nameC)).toEqual(false);
    expect(nodeB.isLeader(nameC)).toEqual(false);
    expect(nodeC.isLeader(nameC)).toEqual(false);

    expect(new Set(Object.keys(nodeA.namedPushSockets))).toEqual(new Set([nameB, nameC]));
    expect(new Set(Object.keys(nodeB.namedPushSockets))).toEqual(new Set([nameA, nameC]));
    expect(new Set(Object.keys(nodeC.namedPushSockets))).toEqual(new Set([nameA, nameB]));

    expect(new Set(nodeA.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    expect(new Set(nodeB.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    expect(new Set(nodeC.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    await nodeA.close();
    await nodeB.close();
    await nodeC.close();
    nodeA.throwOnLeakedReferences();
    nodeB.throwOnLeakedReferences();
    nodeC.throwOnLeakedReferences();
  });


  test('Node A, B, and C reconnect and send pipeline events after a heartbeat timeout', async () => {
    nodeA = await getNode(nameA, addressA, [], 50);
    nodeB = await getNode(nameB, addressB, [], 50);
    nodeC = await getNode(nameC, addressC, [], 50);
    nodeB.addPeer(addressA);
    nodeC.addPeer(addressA);
    await messageTimeout();
    nodeA.providePipeline(topic);
    nodeB.providePipeline(topic);
    nodeC.providePipeline(topic);
    nodeA.consumePipeline(NANOMSG_TOPIC_PORT_A, topic);
    nodeB.consumePipeline(NANOMSG_TOPIC_PORT_B, topic);
    nodeC.consumePipeline(NANOMSG_TOPIC_PORT_C, topic);
    await messageTimeout();
    expect(nodeA.isLeader()).toEqual(true);
    expect(nodeB.isLeader()).toEqual(false);
    expect(nodeC.isLeader()).toEqual(false);
    expect(nodeA.isLeader(nameA)).toEqual(true);
    expect(nodeB.isLeader(nameA)).toEqual(true);
    expect(nodeC.isLeader(nameA)).toEqual(true);
    expect(nodeA.isLeader(nameB)).toEqual(false);
    expect(nodeB.isLeader(nameB)).toEqual(false);
    expect(nodeC.isLeader(nameB)).toEqual(false);
    expect(nodeA.isLeader(nameC)).toEqual(false);
    expect(nodeB.isLeader(nameC)).toEqual(false);
    expect(nodeC.isLeader(nameC)).toEqual(false);
    expect(new Set(nodeA.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    expect(new Set(nodeB.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    expect(new Set(nodeC.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    nodeA.stopHeartbeat();
    await messageTimeout();
    // Node A is leader of itself
    expect(nodeA.isLeader()).toEqual(true);
    expect(nodeB.isLeader()).toEqual(true);
    expect(nodeC.isLeader()).toEqual(false);
    expect(nodeA.isLeader(nameA)).toEqual(true);
    expect(nodeB.isLeader(nameA)).toEqual(false);
    expect(nodeC.isLeader(nameA)).toEqual(false);
    expect(nodeA.isLeader(nameB)).toEqual(false);
    expect(nodeB.isLeader(nameB)).toEqual(true);
    expect(nodeC.isLeader(nameB)).toEqual(true);
    expect(nodeA.isLeader(nameC)).toEqual(false);
    expect(nodeB.isLeader(nameC)).toEqual(false);
    expect(nodeC.isLeader(nameC)).toEqual(false);
    expect(new Set(nodeA.pipelineConsumers(topic))).toEqual(new Set([nameA]));
    expect(new Set(nodeB.pipelineConsumers(topic))).toEqual(new Set([nameB, nameC]));
    expect(new Set(nodeC.pipelineConsumers(topic))).toEqual(new Set([nameB, nameC]));
    nodeA.startHeartbeat();
    nodeA.addPeer(addressB);
    await messageTimeout();
    expect(nodeA.isLeader()).toEqual(true);
    expect(nodeB.isLeader()).toEqual(false);
    expect(nodeC.isLeader()).toEqual(false);
    expect(nodeA.isLeader(nameA)).toEqual(true);
    expect(nodeB.isLeader(nameA)).toEqual(true);
    expect(nodeC.isLeader(nameA)).toEqual(true);
    expect(nodeA.isLeader(nameB)).toEqual(false);
    expect(nodeB.isLeader(nameB)).toEqual(false);
    expect(nodeC.isLeader(nameB)).toEqual(false);
    expect(nodeA.isLeader(nameC)).toEqual(false);
    expect(nodeB.isLeader(nameC)).toEqual(false);
    expect(nodeC.isLeader(nameC)).toEqual(false);
    expect(new Set(nodeA.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    expect(new Set(nodeB.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    expect(new Set(nodeC.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    await nodeA.close();
    await nodeB.close();
    await nodeC.close();
    nodeA.throwOnLeakedReferences();
    nodeB.throwOnLeakedReferences();
    nodeC.throwOnLeakedReferences();
  });

  test('Node A, B, and C reconnect and send pipeline events after a heartbeat timeout followed by a connection by a new peer A2', async () => {
    nodeA = await getNode(nameA, addressA, [], 50);
    nodeB = await getNode(nameB, addressB, [], 50);
    nodeC = await getNode(nameC, addressC, [], 50);
    nodeB.addPeer(addressA);
    nodeC.addPeer(addressA);
    await messageTimeout();
    nodeA.providePipeline(topic);
    nodeB.providePipeline(topic);
    nodeC.providePipeline(topic);
    nodeA.consumePipeline(NANOMSG_TOPIC_PORT_A, topic);
    nodeB.consumePipeline(NANOMSG_TOPIC_PORT_B, topic);
    nodeC.consumePipeline(NANOMSG_TOPIC_PORT_C, topic);
    await messageTimeout();
    expect(nodeA.isLeader()).toEqual(true);
    expect(nodeB.isLeader()).toEqual(false);
    expect(nodeC.isLeader()).toEqual(false);
    expect(nodeA.isLeader(nameA)).toEqual(true);
    expect(nodeB.isLeader(nameA)).toEqual(true);
    expect(nodeC.isLeader(nameA)).toEqual(true);
    expect(nodeA.isLeader(nameB)).toEqual(false);
    expect(nodeB.isLeader(nameB)).toEqual(false);
    expect(nodeC.isLeader(nameB)).toEqual(false);
    expect(nodeA.isLeader(nameC)).toEqual(false);
    expect(nodeB.isLeader(nameC)).toEqual(false);
    expect(nodeC.isLeader(nameC)).toEqual(false);
    expect(new Set(nodeA.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    expect(new Set(nodeB.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    expect(new Set(nodeC.pipelineConsumers(topic))).toEqual(new Set([nameA, nameB, nameC]));
    nodeA.stopHeartbeat();
    await messageTimeout();
    // Node A is leader of itself
    expect(nodeA.isLeader()).toEqual(true);
    expect(nodeB.isLeader()).toEqual(true);
    expect(nodeC.isLeader()).toEqual(false);
    expect(nodeA.isLeader(nameA)).toEqual(true);
    expect(nodeB.isLeader(nameA)).toEqual(false);
    expect(nodeC.isLeader(nameA)).toEqual(false);
    expect(nodeA.isLeader(nameB)).toEqual(false);
    expect(nodeB.isLeader(nameB)).toEqual(true);
    expect(nodeC.isLeader(nameB)).toEqual(true);
    expect(nodeA.isLeader(nameC)).toEqual(false);
    expect(nodeB.isLeader(nameC)).toEqual(false);
    expect(nodeC.isLeader(nameC)).toEqual(false);
    expect(new Set(nodeA.pipelineConsumers(topic))).toEqual(new Set([nameA]));
    expect(new Set(nodeB.pipelineConsumers(topic))).toEqual(new Set([nameB, nameC]));
    expect(new Set(nodeC.pipelineConsumers(topic))).toEqual(new Set([nameB, nameC]));
    await nodeA.close();
    nodeA2 = await getNode(nameA2, addressA, [], 50);
    await nodeA2.addPeer(addressB);
    nodeA2.providePipeline(topic);
    nodeA2.consumePipeline(NANOMSG_TOPIC_PORT_A, topic);
    await messageTimeout();
    expect(nodeA2.isLeader()).toEqual(false);
    expect(nodeB.isLeader()).toEqual(true);
    expect(nodeC.isLeader()).toEqual(false);
    expect(nodeA2.isLeader(nameA2)).toEqual(false);
    expect(nodeB.isLeader(nameA2)).toEqual(false);
    expect(nodeC.isLeader(nameA2)).toEqual(false);
    expect(nodeA2.isLeader(nameB)).toEqual(true);
    expect(nodeB.isLeader(nameB)).toEqual(true);
    expect(nodeC.isLeader(nameB)).toEqual(true);
    expect(nodeA2.isLeader(nameC)).toEqual(false);
    expect(nodeB.isLeader(nameC)).toEqual(false);
    expect(nodeC.isLeader(nameC)).toEqual(false);
    expect(new Set(nodeA2.pipelineConsumers(topic))).toEqual(new Set([nameA2, nameB, nameC]));
    expect(new Set(nodeB.pipelineConsumers(topic))).toEqual(new Set([nameA2, nameB, nameC]));
    expect(new Set(nodeC.pipelineConsumers(topic))).toEqual(new Set([nameA2, nameB, nameC]));
    await nodeA2.close();
    await nodeB.close();
    await nodeC.close();
    nodeA.throwOnLeakedReferences();
    nodeA2.throwOnLeakedReferences();
    nodeB.throwOnLeakedReferences();
    nodeC.throwOnLeakedReferences();
  });
});
