// @flow

import type { SocketSettings } from '../src';

const uuid = require('uuid');
const expect = require('expect');
const ClusterNode = require('../src');

const HOST = '127.0.0.1';
let port = 7000;
const NANOMSG_PUBSUB_PORT_A = port++;
const NANOMSG_PUBSUB_PORT_B = port++;
const NANOMSG_PUBSUB_PORT_C = port++;
const NANOMSG_PUBSUB_PORT_D = port++;
const NANOMSG_PUBSUB_PORT_E = port++;
const NANOMSG_PIPELINE_PORT_A = port++;
const NANOMSG_PIPELINE_PORT_B = port++;
const NANOMSG_PIPELINE_PORT_C = port++;
const NANOMSG_PIPELINE_PORT_D = port++;
const NANOMSG_PIPELINE_PORT_E = port++;
const NANOMSG_TOPIC_PORT_A = port++;
const NANOMSG_TOPIC_PORT_B = port++;
const NANOMSG_TOPIC_PORT_C = port++;
const NANOMSG_TOPIC_PORT_D = port++;
const NANOMSG_TOPIC_PORT_E = port++;

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

const addressD = {
  host: HOST,
  pubsubPort: NANOMSG_PUBSUB_PORT_D,
  pipelinePort: NANOMSG_PIPELINE_PORT_D,
};

const addressE = {
  host: HOST,
  pubsubPort: NANOMSG_PUBSUB_PORT_E,
  pipelinePort: NANOMSG_PIPELINE_PORT_E,
};

const messageTimeout = () => new Promise((resolve) => setTimeout(resolve, 250));

const getNode = async (name:string, bindAddress:SocketSettings, peerAddresses:Array<SocketSettings>):Promise<ClusterNode> => {
  const node = new ClusterNode({
    name,
    cluster: {
      bindAddress,
      peerAddresses,
    },
  });
  expect(node.isReady).toEqual(false);
  await new Promise((resolve, reject) => {
    node.on('ready', resolve);
    node.on('error', reject);
  });
  expect(node.isReady).toEqual(true);
  return node;
};

let nodeA;
let nodeB;
let nodeC;
let nodeD;
let nodeE;
const callback1 = jest.fn();
const callback2 = jest.fn();
const callback3 = jest.fn();
const callback4 = jest.fn();
const nameA = uuid.v4();
const nameB = uuid.v4();
const nameC = uuid.v4();
const nameD = uuid.v4();
const nameE = uuid.v4();
const topic1 = uuid.v4();
const topic2 = uuid.v4();
const topic3 = uuid.v4();
const topic4 = uuid.v4();
const message1 = { [uuid.v4()]: uuid.v4() };
const message2 = { [uuid.v4()]: uuid.v4() };
const message3 = { [uuid.v4()]: uuid.v4() };
const message4 = { [uuid.v4()]: uuid.v4() };
const message5 = { [uuid.v4()]: uuid.v4() };

beforeAll(async () => {
  nodeA = await getNode(nameA, addressA, []);
  nodeB = await getNode(nameB, addressB, [addressA]);
  nodeC = await getNode(nameC, addressC, [addressA]);
  nodeD = await getNode(nameD, addressD, [addressA]);
  nodeE = await getNode(nameE, addressE, [addressA]);
});

test('nodeA, nodeB, and nodeC provide a topic', async () => {
  nodeA.providePipeline(topic1);
  nodeB.providePipeline(topic2);
  nodeC.providePipeline(topic2);
  await messageTimeout();
});

test('nodeB and nodeC consume and subscribe to topic1', async () => {
  nodeB.consumePipeline(NANOMSG_TOPIC_PORT_B, topic1);
  nodeC.consumePipeline(NANOMSG_TOPIC_PORT_C, topic1);
  nodeB.subscribe(topic1, callback1);
  nodeC.subscribe(topic1, callback1);
  await messageTimeout();
});

test('nodeA consumes and subscribes to topic2', async () => {
  nodeA.consumePipeline(NANOMSG_TOPIC_PORT_A, topic2);
  nodeA.subscribe(topic2, callback2);
  await messageTimeout();
});

test('nodeA sends a message on topic1', async () => {
  nodeA.sendToPipeline(topic1, message1);
  await messageTimeout();
  expect(callback1).toHaveBeenCalledTimes(1);
  expect(callback1).toHaveBeenCalledWith(message1, nameA);
});

test('nodeB sends a message on topic2', async () => {
  nodeB.sendToPipeline(topic2, message2);
  await messageTimeout();
  expect(callback2).toHaveBeenCalledTimes(1);
  expect(callback2).toHaveBeenCalledWith(message2, nameB);
});

test('nodeC sends a message on topic2', async () => {
  nodeC.sendToPipeline(topic2, message2);
  await messageTimeout();
  expect(callback2).toHaveBeenCalledTimes(2);
  expect(callback2).toHaveBeenCalledWith(message2, nameC);
});

test('nodeD provides topic1', async () => {
  nodeD.providePipeline(topic1);
  await messageTimeout();
});

test('nodeD sends a message on topic1', async () => {
  nodeD.sendToPipeline(topic1, message3);
  await messageTimeout();
  expect(callback1).toHaveBeenCalledTimes(2);
  expect(callback1).toHaveBeenCalledWith(message3, nameD);
});

test('nodeD provides topic3 first', async () => {
  nodeD.providePipeline(topic3);
  await messageTimeout();
});

test('nodeD consumes and subscribes to topic3 second', async () => {
  nodeD.consumePipeline(NANOMSG_TOPIC_PORT_D, topic3);
  nodeD.subscribe(topic3, callback3);
  await messageTimeout();
});

test('nodeD sends a message on topic3', async () => {
  nodeD.sendToPipeline(topic3, message4);
  await messageTimeout();
  expect(callback3).toHaveBeenCalledTimes(1);
  expect(callback3).toHaveBeenCalledWith(message4, nameD);
});

test('nodeE consumes and subscribes to topic4 first', async () => {
  nodeE.consumePipeline(NANOMSG_TOPIC_PORT_E, topic4);
  nodeE.subscribe(topic4, callback4);
  await messageTimeout();
});

test('nodeE provides topic4 second', async () => {
  nodeE.providePipeline(topic4);
  await messageTimeout();
});

test('nodeE sends a message on topic4', async () => {
  nodeE.sendToPipeline(topic4, message5);
  await messageTimeout();
  expect(callback4).toHaveBeenCalledTimes(1);
  expect(callback4).toHaveBeenCalledWith(message5, nameE);
});

test('nodeA closes gracefuly.', async () => {
  const nodeBRemovePeerPromise = new Promise((resolve) => {
    nodeB.on('removePeer', resolve);
  });
  const nodeCRemovePeerPromise = new Promise((resolve) => {
    nodeC.on('removePeer', resolve);
  });
  const nodeDRemovePeerPromise = new Promise((resolve) => {
    nodeD.on('removePeer', resolve);
  });
  const nodeERemovePeerPromise = new Promise((resolve) => {
    nodeE.on('removePeer', resolve);
  });
  await nodeA.close();
  await nodeBRemovePeerPromise;
  await nodeCRemovePeerPromise;
  await nodeDRemovePeerPromise;
  await nodeERemovePeerPromise;
  await messageTimeout();
});

test('nodeA, nodeC, nodeD, and nodeE close gracefuly.', async () => {
  await nodeB.close();
  await nodeC.close();
  await nodeD.close();
  await nodeE.close();
});

