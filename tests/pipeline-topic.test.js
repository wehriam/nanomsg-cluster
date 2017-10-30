// @flow

import type { SocketSettings } from '../src';

const uuid = require('uuid');
const expect = require('expect');
const ClusterNode = require('../src');

const HOST = '127.0.0.1';
const NANOMSG_PUBSUB_PORT_A = 7789;
const NANOMSG_PUBSUB_PORT_B = 7889;
const NANOMSG_PUBSUB_PORT_C = 7989;
const NANOMSG_PIPELINE_PORT_A = 8789;
const NANOMSG_PIPELINE_PORT_B = 8889;
const NANOMSG_PIPELINE_PORT_C = 8989;
const NANOMSG_TOPIC_PORT_A = 9789;
const NANOMSG_TOPIC_PORT_B = 9889;
const NANOMSG_TOPIC_PORT_C = 9989;

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
const callback1 = jest.fn();
const callback2 = jest.fn();
const nameA = uuid.v4();
const nameB = uuid.v4();
const nameC = uuid.v4();
const topic1 = uuid.v4();
const topic2 = uuid.v4();
const message1 = { [uuid.v4()]: uuid.v4() };
const message2 = { [uuid.v4()]: uuid.v4() };

beforeAll(async () => {
  nodeA = await getNode(nameA, addressA, []);
  nodeB = await getNode(nameB, addressB, [addressA]);
  nodeC = await getNode(nameC, addressC, [addressA]);
});

test('nodeA, nodeB, and nodeC provide a pipeline topic', async () => {
  nodeA.providePipelineTopic(topic1);
  nodeB.providePipelineTopic(topic2);
  nodeC.providePipelineTopic(topic2);
  await messageTimeout();
});

test('nodeB and nodeC subscribe to pipeline topic1', async () => {
  nodeB.subscribePipelineTopic(NANOMSG_TOPIC_PORT_B, topic1, callback1);
  nodeC.subscribePipelineTopic(NANOMSG_TOPIC_PORT_C, topic1, callback1);
  await messageTimeout();
});

test('nodeA subscribes to pipeline topic2', async () => {
  nodeA.subscribePipelineTopic(NANOMSG_TOPIC_PORT_A, topic2, callback2);
  await messageTimeout();
});

test('nodeA sends a message on pipeline topic1', async () => {
  nodeA.sendPipeline(topic1, message1);
  await messageTimeout();
  expect(callback1).toHaveBeenCalledTimes(1);
  expect(callback1).toHaveBeenCalledWith(message1, nameA);
});

test('nodeB sends a message on pipeline topic2', async () => {
  nodeB.sendPipeline(topic2, message2);
  await messageTimeout();
  expect(callback2).toHaveBeenCalledTimes(1);
  expect(callback2).toHaveBeenCalledWith(message2, nameB);
});

test('nodeC sends a message on pipeline topic2', async () => {
  nodeC.sendPipeline(topic2, message2);
  await messageTimeout();
  expect(callback2).toHaveBeenCalledTimes(2);
  expect(callback2).toHaveBeenCalledWith(message2, nameC);
});

test('nodeB closes gracefuly.', async () => {
  const nodeBRemovePeerPromise = new Promise((resolve) => {
    nodeB.on('removePeer', resolve);
  });
  const nodeCRemovePeerPromise = new Promise((resolve) => {
    nodeC.on('removePeer', resolve);
  });
  await nodeA.close();
  await nodeBRemovePeerPromise;
  await nodeCRemovePeerPromise;
  await messageTimeout();
});

test('nodeA and nodeC close gracefuly.', async () => {
  await nodeB.close();
  await nodeC.close();
});

