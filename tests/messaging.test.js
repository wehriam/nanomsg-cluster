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
const callbackA = jest.fn();
const callbackAA = jest.fn();
const callbackB = jest.fn();
const callbackC = jest.fn();
const nameA = uuid.v4();
const nameB = uuid.v4();
const nameC = uuid.v4();
const topic1 = uuid.v4();
const topic1A = uuid.v4();
const topic2 = uuid.v4();
const message1 = { [uuid.v4()]: uuid.v4() };
const message2 = { [uuid.v4()]: uuid.v4() };
const message3 = { [uuid.v4()]: uuid.v4() };
const message4 = { [uuid.v4()]: uuid.v4() };
const message5 = { [uuid.v4()]: uuid.v4() };
const message6 = { [uuid.v4()]: uuid.v4() };
const message7 = { [uuid.v4()]: uuid.v4() };
const message8 = { [uuid.v4()]: uuid.v4() };
const message9 = { [uuid.v4()]: uuid.v4() };

beforeAll(async () => {
  nodeA = await getNode(nameA, addressA, []);
  nodeB = await getNode(nameB, addressB, [addressA]);
  nodeC = await getNode(nameC, addressC, [addressA]);
});

test('subscribes to a topic', async () => {
  nodeA.subscribe(topic1, callbackA);
  nodeB.subscribe(topic1, callbackB);
  nodeC.subscribe(topic1, callbackC);
  expect(callbackA).toHaveBeenCalledTimes(0);
  await messageTimeout();
});

test('nodeB sends a message', async () => {
  nodeB.sendToAll(topic1, message1);
  await messageTimeout();
});

test('nodeA and nodeC have received the message', () => {
  expect(callbackA).toHaveBeenCalledWith(message1, nameB);
  expect(callbackB).toHaveBeenCalledTimes(0);
  expect(callbackC).toHaveBeenCalledWith(message1, nameB);
});

test('nodeC sends a message', async () => {
  nodeC.sendToAll(topic1, message2);
  await messageTimeout();
});

test('nodeA and nodeB have received the message', () => {
  expect(callbackA).toHaveBeenCalledWith(message2, nameC);
  expect(callbackB).toHaveBeenCalledWith(message2, nameC);
  expect(callbackC).toHaveBeenCalledWith(message1, nameB);
});

test('nodeA and nodeC send messages at the same time', async () => {
  nodeC.sendToAll(topic1, message3);
  nodeA.sendToAll(topic1, message4);
  await messageTimeout();
});

test('nodeA and nodeB have received the message', () => {
  expect(callbackA).toHaveBeenCalledWith(message3, nameC);
  expect(callbackB).toHaveBeenCalledWith(message4, nameA);
  expect(callbackB).toHaveBeenCalledWith(message3, nameC);
  expect(callbackC).toHaveBeenCalledWith(message4, nameA);
});

test('nodeA sends a message to itself', async () => {
  nodeA.subscribe(topic1A, callbackAA, true);
  nodeB.subscribe(topic1A, callbackB, true);
  nodeA.sendToAll(topic1A, message8);
  await messageTimeout();
  expect(callbackAA).toHaveBeenCalledWith(message8, nameA);
  expect(callbackB).toHaveBeenCalledWith(message8, nameA);
});

test('nodeA unsubscribes from messages to itself', async () => {
  nodeA.unsubscribe(topic1A, callbackAA);
  nodeA.sendToAll(topic1A, message9);
  await messageTimeout();
  expect(callbackAA).not.toHaveBeenCalledWith(message9, nameA);
  expect(callbackB).toHaveBeenCalledWith(message9, nameA);
});

test('nodeA sends a message', async () => {
  nodeA.sendToAll(topic1, message5);
  await messageTimeout();
  expect(callbackA).not.toHaveBeenCalledWith(message5);
});

test('only connector c has received the message', async () => {
  nodeA.sendToPeer(nameC, topic1, message6);
  await messageTimeout();
  expect(callbackA).not.toHaveBeenCalledWith(message6, nameA);
  expect(callbackB).not.toHaveBeenCalledWith(message6, nameA);
  expect(callbackC).toHaveBeenCalledWith(message6, nameA);
});

test('should remove a peerAddress from the cluster', async () => {
  nodeA.subscribe(topic2, callbackA);
  nodeB.subscribe(topic2, callbackB);
  nodeC.subscribe(topic2, callbackC);
  nodeC.removePeer(addressA);
  await messageTimeout();
  nodeA.sendToAll(topic2, message7);
  expect(callbackA).not.toHaveBeenCalledWith(message7);
  expect(callbackB).not.toHaveBeenCalledWith(message7);
  expect(callbackC).not.toHaveBeenCalledWith(message7);
});

test('nodeB can not be closed more than once.', async () => {
  await nodeB.close();
  try {
    await nodeB.close();
  } catch (e) {
    expect(() => {
      throw e;
    }).toThrow(/Already closed/);
  }
  await messageTimeout();
});

test('nodeA and nodeC close gracefuly.', async () => {
  await nodeA.close();
  await nodeC.close();
});

