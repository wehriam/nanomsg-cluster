// @flow

const { messageTimeout, getNode } = require('./lib/node');

const uuid = require('uuid');
const ip = require('ip');

const HOST = ip.address();
let port = 7000;
const NANOMSG_PUBSUB_PORT_A = port++;
const NANOMSG_PUBSUB_PORT_B = port++;
const NANOMSG_PUBSUB_PORT_C = port++;
const NANOMSG_PIPELINE_PORT_A = port++;
const NANOMSG_PIPELINE_PORT_B = port++;
const NANOMSG_PIPELINE_PORT_C = port++;
const discoveryPort = port++;

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
let nodeB;
let nodeC;
const nameA = uuid.v4();
const nameB = uuid.v4();
const nameC = uuid.v4();

jest.setTimeout(10000);

beforeAll(async () => {
  nodeA = await getNode(nameA, addressA, []);
  nodeB = await getNode(nameB, addressB, []);
  nodeC = await getNode(nameC, addressC, []);
  await messageTimeout();
});

test('nodeA starts discovery.', async () => {
  await nodeA.startDiscovery({
    port: discoveryPort,
    ignoreProcess: false,
  });
});

test('nodeB starts discovery.', async () => {
  const nodeAAddPeerBPromise = new Promise((resolve) => {
    nodeA.on('addPeer', (peerAddress) => {
      if (peerAddress.name === nameB) {
        resolve();
      }
    });
  });
  const nodeBAddPeerAPromise = new Promise((resolve) => {
    nodeB.on('addPeer', (peerAddress) => {
      if (peerAddress.name === nameA) {
        resolve();
      }
    });
  });
  await nodeB.startDiscovery({
    port: discoveryPort,
    ignoreProcess: false,
  });
  await nodeAAddPeerBPromise;
  await nodeBAddPeerAPromise;
});

test('nodeC starts discovery.', async () => {
  const nodeAAddPeerCPromise = new Promise((resolve) => {
    nodeA.on('addPeer', (peerAddress) => {
      if (peerAddress.name === nameC) {
        resolve();
      }
    });
  });
  const nodeBAddPeerCPromise = new Promise((resolve) => {
    nodeB.on('addPeer', (peerAddress) => {
      if (peerAddress.name === nameC) {
        resolve();
      }
    });
  });
  const nodeCAddPeerAPromise = new Promise((resolve) => {
    nodeC.on('addPeer', (peerAddress) => {
      if (peerAddress.name === nameA) {
        resolve();
      }
    });
  });
  const nodeCAddPeerBPromise = new Promise((resolve) => {
    nodeC.on('addPeer', (peerAddress) => {
      if (peerAddress.name === nameA) {
        resolve();
      }
    });
  });
  await nodeC.startDiscovery({
    port: discoveryPort,
    ignoreProcess: false,
  });
  await nodeAAddPeerCPromise;
  await nodeBAddPeerCPromise;
  await nodeCAddPeerAPromise;
  await nodeCAddPeerBPromise;
});

test('nodeA closes gracefuly.', async () => {
  const nodeBRemovePeerAPromise = new Promise((resolve) => {
    nodeB.on('removePeer', (peerAddress) => {
      if (peerAddress.name === nameA) {
        resolve();
      }
    });
  });
  const nodeCRemovePeerAPromise = new Promise((resolve) => {
    nodeC.on('removePeer', (peerAddress) => {
      if (peerAddress.name === nameA) {
        resolve();
      }
    });
  });
  await nodeA.close();
  await nodeBRemovePeerAPromise;
  await nodeCRemovePeerAPromise;
  nodeA.throwOnLeakedReferences();
});

test('nodeB closes gracefuly.', async () => {
  const nodeCRemovePeerBPromise = new Promise((resolve) => {
    nodeC.on('removePeer', (peerAddress) => {
      if (peerAddress.name === nameB) {
        resolve();
      }
    });
  });
  await nodeB.close();
  await nodeCRemovePeerBPromise;
  nodeB.throwOnLeakedReferences();
});

test('nodeC closes gracefuly.', async () => {
  await nodeC.close();
  nodeC.throwOnLeakedReferences();
});

