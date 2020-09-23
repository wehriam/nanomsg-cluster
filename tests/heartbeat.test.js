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

beforeAll(async () => {
  nodeA = await getNode(nameA, addressA, [], 50);
  nodeB = await getNode(nameB, addressB, [], 50);
  nodeC = await getNode(nameC, addressC, [], 50);
  nodeB.addPeer(addressA);
  nodeA.addPeer(addressC);
  await messageTimeout();
});


test('nodeA closes ungracefully, nodeB and nodeC remove peer.', async () => {
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
  nodeA.stopDiscovery();
  if (nodeA.clusterUpdateTimeout) {
    clearTimeout(nodeA.clusterUpdateTimeout);
  }
  if (nodeA.clusterHeartbeatInterval) {
    clearInterval(nodeA.clusterHeartbeatInterval);
  }
  await Promise.all(Object.keys(nodeA.pipelinePushSockets).map(nodeA.closePipelinePushSocket.bind(nodeA)));
  await Promise.all(Object.keys(nodeA.pipelinePullSockets).map(nodeA.closePipelinePullSocket.bind(nodeA)));
  nodeA.stopHeartbeat();
  await nodeBRemovePeerAPromise;
  await nodeCRemovePeerAPromise;
  await nodeA.close();
  await nodeB.close();
  await nodeC.close();
  nodeA.throwOnLeakedReferences();
  nodeB.throwOnLeakedReferences();
  nodeC.throwOnLeakedReferences();
});

