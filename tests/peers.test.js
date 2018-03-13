// @flow

const { messageTimeout, getNode } = require('./lib/node');

const uuid = require('uuid');
const expect = require('expect');

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
  nodeA = await getNode(nameA, addressA, []);
  await messageTimeout();
});

test('nodeB starts gracefuly.', async () => {
  nodeB = await getNode(nameB, addressB, []);
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
  nodeB.addPeer(addressA);
  await nodeAAddPeerBPromise;
  await nodeBAddPeerAPromise;
});

test('nodeC starts gracefuly.', async () => {
  nodeC = await getNode(nameC, addressC, []);
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
  nodeA.addPeer(addressC);
  await nodeAAddPeerCPromise;
  await nodeBAddPeerCPromise;
  await nodeCAddPeerAPromise;
  await nodeCAddPeerBPromise;
});

test('Nodes choose a leader.', async () => {
  const peers = [nameA, nameB, nameC];
  peers.sort();
  if (nodeA.isLeader()) {
    expect(peers[0]).toEqual(nameA);
  } else {
    expect(peers[0]).not.toEqual(nameA);
  }
  if (nodeB.isLeader()) {
    expect(peers[0]).toEqual(nameB);
  } else {
    expect(peers[0]).not.toEqual(nameB);
  }
  if (nodeC.isLeader()) {
    expect(peers[0]).toEqual(nameC);
  } else {
    expect(peers[0]).not.toEqual(nameC);
  }
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
});

test('nodeC closes gracefuly.', async () => {
  await nodeC.close();
});

