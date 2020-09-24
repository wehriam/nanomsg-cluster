// @flow

const { messageTimeout, getNode } = require('./lib/node');

const uuid = require('uuid');

const HOST = '127.0.0.1';
let port = 7000;
const NANOMSG_PUBSUB_PORT_A = port++;
const NANOMSG_PUBSUB_PORT_B = port++;
const NANOMSG_PIPELINE_PORT_A = port++;
const NANOMSG_PIPELINE_PORT_B = port++;

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

let nodeA;
let nodeB;

const nameA = uuid.v4();
const nameB = uuid.v4();

describe('Peer Cleanup', () => {
  test('Node A and Node B cleanup after disconnect', async () => {
    nodeA = await getNode(nameA, addressA, [], 50);
    nodeB = await getNode(nameB, addressB, [], 50);
    nodeB.addPeer(addressA);
    await messageTimeout();
    expect(nodeA.getPeerNames()).toEqual([nameB]);
    expect(nodeB.getPeerNames()).toEqual([nameA]);
    await nodeA.close();
    await nodeB.close();
    nodeA.throwOnLeakedReferences();
    nodeB.throwOnLeakedReferences();
  });

  test('Node B disconnects in response to node A disconnect', async () => {
    nodeA = await getNode(nameA, addressA, [], 50);
    nodeB = await getNode(nameB, addressB, [], 50);
    nodeB.addPeer(addressA);
    await messageTimeout();
    expect(nodeA.getPeerNames()).toEqual([nameB]);
    expect(nodeB.getPeerNames()).toEqual([nameA]);
    await nodeA.removePeer(addressB);
    await messageTimeout();
    nodeA.throwOnLeakedReferences();
    nodeB.throwOnLeakedReferences();
    await nodeA.close();
    await nodeB.close();
  });


  test('Node A disconnects in response to node B heartbeat timeout', async () => {
    nodeA = await getNode(nameA, addressA, [], 50);
    nodeB = await getNode(nameB, addressB, [], 50);
    nodeB.addPeer(addressA);
    await messageTimeout();
    expect(nodeA.getPeerNames()).toEqual([nameB]);
    expect(nodeB.getPeerNames()).toEqual([nameA]);
    clearInterval(nodeB.clusterHeartbeatInterval);
    await messageTimeout();
    nodeA.throwOnLeakedReferences();
    nodeB.throwOnLeakedReferences();
    await nodeA.close();
    await nodeB.close();
  });
});

