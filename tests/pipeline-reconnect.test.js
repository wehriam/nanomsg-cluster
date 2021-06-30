// @flow

const { messageTimeout, getNode } = require('./lib/node');

const uuid = require('uuid');

const HOST = '127.0.0.1';
const topic = uuid.v4();

let port = 10000 + Math.round(Math.random() * 10000);

const addresses = {};
const nodes = {};

const spawnNode = async () => {
  const name = uuid.v4();
  const address = {
    host: HOST,
    pubsubPort: port++,
    pipelinePort: port++,
  };
  const node = await getNode(name, address, [], 50);
  addresses[name] = address;
  nodes[name] = node;
  return node;
};

const closeNode = async (name:string) => {
  const node = nodes[name];
  delete addresses[name];
  delete nodes[name];
  if (node.closed) {
    return;
  }
  await node.close();
};

describe('Pipeline Reconnect', () => {
  beforeAll(async () => {
    await messageTimeout();
  });

  test('Node A, B, and C reconnect and send pipeline events', async () => {
    const spawnedNodes = await Promise.all([spawnNode(), spawnNode(), spawnNode()]);
    spawnedNodes.sort((x, y) => (x.name < y.name ? -1 : 1));
    const [nodeA, nodeB, nodeC] = spawnedNodes;
    const [nameA, nameB, nameC] = [nodeA.name, nodeB.name, nodeC.name];
    const addressA = addresses[nameA];
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
    nodeA.consumePipeline(port++, topic);
    nodeB.consumePipeline(port++, topic);
    nodeC.consumePipeline(port++, topic);
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
    await closeNode(nameA);
    await closeNode(nameB);
    await closeNode(nameC);
    nodeA.throwOnLeakedReferences();
    nodeB.throwOnLeakedReferences();
    nodeC.throwOnLeakedReferences();
  });


  test('Node A, B, and C reconnect and send pipeline events after a heartbeat timeout', async () => {
    const spawnedNodes = await Promise.all([spawnNode(), spawnNode(), spawnNode()]);
    spawnedNodes.sort((x, y) => (x.name < y.name ? -1 : 1));
    const [nodeA, nodeB, nodeC] = spawnedNodes;
    const [nameA, nameB, nameC] = [nodeA.name, nodeB.name, nodeC.name];
    const addressA = addresses[nameA];
    const addressB = addresses[nameB];
    nodeB.addPeer(addressA);
    nodeC.addPeer(addressA);
    await messageTimeout();
    nodeA.providePipeline(topic);
    nodeB.providePipeline(topic);
    nodeC.providePipeline(topic);
    nodeA.consumePipeline(port++, topic);
    nodeB.consumePipeline(port++, topic);
    nodeC.consumePipeline(port++, topic);
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
    await closeNode(nameA);
    await closeNode(nameB);
    await closeNode(nameC);
    nodeA.throwOnLeakedReferences();
    nodeB.throwOnLeakedReferences();
    nodeC.throwOnLeakedReferences();
  });

  test.skip('Node A, B, and C reconnect and send pipeline events after a heartbeat timeout followed by a connection by a new peer A2', async () => {
    const spawnedNodes = await Promise.all([spawnNode(), spawnNode(), spawnNode()]);
    spawnedNodes.sort((x, y) => (x.name < y.name ? -1 : 1));
    const [nodeA, nodeB, nodeC] = spawnedNodes;
    const [nameA, nameB, nameC] = [nodeA.name, nodeB.name, nodeC.name];
    const addressA = addresses[nameA];
    const addressB = addresses[nameB];
    nodeB.addPeer(addressA);
    nodeC.addPeer(addressA);
    await messageTimeout();
    nodeA.providePipeline(topic);
    nodeB.providePipeline(topic);
    nodeC.providePipeline(topic);
    nodeA.consumePipeline(port++, topic);
    nodeB.consumePipeline(port++, topic);
    nodeC.consumePipeline(port++, topic);
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
    await closeNode(nameA);
    let nameA2 = uuid.v4();
    while (nameA2 < nameB) {
      nameA2 = uuid.v4();
    }
    await messageTimeout();
    const nodeA2 = await getNode(nameA2, addressA, [], 50);
    await nodeA2.addPeer(addressB);
    nodeA2.providePipeline(topic);
    nodeA2.consumePipeline(port++, topic);
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
    //await nodeA2.close();
    await closeNode(nameA2);
    await closeNode(nameB);
    await closeNode(nameC);
    nodeA.throwOnLeakedReferences();
    nodeA2.throwOnLeakedReferences();
    nodeB.throwOnLeakedReferences();
    nodeC.throwOnLeakedReferences();
  });

  test.skip('Node A, B, and C reconnect and send pipeline events after a dirty disconnect, followed by a connection by a new peer A2 from the same ports', async () => {
    const spawnedNodes = await Promise.all([spawnNode(), spawnNode(), spawnNode()]);
    spawnedNodes.sort((x, y) => (x.name < y.name ? -1 : 1));
    const [nodeA, nodeB, nodeC] = spawnedNodes;
    const [nameA, nameB, nameC] = [nodeA.name, nodeB.name, nodeC.name];
    const addressA = addresses[nameA];
    const addressB = addresses[nameB];
    nodeB.addPeer(addressA);
    nodeC.addPeer(addressA);
    await messageTimeout();
    nodeA.providePipeline(topic);
    nodeB.providePipeline(topic);
    nodeC.providePipeline(topic);
    nodeA.consumePipeline(port++, topic);
    nodeB.consumePipeline(port++, topic);
    nodeC.consumePipeline(port++, topic);
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
    nodeA.dirtyClose();
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
    // await nodeA.close();
    let nameA2 = uuid.v4();
    while (nameA2 < nameB) {
      nameA2 = uuid.v4();
    }
    await messageTimeout();
    const nodeA2 = await getNode(nameA2, addressA, [], 50);
    await nodeA2.addPeer(addressB);
    nodeA2.providePipeline(topic);
    nodeA2.consumePipeline(port++, topic);
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
    await closeNode(nameA);
    await nodeA2.close();
    await closeNode(nameB);
    await closeNode(nameC);
    // nodeA.throwOnLeakedReferences();
    nodeA2.throwOnLeakedReferences();
    nodeB.throwOnLeakedReferences();
    nodeC.throwOnLeakedReferences();
  });
});
