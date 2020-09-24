// @flow

const uuid = require('uuid');
const { messageTimeout, getNode } = require('./lib/node');


const HOST = '127.0.0.1';
let port = 7000;

const addresses = {};
const nodes = {};

const spawnNode = async () => {
  const name = uuid.v4();
  const address = {
    host: HOST,
    pubsubPort: port++,
    pipelinePort: port++,
  };
  const node = await getNode(name, address, []);
  addresses[name] = address;
  nodes[name] = node;
  return node;
};

const closeNode = async (name:string) => {
  const node = nodes[name];
  delete addresses[name];
  delete nodes[name];
  await node.close();
};

const getRandomNodeName = (...args) => {
  const nodeNames = Object.keys(nodes);
  const nodeName = nodeNames[Math.floor(Math.random() * nodeNames.length)];
  if (args.includes(nodeName)) {
    return getRandomNodeName(...args);
  }
  return nodeName;
};

describe.skip('Chaos testing', () => {
  jest.setTimeout(30000);

  beforeAll(async () => {
    const seedNode = await spawnNode();
    for (let i = 0; i < 10; i += 1) {
      const node = await spawnNode();
      await seedNode.addPeer(addresses[node.name]);
    }
    await messageTimeout();
  });

  afterAll(async () => {
    for (const name of Object.keys(nodes)) {
      await closeNode(name);
    }
  });

  test('Disconnects a node', async () => {
    for (let i = 0; i < 5; i += 1) {
      const callbacks = {};
      const topic1 = uuid.v4();
      for (const name of Object.keys(nodes)) {
        const callback = jest.fn();
        callbacks[name] = callback;
        const node = nodes[name];
        node.subscribe(topic1, callback, true);
      }
      await messageTimeout();
      const nameA = getRandomNodeName();
      const nodeA = nodes[nameA];
      const message1 = { [uuid.v4()]: uuid.v4() };
      nodeA.sendToAll(topic1, message1);
      await messageTimeout();
      for (const name of Object.keys(nodes)) {
        const callback = callbacks[name];
        expect(callback).toHaveBeenCalledWith(message1, nameA);
        const node = nodes[name];
        node.unsubscribe(topic1, callback);
      }
      const topic2 = uuid.v4();
      const topic2Callback = jest.fn();
      const message2 = { [uuid.v4()]: uuid.v4() };
      const nameB = getRandomNodeName(nameA);
      const nodeB = nodes[nameB];
      nodeB.providePipeline(topic2);
      await messageTimeout();
      for (const name of Object.keys(nodes)) {
        const node = nodes[name];
        node.consumePipeline(port++, topic2);
        node.subscribe(topic2, topic2Callback);
      }
      await messageTimeout();
      expect(nodeB.pipelineConsumers(topic2)).toEqual(expect.arrayContaining(Object.keys(nodes)));
      nodeB.sendToPipeline(topic2, message2);
      await messageTimeout();
      expect(topic2Callback).toHaveBeenCalledWith(message2, nameB);
      for (const name of Object.keys(nodes)) {
        const node = nodes[name];
        await node.stopConsumingPipeline(topic2);
        node.unsubscribe(topic2, topic2Callback);
      }
      await messageTimeout();
      const nameC = getRandomNodeName(nameA, nameB);
      const nodeC = nodes[nameC];
      nodeC.stopHeartbeat();
      await messageTimeout();
      nodeC.startHeartbeat();
      const nameD = getRandomNodeName(nameA, nameB, nameC);
      await nodeC.addPeer(nameD);
      await messageTimeout();
      const nodeE = await spawnNode();
      await nodeE.addPeer(addresses[nameC]);
      await messageTimeout();
      const nameF = getRandomNodeName(nameA, nameB, nameC, nameD, nodeE);
      const nodeF = nodes[nameF];
      await closeNode(nameF);
      nodeF.throwOnLeakedReferences();
    }
  });
});

