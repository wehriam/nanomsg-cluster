// @flow

const { messageTimeout, getNode } = require('./lib/node');

const uuid = require('uuid');
const expect = require('expect');

const HOST = '127.0.0.1';
let port = 7000;
const NANOMSG_PUBSUB_PORT_A = port++;
const NANOMSG_PUBSUB_PORT_B = port++;
const NANOMSG_PUBSUB_PORT_C = port++;
const NANOMSG_PUBSUB_PORT_D = port++;
const NANOMSG_PUBSUB_PORT_E = port++;
const NANOMSG_PUBSUB_PORT_F = port++;
const NANOMSG_PIPELINE_PORT_A = port++;
const NANOMSG_PIPELINE_PORT_B = port++;
const NANOMSG_PIPELINE_PORT_C = port++;
const NANOMSG_PIPELINE_PORT_D = port++;
const NANOMSG_PIPELINE_PORT_E = port++;
const NANOMSG_PIPELINE_PORT_F = port++;
const NANOMSG_TOPIC_PORT_A = port++;
const NANOMSG_TOPIC_PORT_B = port++;
const NANOMSG_TOPIC_PORT_C = port++;
const NANOMSG_TOPIC_PORT_D = port++;
const NANOMSG_TOPIC_PORT_E = port++;
const NANOMSG_TOPIC_PORT_F = port++;
const NANOMSG_TOPIC_LEADER_PORT_A = port++;
const NANOMSG_TOPIC_LEADER_PORT_B = port++;
const NANOMSG_TOPIC_LEADER_PORT_C = port++;

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

const addressF = {
  host: HOST,
  pubsubPort: NANOMSG_PUBSUB_PORT_F,
  pipelinePort: NANOMSG_PIPELINE_PORT_F,
};

let nodeA;
let nodeB;
let nodeC;
let nodeD;
let nodeE;
let nodeF;
const callback1 = jest.fn();
const callback2 = jest.fn();
const callback3 = jest.fn();
const callback4 = jest.fn();
const nameA = uuid.v4();
const nameB = uuid.v4();
const nameC = uuid.v4();
const nameD = uuid.v4();
const nameE = uuid.v4();
const nameF = uuid.v4();
const topic1 = uuid.v4();
const topic2 = uuid.v4();
const topic3 = uuid.v4();
const topic4 = uuid.v4();
const topicLeader = uuid.v4();
const message1 = { [uuid.v4()]: uuid.v4() };
const message2 = { [uuid.v4()]: uuid.v4() };
const message3 = { [uuid.v4()]: uuid.v4() };
const message4 = { [uuid.v4()]: uuid.v4() };
const message5 = { [uuid.v4()]: uuid.v4() };

describe('Pipeline', () => {
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
    expect(nodeA.hasPipelineConsumer(topic1)).toEqual(false);
    expect(nodeA.pipelineConsumers(topic1)).toEqual([]);
  });

  test('nodeB and nodeC consume and subscribe to topic1', async () => {
    const connectPipelineConsumerPromise = new Promise((resolve) => {
      nodeA.on('connectPipelineConsumer', (topic, name) => {
        if (topic === topic1 && name === nodeB.name) {
          resolve();
        }
      });
    });
    nodeB.consumePipeline(NANOMSG_TOPIC_PORT_B, topic1);
    nodeC.consumePipeline(NANOMSG_TOPIC_PORT_C, topic1);
    nodeB.subscribe(topic1, callback1);
    nodeC.subscribe(topic1, callback1);
    await messageTimeout();
    expect(nodeA.hasPipelineConsumer(topic1)).toEqual(true);
    expect(nodeA.pipelineConsumers(topic1)).toEqual(expect.arrayContaining([nodeB.name, nodeC.name]));
    await connectPipelineConsumerPromise;
  });

  test('nodeF advertises consumers after connecting to nodeA', async () => {
    const connectPipelineConsumerPromise = new Promise((resolve) => {
      nodeA.on('connectPipelineConsumer', (topic, name) => {
        if (topic === topic1 && name === nodeF.name) {
          resolve();
        }
      });
    });
    nodeF = await getNode(nameF, addressF, []);
    nodeF.consumePipeline(NANOMSG_TOPIC_PORT_F, topic1);
    nodeF.addPeer(addressA);
    await connectPipelineConsumerPromise;
    const disconnectPipelineConsumerPromise = new Promise((resolve) => {
      nodeA.on('disconnectPipelineConsumer', (topic, name) => {
        if (topic === topic1 && name === nodeF.name) {
          resolve();
        }
      });
    });
    await nodeF.close();
    await disconnectPipelineConsumerPromise;
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

  test('nodeA unsubscribes and resubscribes to topic2', async () => {
    nodeA.unsubscribe(topic2, callback2);
    await messageTimeout();
    nodeB.sendToPipeline(topic2, message2);
    nodeC.sendToPipeline(topic2, message2);
    await messageTimeout();
    expect(callback2).toHaveBeenCalledTimes(2);
    nodeA.subscribe(topic2, callback2);
    await messageTimeout();
    nodeB.sendToPipeline(topic2, message2);
    nodeC.sendToPipeline(topic2, message2);
    await messageTimeout();
    expect(callback2).toHaveBeenCalledTimes(4);
    nodeA.unsubscribe(topic2, callback2);
  });

  test('nodeA stops consuming pipeline', async () => {
    nodeA.stopConsumingPipeline(topic2);
    await messageTimeout();
    nodeB.sendToPipeline(topic2, message2);
    nodeC.sendToPipeline(topic2, message2);
    await messageTimeout();
    expect(callback2).toHaveBeenCalledTimes(4);
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

  test('nodeD stops consuming pipeline then sends a message on topic3', async () => {
    nodeD.stopConsumingPipeline(topic3);
    await messageTimeout();
    nodeD.sendToPipeline(topic3, message4);
    await messageTimeout();
    expect(callback3).toHaveBeenCalledTimes(1);
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


  test('Pipeline nodes choose a leader', async () => {
    const leaderCallback1 = jest.fn();
    const leaderCallback2 = jest.fn();
    const leaderCallback3 = jest.fn();
    nodeA.subscribe(topicLeader, leaderCallback1);
    nodeB.subscribe(topicLeader, leaderCallback2);
    nodeC.subscribe(topicLeader, leaderCallback3);
    nodeA.providePipeline(topicLeader);
    nodeB.providePipeline(topicLeader);
    nodeC.providePipeline(topicLeader);

    await messageTimeout();
    nodeA.consumePipeline(NANOMSG_TOPIC_LEADER_PORT_A, topicLeader);
    nodeB.consumePipeline(NANOMSG_TOPIC_LEADER_PORT_B, topicLeader);
    nodeC.consumePipeline(NANOMSG_TOPIC_LEADER_PORT_C, topicLeader);

    await messageTimeout();

    const peers = [nameA, nameB, nameC];
    peers.sort();
    let hasLeader = false;
    if (nodeA.isPipelineLeader(topicLeader)) {
      expect(peers[0]).toEqual(nameA);
      hasLeader = true;
    } else {
      expect(peers[0]).not.toEqual(nameA);
    }
    if (nodeB.isPipelineLeader(topicLeader)) {
      expect(peers[0]).toEqual(nameB);
      hasLeader = true;
    } else {
      expect(peers[0]).not.toEqual(nameB);
    }
    if (nodeC.isPipelineLeader(topicLeader)) {
      expect(peers[0]).toEqual(nameC);
      hasLeader = true;
    } else {
      expect(peers[0]).not.toEqual(nameC);
    }
    expect(hasLeader).toEqual(true);
    nodeA.unsubscribe(topicLeader, leaderCallback1);
    nodeB.unsubscribe(topicLeader, leaderCallback2);
    nodeC.unsubscribe(topicLeader, leaderCallback3);
  });

  test('nodeB and nodeC stop consuming topic 1.', async () => {
    const disconnectPipelineConsumerPromise = new Promise((resolve) => {
      nodeA.on('disconnectPipelineConsumer', (topic, name) => {
        if (topic === topic1 && name === nodeB.name) {
          resolve();
        }
      });
    });
    nodeB.stopConsumingPipeline(topic1);
    nodeC.stopConsumingPipeline(topic1);
    await messageTimeout();
    expect(nodeA.hasPipelineConsumer(topic1)).toEqual(false);
    expect(nodeA.pipelineConsumers(topic1)).toEqual([]);
    await disconnectPipelineConsumerPromise;
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
    nodeA.throwOnLeakedReferences();
  });

  test('nodeA, nodeC, nodeD, and nodeE close gracefuly.', async () => {
    nodeB.unsubscribe(topic1, callback1);
    nodeC.unsubscribe(topic1, callback1);
    nodeD.unsubscribe(topic3, callback3);
    nodeE.unsubscribe(topic4, callback4);
    await nodeB.close();
    await nodeC.close();
    await nodeD.close();
    await nodeE.close();
    nodeB.throwOnLeakedReferences();
    nodeC.throwOnLeakedReferences();
    nodeD.throwOnLeakedReferences();
    nodeE.throwOnLeakedReferences();
  });
});
