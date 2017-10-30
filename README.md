## Nanomsg Cluster
Nanomsg based clustering.

[![CircleCI](https://circleci.com/gh/wehriam/nanomsg-cluster.svg?style=svg)](https://circleci.com/gh/wehriam/nanomsg-cluster) [![npm version](https://badge.fury.io/js/nanomsg-cluster.svg)](http://badge.fury.io/js/nanomsg-cluster)

## Usage

```sh
yarn install nanomsg-cluster
```

Peers bootstrap off of each other.

### Server A, 192.168.1.1

```js
const ClusterNode = require('nanomsg-cluster');

const node = new ClusterNode();
```

### Server B, 192.168.1.2

```js
const ClusterNode = require('nanomsg-cluster');

const node = new ClusterNode({
  peerAddresses: [
    {
      host: '192.168.1.1'
    }
  ]
});
```

### Server C, 192.168.1.3

```js
const ClusterNode = require('nanomsg-cluster');

const node = new ClusterNode();

node.addPeer({host: '192.168.1.1'});
```

## Options

```js
const ClusterNode = require('nanomsg-cluster');

const node = new ClusterNode({
  bindAddress: {
    host: '127.0.0.1', // Optional, default '127.0.0.1'
    pubsubPort: 13001, // Optional, default 13001
    pipelinePort: 13002, // Optional, default 13002
  },
  peerAddresses: [
    {
      host: '127.0.0.1', // Required
      pubsubPort: 13021, // Optional, default 13001
      pipelinePort: 13022, // Optional, default 13002
    }
  ]
});
```

## Methods

Subscribe to a topic:

```js
const topic = "example";
const callback = (message, sender) => {
  console.log("Message", message);
  console.log("Sender", sender);
}

node.subscribe(topic, callback);
```

Broadcast to all nodes:

```js
const topic = "example";
const message = {foo: "bar"};

node.sendToAll(topic, message);
```

Send to a specific node:

```js
const topic = "example";
const message = {foo: "bar"};
const name = "node-2";

node.sendToPeer(name, topic, message);
```

Send to a pipeline:

```js
const topic = "pipeline example";

node.providePipeline(topic);

// later

const message = {foo: "bar"};
node.sendToPipeline(topic, message);
```

Subscribe to a pipeline:

```js
const topic = "pipeline example";
const pipelineSubscriptionPort = 14000;

node.consumePipeline(pipelineSubscriptionPort, topic);

// later

const callback = (message, sender) => {
  console.log("Message", message);
  console.log("Sender", sender);
}
node.subscribe(topic, callback);
```

Add a peer:

```js
node.addPeer({
  host: '192.168.1.2', // Required
  pubsubPort: 13001, // Optional, default 13001
  pipelinePort: 13002, // Optional, default 13002
});
```

Remove a peer:

```js
// Returns a Promise.
node.removePeer({
  host: '192.168.1.2', // Required
  pubsubPort: 13001, // Optional, default 13001
  pipelinePort: 13002, // Optional, default 13002
});
```

Get a list of peers:

```js
node.getPeers();

// Returns:
//
// [
//   {
//     name: "node-2",
//     host: '192.168.1.2',
//     pubsubPort: 13001,
//     pipelinePort: 13002,
//   },
//   {
//     name: "node-3",
//     host: '192.168.1.3',
//     pubsubPort: 13001,
//     pipelinePort: 13002,
//   },
// ]
```


