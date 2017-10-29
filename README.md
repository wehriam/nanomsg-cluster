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

const server = new ClusterNode({
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

```js
server.addPeer({
  host: '192.168.1.2', // Required
  pubsubPort: 13001, // Optional, default 13001
  pipelinePort: 13002, // Optional, default 13002
});
```

```js
// Returns a Promise.

server.removePeer({
  host: '192.168.1.2', // Required
  pubsubPort: 13001, // Optional, default 13001
  pipelinePort: 13002, // Optional, default 13002
});
```

```js
server.getPeers();

// Returns:
//
// [
//   {
//     serverName: "server-2",
//     host: '192.168.1.2',
//     pubsubPort: 13001,
//     pipelinePort: 13002,
//   },
//   {
//     serverName: "server-3",
//     host: '192.168.1.3',
//     pubsubPort: 13001,
//     pipelinePort: 13002,
//   },
// ]
```


