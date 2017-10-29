// @flow


const nano = require('nanomsg');
const events = require('events');
const { merge } = require('lodash');

export type SocketSettings = {
  host: string,
  pubsubPort?: number,
  pipelinePort?: number
};

type BindSocket = {
  on: Function,
  bind: Function,
  send: Function,
  close: Function,
  bound: {[string]: number},
  removeListener: Function
};

type ConnectSocket = {
  on: Function,
  connect: Function,
  close: Function,
  send: Function,
  connected: {[string]: number},
  removeListener: Function
};

type Options = {
  cluster?: {
    bindAddress?: SocketSettings,
    peerAddresses?: Array<SocketSettings>,
  },
  name: string
};

const DEFAULT_PUBSUB_PORT = 13001;
const DEFAULT_PIPELINE_PORT = 13002;

const getSocketHash = (name:string, socketSettings: SocketSettings):string => `${name}/${socketSettings.host}/${socketSettings.pubsubPort || DEFAULT_PUBSUB_PORT}/${socketSettings.pipelinePort || DEFAULT_PIPELINE_PORT}`;

const getSocketSettings = (hash:string):SocketSettings => {
  const [host, pubsubPort, pipelinePort] = hash.split('/').slice(1);
  return {
    host,
    pubsubPort: parseInt(pubsubPort, 10),
    pipelinePort: parseInt(pipelinePort, 10),
  };
};

class ClusterNode extends events.EventEmitter {
  isReady: boolean;
  options: Options;
  pullSocket: BindSocket;
  pushSockets: {[string]:ConnectSocket};
  pubSocket: BindSocket;
  subSockets: {[string]:ConnectSocket};
  name: string;
  subscriptions: {[string]:Function};
  boundReceiveMessage: Function;
  socketHash: string;
  peerSocketHashes: {[string]:boolean};
  namedPushSockets: {[string]:ConnectSocket};
  closed: boolean;
  clusterUpdateTimeout: number;

  constructor(options:Options) {
    super();
    this.options = options;
    this.isReady = false;
    this.subscriptions = {};
    this.name = options.name;
    this.boundReceiveMessage = this.receiveMessage.bind(this);
    const clusterOptions = merge({
      bindAddress: {
        host: '127.0.0.1',
        pubsubPort: DEFAULT_PUBSUB_PORT,
        pipelinePort: DEFAULT_PIPELINE_PORT,
      },
      peerAddresses: [],
    }, options.cluster);
    // String version of this node address.
    this.socketHash = getSocketHash(options.name, clusterOptions.bindAddress);
    // String versions of peer addresses.
    this.peerSocketHashes = {};
    // Bind a nanomsg pull socket for incoming direct messages
    // http://nanomsg.org/v0.1/nn_pipeline.7.html
    const pullBindAddress = `tcp://${clusterOptions.bindAddress.host}:${clusterOptions.bindAddress.pipelinePort}`;
    this.pullSocket = nano.socket('pull');
    this.pullSocket.bind(pullBindAddress);
    this.pullSocket.on('error', function (error) {
      this.emit('error', `Nanomsg pull socket "${pullBindAddress}": ${error.message}`);
    });
    this.pullSocket.on('data', this.boundReceiveMessage);
    if (this.pullSocket.bound[pullBindAddress] <= -1) {
      this.emit('error', `Nanomsg: Could not bind pull socket to ${pullBindAddress}`);
    }
    // Bind a Nanomsg pub socket for outgoing messages to all nodes
    // http://nanomsg.org/v0.5/nn_pubsub.7.html
    const pubsubBindAddress = `tcp://${clusterOptions.bindAddress.host}:${clusterOptions.bindAddress.pubsubPort}`;
    this.pubSocket = nano.socket('pub');
    this.pubSocket.bind(pubsubBindAddress);
    this.pubSocket.on('error', function (error) {
      this.emit('error', `Nanomsg pub socket: ${error.message}`);
    });
    if (this.pubSocket.bound[pubsubBindAddress] <= -1) {
      this.emit('error', `Nanomsg: Could not bind pub socket to ${pubsubBindAddress}`);
    }
    // Nanomsg sub sockets for incoming messages from all nodes
    // http://nanomsg.org/v1.0.0/nn_pubsub.7.html
    // Socket object is keyed to the connection string, 
    // i.e.: this.subSockets['tcp://127.0.0.1:DEFAULT_PUBSUB_PORT'] = nano.socket('sub')
    this.subSockets = {};
    // Nanomsg push sockets for outgoing direct messages
    // http://nanomsg.org/v1.0.0/nn_pipeline.7.html
    // Socket object is keyed to the connection string, 
    // i.e.: this.subSockets['tcp://127.0.0.1:DEFAULT_PIPELINE_PORT'] = nano.socket('push')
    this.pushSockets = {};
    // Socket object is keyed to the server name,
    // i.e.: this.subSockets[name] = nano.socket('push')
    this.namedPushSockets = {};
    // Messaging about peers
    this.subscribe('_clusterAddPeers', (message) => {
      const peerSocketHashes = [message.socketHash].concat(message.peerSocketHashes.filter((peerSocketHash) => this.socketHash !== peerSocketHash));
      peerSocketHashes.forEach((socketHash) => {
        const name = socketHash.split('/').shift();
        this.peerSocketHashes[socketHash] = true;
        this.namedPushSockets[name] = this.addPeer(getSocketSettings(socketHash));
      });
    });
    this.subscribe('_clusterRemovePeer', (message) => {
      this.removePeer(getSocketSettings(message.socketHash));
    });
    // Connect to peers included in the options
    clusterOptions.peerAddresses.forEach(this.addPeer.bind(this));
    setImmediate(() => {
      this.isReady = true;
      this.emit('ready');
    });
    setTimeout(() => {
      this.send('_clusterRequestState', {
        name: this.name,
      });
    }, 100);
  }

  receiveMessage(buffer:Buffer):void {
    const [topic, message, name] = JSON.parse(String(buffer));
    if (!this.subscriptions[topic]) {
      return;
    }
    this.subscriptions[topic].forEach((callback) => {
      callback(message, name);
    });
  }

  sendDirect(name:string, topic:string, message:any):void {
    const push = this.namedPushSockets[name];
    if (!push) {
      this.emit('error', `${this.name} is unable to send message "${topic}":"${JSON.stringify(message)}" to "${name}"`);
      return;
    }
    push.send(JSON.stringify([topic, message, this.name]));
  }

  send(topic:string, message:any):void {
    this.pubSocket.send(JSON.stringify([topic, message, this.name]));
  }

  subscribe(topic:string, callback:Function):void {
    this.subscriptions[topic] = this.subscriptions[topic] || [];
    this.subscriptions[topic].push(callback);
  }

  async close(callback?:Function):Promise<void> {
    if (this.closed) {
      throw new Error('ClusterNode already closed.');
    }
    if (this.clusterUpdateTimeout) {
      clearTimeout(this.clusterUpdateTimeout);
    }
    this.send('_clusterRemovePeer', {
      socketHash: this.socketHash,
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    this.pullSocket.removeListener('data', this.boundReceiveMessage);
    await new Promise((resolve) => {
      this.pullSocket.on('close', resolve);
      this.pullSocket.close();
    });
    await new Promise((resolve) => {
      this.pubSocket.on('close', resolve);
      this.pubSocket.close();
    });
    await Promise.all(Object.keys(this.subSockets).map(this.closeSubConnectSocket.bind(this)));
    await Promise.all(Object.keys(this.pushSockets).map(this.closePushConnectSocket.bind(this)));
    this.closed = true;
    this.emit('close');
    if (callback) {
      callback();
    }
  }

  addPeer(peerAddress: SocketSettings):ConnectSocket {
    const pubsubConnectAddress = `tcp://${peerAddress.host}:${peerAddress.pubsubPort || DEFAULT_PUBSUB_PORT}`;
    const pushConnectAddress = `tcp://${peerAddress.host}:${peerAddress.pipelinePort || DEFAULT_PIPELINE_PORT}`;
    const newPeer = !this.subSockets[pubsubConnectAddress] || !this.pushSockets[pushConnectAddress];
    if (!newPeer) {
      return this.pushSockets[pushConnectAddress];
    }
    if (!this.subSockets[pubsubConnectAddress]) {
      const sub = nano.socket('sub');
      sub.on('error', function (error) {
        this.emit('error', `Nanomsg sub socket "${pubsubConnectAddress}": ${error.message}`);
      });
      sub.connect(pubsubConnectAddress);
      if (sub.connected[pubsubConnectAddress] <= -1) {
        throw new Error(`Could not connect sub socket to ${pubsubConnectAddress}`);
      }
      this.subSockets[pubsubConnectAddress] = sub;
      sub.on('data', this.boundReceiveMessage);
    }
    if (!this.pushSockets[pushConnectAddress]) {
      const push = nano.socket('push');
      push.on('error', function (error) {
        this.emit('error', `Nanomsg push socket "${pushConnectAddress}": ${error.message}`);
      });
      push.connect(pushConnectAddress);
      if (push.connected[pushConnectAddress] <= -1) {
        throw new Error(`Could not connect push socket to ${pushConnectAddress}`);
      }
      this.pushSockets[pushConnectAddress] = push;
      push.send(JSON.stringify(['_clusterAddPeers', {
        socketHash: this.socketHash,
        peerSocketHashes: Object.keys(this.peerSocketHashes),
      }]));
    }
    if (this.clusterUpdateTimeout) {
      clearTimeout(this.clusterUpdateTimeout);
    }
    this.clusterUpdateTimeout = setTimeout(() => {
      this.send('_clusterAddPeers', {
        socketHash: this.socketHash,
        peerSocketHashes: Object.keys(this.peerSocketHashes),
      });
      delete this.clusterUpdateTimeout;
    }, 10);
    return this.pushSockets[pushConnectAddress];
  }

  async removePeer(peerAddress:SocketSettings):Promise<void> {
    const pubsubConnectAddress = `tcp://${peerAddress.host}:${peerAddress.pubsubPort || DEFAULT_PUBSUB_PORT}`;
    const pushConnectAddress = `tcp://${peerAddress.host}:${peerAddress.pipelinePort || DEFAULT_PIPELINE_PORT}`;
    const peerExists = this.subSockets[pubsubConnectAddress] || this.pushSockets[pushConnectAddress];
    if (!peerExists) {
      return;
    }
    Object.keys(this.namedPushSockets).forEach((name) => {
      if (this.namedPushSockets[name] === this.pushSockets[pushConnectAddress]) {
        delete this.namedPushSockets[name];
        const socketHash = getSocketHash(name, peerAddress);
        delete this.peerSocketHashes[socketHash];
        this.send('_clusterRemovePeer', {
          socketHash,
        });
      }
    });
    await Promise.all([
      this.closeSubConnectSocket(pubsubConnectAddress),
      this.closePushConnectSocket(pushConnectAddress),
    ]);
  }

  async closeSubConnectSocket(address:string):Promise<void> {
    const sub = this.subSockets[address];
    delete this.subSockets[address];
    sub.removeListener('data', this.boundReceiveMessage);
    await new Promise((resolve, reject) => {
      sub.on('close', resolve);
      sub.on('error', reject);
      sub.close();
    });
  }

  async closePushConnectSocket(address:string):Promise<void> {
    const push = this.pushSockets[address];
    delete this.pushSockets[address];
    await new Promise((resolve, reject) => {
      push.on('close', resolve);
      push.on('error', reject);
      push.close();
    });
  }

  getPeers(): Array<SocketSettings & {name: string}> {
    return Object.keys(this.peerSocketHashes).map((socketHash) => Object.assign({}, {
      name: socketHash.split('/').shift(),
    }, getSocketSettings(socketHash)));
  }
}

module.exports = ClusterNode;
