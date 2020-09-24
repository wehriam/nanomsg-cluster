//      

const uuid = require('uuid');
const nano = require('nanomsg');
const events = require('events');
const { merge, without } = require('lodash');
const Discover = require('node-discover');
const { encode, decode } = require('@msgpack/msgpack');

                              
                
               
                      
                       
  

                   
               
                 
                 
                  
                  
                            
                           
                    
  

                      
               
                    
                  
                  
                 
                                
                           
                    
  

                
                               
                                        
                
                            
  

const DEFAULT_PUBSUB_PORT = 13001;
const DEFAULT_PIPELINE_PORT = 13002;

const socketOptions = { sndbuf: 4194304, dontwait: true, rcvbuf: 4194304, rcvmaxsize: 4194304 };

const getSocketHash = (name       , socketSettings                )        => `${name}/${socketSettings.host}/${socketSettings.pubsubPort || DEFAULT_PUBSUB_PORT}/${socketSettings.pipelinePort || DEFAULT_PIPELINE_PORT}`;

const getSocketSettings = (hash       )                => {
  const [name, host, pubsubPort, pipelinePort] = hash.split('/');
  return {
    name,
    host,
    pubsubPort: parseInt(pubsubPort, 10),
    pipelinePort: parseInt(pipelinePort, 10),
  };
};

const getUnnamedSocketHash = (socketSettings                )        => `${socketSettings.host}/${socketSettings.pubsubPort || DEFAULT_PUBSUB_PORT}/${socketSettings.pipelinePort || DEFAULT_PIPELINE_PORT}`;

const getUnnamedSocketSettings = (hash       )                => {
  const [host, pubsubPort, pipelinePort] = hash.split('/');
  return {
    host,
    pubsubPort: parseInt(pubsubPort, 10),
    pipelinePort: parseInt(pipelinePort, 10),
  };
};

class ClusterNode extends events.EventEmitter {
                           
                           
                                 
                                                
                                
                                               
                                                        
                                                             
                                                     
                                                     
                       
                                                    
                                                         
                                        
                             
                                               
                                                      
                                                  
                                                     
                          
                                                 
                                                      
                            
                                    
                                             
                                                  

  constructor(options          = {}) {
    super();
    this.options = options;
    this.isReady = false;
    this.subscriptions = {};
    this.localSubscriptions = {};
    this.name = options.name || uuid.v4();
    this.boundReceiveMessage = this.receiveMessage.bind(this);
    const clusterOptions = merge({
      bindAddress: {
        host: '0.0.0.0',
        pubsubPort: DEFAULT_PUBSUB_PORT,
        pipelinePort: DEFAULT_PIPELINE_PORT,
      },
      peerAddresses: [],
    }, options);
    // String version of this node address.
    this.socketHash = getSocketHash(this.name, clusterOptions.bindAddress);
    // String versions of peer addresses.
    this.peerSocketHashes = {};
    // String versions of peer addresses before names are received.
    this.unnamedPeerSocketHashes = {};
    // Bind a nanomsg pull socket for incoming direct messages
    // http://nanomsg.org/v1.0.0/nn_pipeline.7.html
    const pullBindAddress = `tcp://${clusterOptions.bindAddress.host}:${clusterOptions.bindAddress.pipelinePort}`;
    this.pullSocket = nano.socket('pull', socketOptions);
    try {
      this.pullSocket.bind(pullBindAddress);
    } catch (error) {
      error.message = `Could not bind pull socket to ${pullBindAddress}: ${error.message}`;
      throw error;
    }
    this.pullSocket.on('error', (error) => {
      this.emit('error', `Pull socket "${pullBindAddress}": ${error.message}`);
    });
    this.pullSocket.on('data', this.boundReceiveMessage);
    if (this.pullSocket.bound[pullBindAddress] <= -1) {
      this.emit('error', `Could not bind pull socket to ${pullBindAddress}`);
    }
    // Bind a Nanomsg pub socket for outgoing messages to all nodes
    // http://nanomsg.org/v1.0.0/nn_pubsub.7.html
    const pubsubBindAddress = `tcp://${clusterOptions.bindAddress.host}:${clusterOptions.bindAddress.pubsubPort}`;
    this.pubSocket = nano.socket('pub', socketOptions);
    try {
      this.pubSocket.bind(pubsubBindAddress);
    } catch (error) {
      error.message = `Could not bind pub socket to ${pubsubBindAddress}: ${error.message}`;
      throw error;
    }
    this.pubSocket.on('error', (error) => {
      this.emit('error', `Pub socket: ${error.message}`);
    });
    if (this.pubSocket.bound[pubsubBindAddress] <= -1) {
      this.emit('error', `Could not bind pub socket to ${pubsubBindAddress}`);
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
    // Nanomsg push and pull sockets for pipeline topics
    // http://nanomsg.org/v1.0.0/nn_pipeline.7.html
    // Socket object is keyed to the topic
    // i.e.: this.pipelinePushSockets['topic'] = nano.socket('push')
    // i.e.: this.namedPipelinePushSockets['topic' + name] = this.pipelinePushSockets['topic']
    // i.e.: this.pipelinePullSockets['topic'] = nano.socket('pull')
    this.pipelinePushSockets = {};
    this.namedPipelinePushSockets = {};
    this.pipelinePullSockets = {};
    this.pipelinePullBindAddress = {};
    // Messaging about peers
    this.subscribe('_clusterAddPeers', (message       ) => {
      const peerSocketHashes = [message.socketHash].concat(message.peerSocketHashes.filter((peerSocketHash) => this.socketHash !== peerSocketHash));
      peerSocketHashes.forEach((socketHash) => {
        const socketSettings = getSocketSettings(socketHash);
        if (!this.peerSocketHashes[socketHash]) {
          const pushConnectAddress = `tcp://${socketSettings.host}:${socketSettings.pipelinePort || DEFAULT_PIPELINE_PORT}`;
          const push = this.pushSockets[pushConnectAddress];
          if (push) {
            push.send(encode(['_clusterAddPeers', {
              socketHash: this.socketHash,
              peerSocketHashes: Object.keys(this.peerSocketHashes),
            }]));
          }
          this.advertisePipelines();
        }
        const name = socketHash.split('/').shift();
        this.peerSocketHashes[socketHash] = true;
        this.peerSocketHeartbeats[socketHash] = Date.now();
        if (!this.namedPushSockets[name]) {
          this.emit('addPeer', socketSettings);
        }
        this.namedPushSockets[name] = this.addPeer(socketSettings);
        delete this.unnamedPeerSocketHashes[getUnnamedSocketHash(socketSettings)];
      });
    });
    this.subscribe('_clusterRemovePeer', (message       , name       ) => {
      const socketSettings = getSocketSettings(this.socketHash);
      if (name !== this.name && socketSettings.host === message.peerAddress.host && socketSettings.pubsubPort === message.peerAddress.pubsubPort && socketSettings.pipelinePort === message.peerAddress.pipelinePort) {
        if (message.socketHash) {
          const peerSocketSettings = getSocketSettings(message.socketHash);
          this.removePeer(peerSocketSettings, false);
        }
        return;
      }
      this.removePeer(message.peerAddress, false);
    });
    this.subscribe('_clusterRemoveHost', (message       ) => {
      this.removeHost(message.host, false);
    });
    this.subscribe('_clusterAddPipelineConsumer', (message       , name       ) => {
      const { topic, pushConnectAddress } = message;
      this.connectPipelineConsumer(topic, pushConnectAddress, name);
    });
    this.subscribe('_clusterRemovePipelineConsumer', (message       , name       ) => {
      const { topic } = message;
      this.disconnectPipelineConsumer(topic, name);
    });
    this.subscribe('_clusterAddPipelineProvider', (message       ) => {
      const { topic } = message;
      const pushConnectAddress = this.pipelinePullBindAddress[topic];
      if (pushConnectAddress) {
        this.sendToAll('_clusterAddPipelineConsumer', {
          topic,
          pushConnectAddress,
        });
      }
    });
    this.heartbeatInterval = options.heartbeatInterval || 5000;
    this.peerSocketHeartbeats = {};
    this.subscribe('_clusterHeartbeat', (message       ) => {
      const { socketHash } = message;
      if (typeof socketHash !== 'string') {
        this.emit('error', 'Received unknown socket hash from cluster heartbeat');
        return;
      }
      this.peerSocketHeartbeats[socketHash] = Date.now();
    });
    this.startHeartbeat();
    // Connect to peers included in the options
    clusterOptions.peerAddresses.forEach(this.addPeer.bind(this));
    setImmediate(() => {
      this.isReady = true;
      this.emit('ready');
    });
    this.advertisePeersInterval = setInterval(this.advertisePeers.bind(this), 60000);
  }

  startHeartbeat()      {
    this.clusterHeartbeatInterval = setInterval(() => {
      this.sendToAll('_clusterHeartbeat', {
        socketHash: this.socketHash,
      });
      Object.keys(this.peerSocketHeartbeats).forEach((socketHash) => {
        if (this.peerSocketHeartbeats[socketHash] + this.heartbeatInterval * 2.5 > Date.now()) {
          return;
        }
        delete this.peerSocketHeartbeats[socketHash];
        this.removePeer(getSocketSettings(socketHash));
      });
    }, this.heartbeatInterval);
  }

  stopHeartbeat()      {
    clearInterval(this.clusterHeartbeatInterval);
  }

  receiveMessage(buffer       )      {
    const [topic, message, name] = decode(buffer);
    if (!this.subscriptions[topic]) {
      return;
    }
    this.subscriptions[topic].forEach((callback) => {
      callback(message, name);
    });
  }

  sendToPeer(name       , topic       , message    )      {
    const push = this.namedPushSockets[name];
    if (!push) {
      this.emit('error', `${this.name} is unable to send message "${topic}":"${JSON.stringify(message)}" to "${name}"`);
      return;
    }
    push.send(encode([topic, message, this.name]));
  }

  sendToAll(topic       , message    )      {
    this.pubSocket.send(encode([topic, message, this.name]));
    if (!this.localSubscriptions[topic]) {
      return;
    }
    this.localSubscriptions[topic].forEach((callback) => {
      callback(message, this.name);
    });
  }

  sendToPipeline(topic       , message    )      {
    const push = this.pipelinePushSockets[topic];
    if (!push) {
      throw new Error(`Not providing pipeline "${topic}"`);
    }
    push.send(encode([topic, message, this.name]));
  }

  subscribe(topic       , callback         , includeLocal         )      {
    this.subscriptions[topic] = this.subscriptions[topic] || [];
    this.subscriptions[topic].push(callback);
    if (includeLocal) {
      this.localSubscriptions[topic] = this.localSubscriptions[topic] || [];
      this.localSubscriptions[topic].push(callback);
    }
  }

  unsubscribe(topic       , callback          )      {
    this.subscriptions[topic] = this.subscriptions[topic] || [];
    this.localSubscriptions[topic] = this.localSubscriptions[topic] || [];
    if (callback) {
      this.subscriptions[topic] = this.subscriptions[topic].filter((cb) => cb !== callback);
      this.localSubscriptions[topic] = this.localSubscriptions[topic].filter((cb) => cb !== callback);
      if (this.subscriptions[topic].length === 0) {
        delete this.subscriptions[topic];
      }
      if (this.localSubscriptions[topic].length === 0) {
        delete this.localSubscriptions[topic];
      }
      return;
    }
    delete this.subscriptions[topic];
    delete this.localSubscriptions[topic];
  }

  // Used for simulating broken closes in test scenarios
  async dirtyClose()               {
    this.stopDiscovery();
    await Promise.all(Object.keys(this.pipelinePullSockets).map(this.closePipelinePullSocket.bind(this)));
    await Promise.all(Object.keys(this.pipelinePushSockets).map(this.closePipelinePushSocket.bind(this)));
    await new Promise((resolve) => {
      if (this.pubSocket.closed) {
        resolve();
      }
      this.pullSocket.on('close', resolve);
      this.pullSocket.close();
    });
    await new Promise((resolve) => {
      if (this.pubSocket.closed) {
        resolve();
      }
      this.pubSocket.on('close', resolve);
      this.pubSocket.close();
    });
  }

  async close()               {
    if (this.closed) {
      throw new Error('Already closed.');
    }
    this.stopDiscovery();
    if (this.advertisePeersTimeout) {
      clearTimeout(this.advertisePeersTimeout);
    }
    clearInterval(this.advertisePeersInterval);
    if (this.clusterUpdateTimeout) {
      clearTimeout(this.clusterUpdateTimeout);
    }
    if (this.clusterHeartbeatInterval) {
      clearInterval(this.clusterHeartbeatInterval);
    }
    await Promise.all(Object.keys(this.pipelinePullSockets).map(this.stopConsumingPipeline.bind(this)));
    await Promise.all(Object.keys(this.pipelinePushSockets).map(this.stopProvidingPipeline.bind(this)));
    this.sendToAll('_clusterRemovePeer', {
      socketHash: this.socketHash,
      peerAddress: getSocketSettings(this.socketHash),
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    const closePromises = [];
    for (const socketSettings of this.getPeers()) {
      closePromises.push(new Promise((resolve) => {
        const timeout = setTimeout(() => {
          this.removeListener('removePeer', handleRemovePeer);
          resolve();
        }, 1000);
        const handleRemovePeer = (sSettings) => {
          if (socketSettings.host === sSettings.host && socketSettings.pubsubPort === sSettings.pubsubPort && socketSettings.pipelinePort === sSettings.pipelinePort) {
            clearTimeout(timeout);
            this.removeListener('removePeer', handleRemovePeer);
            resolve();
          }
        };
        this.on('removePeer', handleRemovePeer);
      }));
      this.removePeer(socketSettings, false);
    }
    await Promise.all(closePromises);
    await new Promise((resolve) => {
      if (this.pubSocket.closed) {
        resolve();
      }
      this.pullSocket.on('close', resolve);
      this.pullSocket.close();
    });
    await new Promise((resolve) => {
      if (this.pubSocket.closed) {
        resolve();
      }
      this.pubSocket.on('close', resolve);
      this.pubSocket.close();
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    this.unsubscribe('_clusterAddPeers');
    this.unsubscribe('_clusterRemovePeer');
    this.unsubscribe('_clusterRemoveHost');
    this.unsubscribe('_clusterAddPipelineConsumer');
    this.unsubscribe('_clusterRemovePipelineConsumer');
    this.unsubscribe('_clusterAddPipelineProvider');
    this.unsubscribe('_clusterHeartbeat');
    this.unsubscribe('_clusterAddPeers');
    this.closed = true;
    this.emit('close');
  }

  addPeer(peerAddress                )               {
    const pubsubConnectAddress = `tcp://${peerAddress.host}:${peerAddress.pubsubPort || DEFAULT_PUBSUB_PORT}`;
    const pushConnectAddress = `tcp://${peerAddress.host}:${peerAddress.pipelinePort || DEFAULT_PIPELINE_PORT}`;
    const newPeer = !this.subSockets[pubsubConnectAddress] || !this.pushSockets[pushConnectAddress];
    if (!newPeer) {
      return this.pushSockets[pushConnectAddress];
    }
    if (!this.subSockets[pubsubConnectAddress]) {
      this.unnamedPeerSocketHashes[getUnnamedSocketHash(peerAddress)] = true;
      const sub = nano.socket('sub', socketOptions);
      sub.on('error', (error) => {
        this.emit('error', `Sub socket "${pubsubConnectAddress}": ${error.message}`);
      });
      try {
        sub.connect(pubsubConnectAddress);
      } catch (error) {
        error.message = `Could not connect sub socket to ${pubsubConnectAddress}: ${error.message}`;
        throw error;
      }
      if (sub.connected[pubsubConnectAddress] <= -1) {
        throw new Error(`Could not connect sub socket to ${pubsubConnectAddress}`);
      }
      this.subSockets[pubsubConnectAddress] = sub;
      sub.on('data', this.boundReceiveMessage);
    }
    if (!this.pushSockets[pushConnectAddress]) {
      this.unnamedPeerSocketHashes[getUnnamedSocketHash(peerAddress)] = true;
      const push = nano.socket('push', socketOptions);
      push.on('error', (error) => {
        this.emit('error', `Push socket "${pushConnectAddress}": ${error.message}`);
      });
      try {
        push.connect(pushConnectAddress);
      } catch (error) {
        error.message = `Could not connect push socket to ${pushConnectAddress}: ${error.message}`;
        throw error;
      }
      if (push.connected[pushConnectAddress] <= -1) {
        throw new Error(`Could not connect push socket to ${pushConnectAddress}`);
      }
      this.pushSockets[pushConnectAddress] = push;
      push.send(encode(['_clusterAddPeers', {
        socketHash: this.socketHash,
        peerSocketHashes: Object.keys(this.peerSocketHashes),
      }]));
    }
    if (this.clusterUpdateTimeout) {
      clearTimeout(this.clusterUpdateTimeout);
    }
    this.clusterUpdateTimeout = setTimeout(() => {
      this.sendToAll('_clusterAddPeers', {
        socketHash: this.socketHash,
        peerSocketHashes: Object.keys(this.peerSocketHashes),
      });
      delete this.clusterUpdateTimeout;
      this.advertisePipelines();
    }, 100);
    clearTimeout(this.advertisePeersTimeout);
    this.advertisePeersTimeout = setTimeout(this.advertisePeers.bind(this), 5000);
    return this.pushSockets[pushConnectAddress];
  }

  async advertisePeers()               {
    this.sendToAll('_clusterAddPeers', {
      socketHash: this.socketHash,
      peerSocketHashes: Object.keys(this.peerSocketHashes),
    });
    await new Promise((resolve) => setTimeout(resolve, 1000));
    this.advertisePipelines();
  }

  async removeHost(host       , sendToAll           = true)               {
    if (sendToAll) {
      this.sendToAll('_clusterRemoveHost', {
        host,
      });
    }
    const socketSettings = getSocketSettings(this.socketHash);
    // This peer's host is being removed
    if (socketSettings.host === host) {
      for (const peerAddress of this.getPeers()) {
        if (peerAddress.host === host) {
          continue;
        }
        await this._removePeer(peerAddress); // eslint-disable-line no-underscore-dangle
      }
      return;
    }
    // A different host is being removed
    for (const peerAddress of this.getPeers()) {
      if (peerAddress.host !== host) {
        continue;
      }
      await this._removePeer(peerAddress); // eslint-disable-line no-underscore-dangle
    }
  }

  async removePeer(peerAddress               , sendToAll           = true)               {
    if (sendToAll) {
      this.sendToAll('_clusterRemovePeer', {
        socketHash: this.socketHash,
        peerAddress,
      });
    }
    await this._removePeer(peerAddress); // eslint-disable-line no-underscore-dangle
  }

  async _removePeer(peerAddress               )               {
    const removedPeers = [];
    const { name } = peerAddress;
    if (name) {
      delete this.namedPushSockets[name];
      const socketHash = getSocketHash(name, peerAddress);
      delete this.peerSocketHeartbeats[socketHash];
      delete this.peerSocketHashes[socketHash];
      if (this.namedPipelinePushSockets[name]) {
        Object.keys(this.namedPipelinePushSockets[name]).forEach((topic       ) => {
          this.disconnectPipelineConsumer(topic, name);
        });
      }
      removedPeers.push(Object.assign({}, { name }, peerAddress));
    } else {
      for (const socketHash of Object.keys(this.peerSocketHashes)) {
        const socketSettings = getSocketSettings(socketHash);
        const socketName = socketSettings.name;
        if (typeof socketName === 'string' && socketSettings.host === peerAddress.host && socketSettings.pubsubPort === peerAddress.pubsubPort && socketSettings.pipelinePort === peerAddress.pipelinePort) {
          delete this.namedPushSockets[socketName];
          delete this.peerSocketHeartbeats[socketHash];
          delete this.peerSocketHashes[socketHash];
          if (this.namedPipelinePushSockets[socketName]) {
            Object.keys(this.namedPipelinePushSockets[socketName]).forEach((topic       ) => {
              this.disconnectPipelineConsumer(topic, socketName);
            });
          }
          removedPeers.push(Object.assign({}, { name: socketName }, peerAddress));
        }
      }
    }
    const pubsubConnectAddress = `tcp://${peerAddress.host}:${peerAddress.pubsubPort || DEFAULT_PUBSUB_PORT}`;
    const pushConnectAddress = `tcp://${peerAddress.host}:${peerAddress.pipelinePort || DEFAULT_PIPELINE_PORT}`;
    const peerExists = this.subSockets[pubsubConnectAddress] || this.pushSockets[pushConnectAddress];
    if (peerExists && removedPeers.length === 0) {
      removedPeers.push(peerAddress);
    }
    delete this.unnamedPeerSocketHashes[getUnnamedSocketHash(peerAddress)];
    await Promise.all([
      this.closeSubSocket(pubsubConnectAddress),
      this.closePushSocket(pushConnectAddress),
    ]);
    for (const removedPeer of removedPeers) {
      this.emit('removePeer', removedPeer);
    }
    clearTimeout(this.advertisePeersTimeout);
    this.advertisePeersTimeout = setTimeout(this.advertisePeers.bind(this), 5000);
  }

  async closeSubSocket(address       )               {
    const sub = this.subSockets[address];
    delete this.subSockets[address];
    if (!sub) {
      return;
    }
    sub.removeListener('data', this.boundReceiveMessage);
    if (sub.closed) {
      return;
    }
    await new Promise((resolve, reject) => {
      sub.on('close', resolve);
      sub.on('error', reject);
      sub.close();
    });
  }

  async closePushSocket(address       )               {
    const push = this.pushSockets[address];
    delete this.pushSockets[address];
    if (!push) {
      return;
    }
    if (push.closed) {
      return;
    }
    await new Promise((resolve, reject) => {
      push.on('close', resolve);
      push.on('error', reject);
      push.close();
    });
  }

  shutdownPipelinePushAddress(topic       , address       )      {
    const push = this.pipelinePushSockets[topic];
    if (!push) {
      return;
    }
    if (typeof push.connected[address] === 'undefined') {
      return;
    }
    push.shutdown(address);
    if (typeof push.connected[address] !== 'undefined') {
      throw new Error(`Could not shutdown topic "${topic}" on ${address}`);
    }
  }

  async closePipelinePushSocket(topic       )               {
    const push = this.pipelinePushSockets[topic];
    delete this.pipelinePushSockets[topic];
    if (!push) {
      return;
    }
    if (push.closed) {
      return;
    }
    await new Promise((resolve, reject) => {
      push.on('close', resolve);
      push.on('error', reject);
      push.close();
    });
  }

  async closePipelinePullSocket(topic       )               {
    const pullSocket = this.pipelinePullSockets[topic];
    delete this.pipelinePullSockets[topic];
    delete this.pipelinePullBindAddress[topic];
    if (!pullSocket) {
      return;
    }
    pullSocket.removeListener('data', this.boundReceiveMessage);
    if (pullSocket.closed) {
      return;
    }
    await new Promise((resolve, reject) => {
      pullSocket.on('close', resolve);
      pullSocket.on('error', reject);
      pullSocket.close();
    });
  }

  disconnectPipelineConsumer(topic        , name        )       {
    if (!this.namedPipelinePushSockets[name]) {
      return;
    }
    const address = this.namedPipelinePushSockets[name][topic];
    delete this.namedPipelinePushSockets[name][topic];
    if (Object.keys(this.namedPipelinePushSockets[name]).length === 0) {
      delete this.namedPipelinePushSockets[name];
    }
    if (!address) {
      return;
    }
    this.shutdownPipelinePushAddress(topic, address);
    this.emit('disconnectPipelineConsumer', topic, name);
  }

  connectPipelineConsumer(topic        , address        , name        )      {
    if (!this.pipelinePushSockets[topic]) {
      return;
    }
    const push = this.pipelinePushSockets[topic];
    if (typeof push.connected[address] !== 'undefined') {
      return;
    }
    if (this.namedPipelinePushSockets[name] && this.namedPipelinePushSockets[name][topic] === address) {
      // Already connected
      return;
    }
    try {
      push.connect(address);
    } catch (error) {
      error.message = `Could not connect topic "${topic}" for push socket to ${address}: ${error.message}`;
      throw error;
    }
    if (push.connected[address] <= -1) {
      throw new Error(`Could not connect topic "${topic}" for push socket to ${address}`);
    }
    this.namedPipelinePushSockets[name] = this.namedPipelinePushSockets[name] || {};
    this.namedPipelinePushSockets[name][topic] = address;
    this.emit('connectPipelineConsumer', topic, name);
  }

  hasPipelineConsumer(topic       )         {
    return this.pipelineConsumers(topic).length > 0;
  }

  pipelineConsumers(topic       )                { // eslint-disable-line consistent-return
    return Object.keys(this.namedPipelinePushSockets).filter((name) => !!this.namedPipelinePushSockets[name][topic]);
  }

  isPipelineLeader(topic       )          {
    const peers = Object.keys(this.namedPipelinePushSockets).filter((name) => !!this.namedPipelinePushSockets[name][topic]);
    peers.sort();
    return peers[0] === this.name;
  }

  isProvidingPipeline(topic        ) {
    return !!this.pipelinePushSockets[topic];
  }

  providePipeline(topic        ) {
    if (this.pipelinePushSockets[topic]) {
      return;
    }
    const push = nano.socket('push', socketOptions);
    push.on('error', (error) => {
      this.emit('error', `Pipeline push socket for topic "${topic}": ${error.message}`);
    });
    this.pipelinePushSockets[topic] = push;
    this.sendToAll('_clusterAddPipelineProvider', {
      topic,
    });
    if (this.pipelinePullSockets[topic]) {
      this.connectPipelineConsumer(topic, this.pipelinePullBindAddress[topic], this.name);
    }
  }

  async stopProvidingPipeline(topic        ) {
    if (!this.pipelinePushSockets[topic]) {
      return;
    }
    await this.closePipelinePushSocket(topic);
  }

  consumePipeline(port        , topic        ) {
    if (this.pipelinePullSockets[topic]) {
      return;
    }
    const { host } = getSocketSettings(this.socketHash);
    const pullBindAddress = `tcp://${host}:${port}`;
    const pullSocket = nano.socket('pull', socketOptions);
    try {
      pullSocket.bind(pullBindAddress);
    } catch (error) {
      error.message = `Could not bind pipeline pull socket for topic "${topic}" to ${pullBindAddress}: ${error.message}`;
      throw error;
    }
    pullSocket.on('error', (error) => {
      this.emit('error', `Pipeline pull socket for topic "${topic}" at "${pullBindAddress}": ${error.message}`);
    });
    pullSocket.on('data', this.boundReceiveMessage);
    if (pullSocket.bound[pullBindAddress] <= -1) {
      this.emit('error', `Could not bind pipeline pull socket for topic "${topic}" to ${pullBindAddress}`);
    }
    this.pipelinePullSockets[topic] = pullSocket;
    this.pipelinePullBindAddress[topic] = pullBindAddress;
    this.sendToAll('_clusterAddPipelineConsumer', {
      topic,
      pushConnectAddress: pullBindAddress,
    });
    if (this.pipelinePushSockets[topic]) {
      this.connectPipelineConsumer(topic, pullBindAddress, this.name);
    }
  }

  advertisePipelines() {
    for (const topic of Object.keys(this.pipelinePushSockets)) {
      this.sendToAll('_clusterAddPipelineProvider', {
        topic,
      });
    }
  }

  async stopConsumingPipeline(topic        ) {
    this.sendToAll('_clusterRemovePipelineConsumer', { topic });
    this.disconnectPipelineConsumer(topic, this.name);
    await new Promise((resolve) => setTimeout(resolve, 100));
    await this.closePipelinePullSocket(topic);
  }

  getPeerNames()                {
    const names = [];
    for (const { name } of this.getPeers()) {
      if (typeof name === 'string') {
        names.push(name);
      }
    }
    return names;
  }

  getPeers()                        {
    const peers = [];
    for (const socketSettings of Object.keys(this.peerSocketHashes).map((socketHash) => getSocketSettings(socketHash))) {
      peers.push(socketSettings);
    }
    for (const socketSettings of Object.keys(this.unnamedPeerSocketHashes).map((socketHash) => getUnnamedSocketSettings(socketHash))) {
      peers.push(socketSettings);
    }
    return peers;
  }

  isLeader(name         = this.name)          {
    const peerSet = new Set(this.getPeers().map((settings) => settings.name));
    peerSet.add(this.name);
    const peers = [...peerSet];
    peers.sort();
    return peers[0] === name;
  }

  // See node-discover options https://github.com/wankdanker/node-discover#constructor
  async startDiscovery(options          = {}) {
    this.stopDiscovery();
    this.discovery = await new Promise((resolve, reject) => {
      const d = Discover(options, (error, success) => {
        if (error) {
          reject(error);
        } else if (success) {
          resolve(d);
        } else {
          reject(new Error(`Unknown discovery error with options ${JSON.stringify(options)}`));
        }
      });
    });
    this.discovery.advertise(getSocketSettings(this.socketHash));
    this.discovery.on('added', (message       ) => {
      if (message.address && message.advertisement && message.advertisement.name && message.advertisement.pubsubPort && message.advertisement.pipelinePort) {
        const socketSettings = {
          name: message.advertisement.name,
          host: message.address,
          pubsubPort: message.advertisement.pubsubPort,
          pipelinePort: message.advertisement.pipelinePort,
        };
        this.addPeer(socketSettings);
      }
    });
  }

  stopDiscovery() {
    if (this.discovery) {
      this.discovery.stop();
    }
    this.discovery = null;
  }

  /**
   * Throw an error if any internal data exists. Intended for tests and debugging.
   * @return {void}
   */
  throwOnLeakedReferences() {
    if (Object.keys(this.subSockets).length > 0) {
      throw new Error(`${this.name}: ${Object.keys(this.subSockets).length} referenced subSockets - ${Object.keys(this.subSockets).join(', ')}`);
    }
    if (Object.keys(this.pipelinePushSockets).length > 0) {
      throw new Error(`${this.name}: ${Object.keys(this.pipelinePushSockets).length} referenced pipelinePushSockets - ${Object.keys(this.pipelinePushSockets).join(', ')}`);
    }
    if (Object.keys(this.namedPipelinePushSockets).length > 0) {
      throw new Error(`${this.name}: ${Object.keys(this.namedPipelinePushSockets).length} referenced namedPipelinePushSockets - ${Object.keys(this.namedPipelinePushSockets).join(', ')}`);
    }
    if (Object.keys(this.pipelinePullSockets).length > 0) {
      throw new Error(`${this.name}: ${Object.keys(this.pipelinePullSockets).length} referenced pipelinePullSockets - ${Object.keys(this.pipelinePullSockets).join(', ')}`);
    }
    if (Object.keys(this.pipelinePullBindAddress).length > 0) {
      throw new Error(`${this.name}: ${Object.keys(this.pipelinePullBindAddress).length} referenced pipelinePullBindAddresses - ${Object.keys(this.pipelinePullBindAddress).join(', ')}`);
    }
    if (this.closed) {
      if (Object.keys(this.subscriptions).length > 0) {
        throw new Error(`${this.name}: ${Object.keys(this.subscriptions).length} referenced subscriptions - ${Object.keys(this.subscriptions).join(', ')}`);
      }
      if (Object.keys(this.localSubscriptions).length > 0) {
        throw new Error(`${this.name}: ${Object.keys(this.localSubscriptions).length} referenced localSubscriptions - ${Object.keys(this.localSubscriptions).join(', ')}`);
      }
    } else {
      const scrubbedSubscriptionKeys = without(Object.keys(this.subscriptions), '_clusterAddPeers', '_clusterRemovePeer', '_clusterRemoveHost', '_clusterAddPipelineConsumer', '_clusterRemovePipelineConsumer', '_clusterAddPipelineProvider', '_clusterHeartbeat', '_clusterAddPeers');
      if (scrubbedSubscriptionKeys.length > 0) {
        throw new Error(`${this.name}: ${scrubbedSubscriptionKeys.length} referenced subscriptions - ${scrubbedSubscriptionKeys.join(', ')}`);
      }
      const scrubbedLocalSubscriptionKeys = without(Object.keys(this.localSubscriptions), '_clusterAddPeers', '_clusterRemovePeer', '_clusterRemoveHost', '_clusterAddPipelineConsumer', '_clusterRemovePipelineConsumer', '_clusterAddPipelineProvider', '_clusterHeartbeat', '_clusterAddPeers');
      if (scrubbedLocalSubscriptionKeys.length > 0) {
        throw new Error(`${this.name}: ${scrubbedLocalSubscriptionKeys.length} referenced localSubscriptions - ${scrubbedLocalSubscriptionKeys.join(', ')}`);
      }
    }
    if (Object.keys(this.peerSocketHashes).length > 0) {
      throw new Error(`${this.name}: ${Object.keys(this.peerSocketHashes).length} referenced peerSocketHashes - ${Object.keys(this.peerSocketHashes).join(', ')}`);
    }
    if (Object.keys(this.unnamedPeerSocketHashes).length > 0) {
      throw new Error(`${this.name}: ${Object.keys(this.unnamedPeerSocketHashes).length} referenced unnamedPeerSocketHashes - ${Object.keys(this.unnamedPeerSocketHashes).join(', ')}`);
    }
    if (Object.keys(this.peerSocketHeartbeats).length > 0) {
      throw new Error(`${this.name}: ${Object.keys(this.peerSocketHeartbeats).length} referenced peerSocketHeartbeats - ${Object.keys(this.peerSocketHeartbeats).join(', ')}`);
    }
    if (Object.keys(this.namedPushSockets).length > 0) {
      throw new Error(`${this.name}: ${Object.keys(this.namedPushSockets).length} referenced namedPushSockets - ${Object.keys(this.namedPushSockets).join(', ')}`);
    }
  }
}

module.exports = ClusterNode;
