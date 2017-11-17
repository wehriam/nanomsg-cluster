//      

const uuid = require('uuid');
const nano = require('nanomsg');
const events = require('events');
const { merge } = require('lodash');
const Discover = require('node-discover');

                              
                
               
                      
                       
  

                   
               
                 
                 
                  
                            
                           
                    
  

                      
               
                    
                  
                 
                                
                           
                    
  

                
             
                                 
                                          
    
               
  

const DEFAULT_PUBSUB_PORT = 13001;
const DEFAULT_PIPELINE_PORT = 13002;

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
    // Bind a nanomsg pull socket for incoming direct messages
    // http://nanomsg.org/v1.0.0/nn_pipeline.7.html
    const pullBindAddress = `tcp://${clusterOptions.bindAddress.host}:${clusterOptions.bindAddress.pipelinePort}`;
    this.pullSocket = nano.socket('pull');
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
    this.pubSocket = nano.socket('pub');
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
        const name = socketHash.split('/').shift();
        this.peerSocketHashes[socketHash] = true;
        const socketSettings = getSocketSettings(socketHash);
        if (!this.namedPushSockets[name]) {
          this.emit('addPeer', socketSettings);
        }
        this.namedPushSockets[name] = this.addPeer(socketSettings);
      });
    });
    this.subscribe('_clusterRemovePeer', (message       ) => {
      this.removePeer(getSocketSettings(message.socketHash));
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
    // Connect to peers included in the options
    clusterOptions.peerAddresses.forEach(this.addPeer.bind(this));
    setImmediate(() => {
      this.isReady = true;
      this.emit('ready');
    });
  }

  receiveMessage(buffer       )      {
    const [topic, message, name] = JSON.parse(String(buffer));
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
    push.send(JSON.stringify([topic, message, this.name]));
  }

  sendToAll(topic       , message    )      {
    this.pubSocket.send(JSON.stringify([topic, message, this.name]));
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
    push.send(JSON.stringify([topic, message, this.name]));
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
      return;
    }
    this.subscriptions[topic] = [];
    this.localSubscriptions[topic] = [];
  }

  async close()               {
    if (this.closed) {
      throw new Error('Already closed.');
    }
    this.stopDiscovery();
    if (this.clusterUpdateTimeout) {
      clearTimeout(this.clusterUpdateTimeout);
    }
    this.sendToAll('_clusterRemovePeer', {
      socketHash: this.socketHash,
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    await Promise.all(Object.keys(this.pipelinePushSockets).map(this.closePipelinePushSocket.bind(this)));
    await Promise.all(Object.keys(this.pipelinePullSockets).map(this.closePipelinePullSocket.bind(this)));
    this.pullSocket.removeListener('data', this.boundReceiveMessage);
    await new Promise((resolve) => {
      this.pullSocket.on('close', resolve);
      this.pullSocket.close();
    });
    await new Promise((resolve) => {
      this.pubSocket.on('close', resolve);
      this.pubSocket.close();
    });
    await Promise.all(Object.keys(this.subSockets).map(this.closeSubSocket.bind(this)));
    await Promise.all(Object.keys(this.pushSockets).map(this.closePushSocket.bind(this)));
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
      const sub = nano.socket('sub');
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
      const push = nano.socket('push');
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
      push.send(JSON.stringify(['_clusterAddPeers', {
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
    }, 10);
    return this.pushSockets[pushConnectAddress];
  }

  async removePeer(peerAddress               )               {
    const pubsubConnectAddress = `tcp://${peerAddress.host}:${peerAddress.pubsubPort || DEFAULT_PUBSUB_PORT}`;
    const pushConnectAddress = `tcp://${peerAddress.host}:${peerAddress.pipelinePort || DEFAULT_PIPELINE_PORT}`;
    const peerExists = this.subSockets[pubsubConnectAddress] || this.pushSockets[pushConnectAddress];
    if (!peerExists) {
      return;
    }
    Object.keys(this.namedPushSockets).forEach((name       ) => {
      if (this.namedPushSockets[name] === this.pushSockets[pushConnectAddress]) {
        delete this.namedPushSockets[name];
        const socketHash = getSocketHash(name, peerAddress);
        delete this.peerSocketHashes[socketHash];
        this.sendToAll('_clusterRemovePeer', {
          socketHash,
        });
        if (this.namedPipelinePushSockets[name]) {
          Object.keys(this.namedPipelinePushSockets[name]).forEach((topic       ) => {
            this.disconnectPipelineConsumer(topic, name);
          });
        }
      }
      this.emit('removePeer', Object.assign({}, { name }, peerAddress));
    });
    await Promise.all([
      this.closeSubSocket(pubsubConnectAddress),
      this.closePushSocket(pushConnectAddress),
    ]);
  }

  async closeSubSocket(address       )               {
    const sub = this.subSockets[address];
    delete this.subSockets[address];
    sub.removeListener('data', this.boundReceiveMessage);
    await new Promise((resolve, reject) => {
      sub.on('close', resolve);
      sub.on('error', reject);
      sub.close();
    });
  }

  async closePushSocket(address       )               {
    const push = this.pushSockets[address];
    delete this.pushSockets[address];
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
    await new Promise((resolve) => {
      pullSocket.on('close', resolve);
      pullSocket.close();
    });
  }

  disconnectPipelineConsumer(topic        , name        )       {
    if (!this.namedPipelinePushSockets[name]) {
      return;
    }
    const address = this.namedPipelinePushSockets[name][topic];
    delete this.namedPipelinePushSockets[name][topic];
    if (!address) {
      return;
    }
    this.shutdownPipelinePushAddress(topic, address);
  }

  connectPipelineConsumer(topic        , address        , name        )      {
    if (!this.pipelinePushSockets[topic]) {
      return;
    }
    const push = this.pipelinePushSockets[topic];
    if (typeof push.connected[address] !== 'undefined') {
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
  }

  providePipeline(topic        ) {
    if (this.pipelinePushSockets[topic]) {
      return;
    }
    const push = nano.socket('push');
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

  consumePipeline(port        , topic        ) {
    if (this.pipelinePullSockets[topic]) {
      return;
    }
    const { host } = getSocketSettings(this.socketHash);
    const pullBindAddress = `tcp://${host}:${port}`;
    const pullSocket = nano.socket('pull');
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

  async stopConsumingPipeline(topic        ) {
    this.sendToAll('_clusterRemovePipelineConsumer', { topic });
    await new Promise((resolve) => setTimeout(resolve, 100));
    await this.closePipelinePullSocket(topic);
  }

  getPeers()                        {
    return Object.keys(this.peerSocketHashes).map((socketHash) => getSocketSettings(socketHash));
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
}

module.exports = ClusterNode;
