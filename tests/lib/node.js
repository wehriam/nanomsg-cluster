// @flow

import type { SocketSettings } from '../../src';

const expect = require('expect');
const ClusterNode = require('../../src');

module.exports.getNode = async (name:string, bindAddress:SocketSettings, peerAddresses:Array<SocketSettings>):Promise<ClusterNode> => {
  const node = new ClusterNode({
    name,
    bindAddress,
    peerAddresses,
  });
  expect(node.isReady).toEqual(false);
  await new Promise((resolve) => {
    node.on('ready', resolve);
  });
  node.on('error', console.error); // eslint-disable-line no-console
  expect(node.isReady).toEqual(true);
  return node;
};

module.exports.messageTimeout = ():Promise<void> => new Promise((resolve) => setTimeout(resolve, 250));
