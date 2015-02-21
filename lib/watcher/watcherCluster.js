/*!
 * .______    _______     ___      .______       ______     ___   .__________.
 * (   _  )  (   ____)   /   \     (   _  )     (      )   /   \  (          )
 * |  |_)  ) |  |__     /  ^  \    |  |_)  )   |  ,----'  /  ^  \ `---|  |---`
 * |   _  <  |   __)   /  /_\  \   |      )    |  |      /  /_\  \    |  |
 * |  |_)  ) |  |____ /  _____  \  |  |)  ----.|  `----./  _____  \   |  |
 * (______)  (_______/__/     \__\ ( _| `.____) (______)__/     \__\  |__|
 *
 * Bearcat-ha WatcherCluster
 * Copyright(c) 2015 fantasyni <fantasyni@163.com>
 * MIT Licensed
 */

var WatcherManager = require('./watcher_manager');
var Zookeeper = require('./zookeeper');
var async = require('async');

var watchers = {};
var DELAY = 3500;

var WatcherCluster = module.exports = {};

//set up
WatcherCluster.setup = function(config) {
  Zookeeper.createClient(config.zooKeeper, function() {
    async.eachSeries(config.nodes, function(node, next) {
      node.zooKeeper = config.zooKeeper;
      var name = node.name;
      if (!name || watchers[name]) {
        throw new Error('node name must be unequal!');
      }
      watchers[name] = new WatcherManager(node);

      setTimeout(next, DELAY);
    }, function() {
      console.log('watcher started');
    });
  });
};

// reset node after the config was changed
WatcherCluster.reset = function(config) {
  Zookeeper.createClient(config.zooKeeper, function() {
    async.eachSeries(config.nodes, function(node, next) {
      var watcher = watchers[node.name];
      node.zooKeeper = config.zooKeeper;
      if (!watcher) {
        console.log('add new watcher: %s', node.name);
        watchers[node.name] = new WatcherManager(node);
      } else {
        watcher.resetNode(node);
      }

      setTimeout(next, DELAY);
    }, function() {
      console.log('reset config complete!');
    });
  });
};