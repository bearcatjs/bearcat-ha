/*!
 * .______    _______     ___      .______       ______     ___   .__________.
 * (   _  )  (   ____)   /   \     (   _  )     (      )   /   \  (          )
 * |  |_)  ) |  |__     /  ^  \    |  |_)  )   |  ,----'  /  ^  \ `---|  |---`
 * |   _  <  |   __)   /  /_\  \   |      )    |  |      /  /_\  \    |  |
 * |  |_)  ) |  |____ /  _____  \  |  |)  ----.|  `----./  _____  \   |  |
 * (______)  (_______/__/     \__\ ( _| `.____) (______)__/     \__\  |__|
 *
 * Bearcat-ha WatcherLock
 * Copyright(c) 2015 fantasyni <fantasyni@163.com>
 * MIT Licensed
 */

var logger = require('pomelo-logger').getLogger('bearcat-ha', 'WatcherLock');
var EventEmitter = require('events').EventEmitter;
var Zookeeper = require('node-zookeeper-client');
var Constant = require('../util/constant');
var zkCreateMode = Zookeeper.CreateMode;
var zkEvent = Zookeeper.Event;
var Util = require('util');

var WATCHER_LOCK_PREFIX = Constant.WATCHER_LOCK_PREFIX;

var WatcherLock = function(zkClient, lockPath, callback) {
  EventEmitter.call(this);
  this.leader = null;
  this.isMaster = false;
  this.zkClient = zkClient;
  this.lockPath = lockPath;
  this.currentLockPath = null;
  this._init(callback);
}

Util.inherits(WatcherLock, EventEmitter);

WatcherLock.prototype._init = function(callback) {
  var self = this;
  this.zkClient.create(this.lockPath + '/' + WATCHER_LOCK_PREFIX, zkCreateMode.EPHEMERAL_SEQUENTIAL, function(err, path) {
    if (err) {
      callback(err);
      return;
    }
    self.currentLockPath = path.replace(self.lockPath + '/', '');
    self.check(callback);
  });
};

WatcherLock.prototype.check = function(callback) {
  var self = this;
  callback = callback || function() {};

  this.getLocks(function(err, children) {
    if (err) {
      logger.error('check getLocks error: ' + err.stack);
      return callback(err);
    }

    logger.warn('children, %j, self path: %s', children, self.currentLockPath);
    if (self.currentLockPath == children[0]) {
      self.isMaster = true;
      self.leader = null;
      logger.info('this lock: %s is Master', self.currentLockPath);
      callback(null, self);
      self.emit('promote', self);
    } else {
      var index = children.indexOf(self.currentLockPath);
      self.leader = children[index - 1];
      self.watchLeader();
      callback(null, self);
    }
  });
};

WatcherLock.prototype.getLocks = function(callback) {
  this.zkClient.getChildren(this.lockPath, function(err, children) {
    if (err) {
      return callback(err);
    }

    children.sort(function(a, b) {
      return parseInt(a.substr(WATCHER_LOCK_PREFIX.length), 10) - parseInt(b.substr(WATCHER_LOCK_PREFIX.length), 10);
    });

    callback(null, children);
  });
};

WatcherLock.prototype.watchLeader = function() {
  if (this.isMaster) {
    return;
  }

  var self = this;
  this.zkClient.exists(this.lockPath + '/' + this.leader, this.watcherCb.bind(this), function(err, stat) {
    if (err) {
      logger.error('watchLeader exists error ' + err.stack);
    }

    if (!stat) {
      logger.warn('leader not exist! , leader: %s', self.leader);
    }
  });
};

WatcherLock.prototype.watcherCb = function(event) {
  if (event.type === zkEvent.NODE_DELETED) {
    logger.info('leader node was deleted!, leader: %s', event.toString());
    this.check();
  }
};

module.exports = WatcherLock;