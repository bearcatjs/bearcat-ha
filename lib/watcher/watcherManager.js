/*!
 * .______    _______     ___      .______       ______     ___   .__________.
 * (   _  )  (   ____)   /   \     (   _  )     (      )   /   \  (          )
 * |  |_)  ) |  |__     /  ^  \    |  |_)  )   |  ,----'  /  ^  \ `---|  |---`
 * |   _  <  |   __)   /  /_\  \   |      )    |  |      /  /_\  \    |  |
 * |  |_)  ) |  |____ /  _____  \  |  |)  ----.|  `----./  _____  \   |  |
 * (______)  (_______/__/     \__\ ( _| `.____) (______)__/     \__\  |__|
 *
 * Bearcat-ha WatcherManager
 * Copyright(c) 2015 fantasyni <fantasyni@163.com>
 * MIT Licensed
 */

var logger = require('pomelo-logger').getLogger('bearcat-ha', 'WatcherManager');
var ZookeeperClient = require('../client/zookeeperClient');
var EventEmitter = require('events').EventEmitter;
var WatcherStrategy = require('./strategy');
var Constant = require('../util/constant');
var Utils = require('../util/utils');
var async = require('async');
var Util = require('util');

var ADD_NODE_DELAY_TIME = Constant.ADD_NODE_DELAY_TIME;
var ADD_NODES_DELAY_TIME = Constant.ADD_NODES_DELAY_TIME;
var MASTER_CHECK_NODE_TIME = Constant.MASTER_CHECK_NODE_TIME;

var HOST_NAME = Utils.getHostName();
var PROCESS_PID = process.pid;

var WatcherManager = function(opts) {
    EventEmitter.call(this);
    this.opts = opts;
    this.haState = null;
    this.masterNode = null;
    this.watcherNodes = {};

    this.rootPath = Constant.ZK_DEFAULT_PATH + '/' + opts.name;
    this.locksPath = this.rootPath + '/locks';
    this.watchersPath = this.rootPath + '/watchers';
    this.watcherManagerPath = this.watchersPath + '/' + HOST_NAME + '-' + PROCESS_PID;

    this.failures = 0;
    this.zkClient = null;
    this.isMaster = false;
    this.intervalId = null;
    this.isolatedNodes = [];
    this.watcherClient = null; // setup self-defined watcher client
    this.watcherType = opts.watcherType || Constant.WATCHER_TYPE_REDIS; // builtin ha watcher type redis or mysql
    this.readyForCollectAndUpdate = false;

    this.init();
}

Util.inherits(WatcherManager, EventEmitter);

WatcherManager.prototype.init = function() {
    var opts = this.opts;

    if (opts.addNodeDelayTime) {
        ADD_NODE_DELAY_TIME = opts.addNodeDelayTime;
    }

    if (opts.addNodesDelayTime) {
        ADD_NODES_DELAY_TIME = opts.addNodesDelayTime;
    }

    if (opts.masterCheckNodeTime) {
        MASTER_CHECK_NODE_TIME = opts.masterCheckNodeTime;
    }

    var watcherType = this.watcherType;
    if (watcherType !== Constant.WATCHER_TYPE_REDIS && watcherType !== WATCHER_TYPE_MYSQL) {
        logger.error('invalid watcherType %s', watcherType);
        return;
    }

    this.initZKClient();
};

WatcherManager.prototype.initZKClient = function() {
    var self = this;
    var opts = this.opts;

    this.zkClient = ZookeeperClient.createClient(opts.zooKeeper);
    var paths = [self.locksPath, self.watchersPath];
    self.zkClient.createPathBatch(paths, function(err) {
        if (err) {
            logger.error('init createPathBatch error: ' + err.stack);
            throw err;
        }

        self.zkClient.createEphemeral(self.watcherManagerPath, function(err) {
            if (err) {
                logger.error('init createEphemeral error: ' + err.stack);
                throw err;
            }

            self.ready();
        });
    });
}

WatcherManager.prototype.ready = function() {
    var self = this;
    this.getLock(function() {
        self.initWatcherNodes();

        if (self.checkMaster()) {
            self.startCollect();
        }
    });
};

WatcherManager.prototype.startCollect = function() {
    var self = this;
    if (self.intervalId) {
        clearInterval(self.intervalId);
    }

    if (self.checkMaster()) {
        self.intervalId = setInterval(function() {
            self.collectData();
        }, MASTER_CHECK_NODE_TIME);
    }
};

WatcherManager.prototype.collectData = function() {
    if (!this.readyForCollectAndUpdate) {
        return;
    }

    var self = this;
    self.zkClient.getChildrenData(self.watchersPath, function(err, data) {
        if (err) {
            logger.error('collectData getChildrenData error ' + err.stack);
            self.failures += 1;
            if (self.failures >= 3) {
                throw err;
            }
            return;
        }

        var result = WatcherStrategy.elect(data);
        // master unavailable
        if (!self.masterNode || result.unavailable.indexOf(self.masterNode.name) > -1) {
            logger.warn('master node unavailable, will promote a new one in %j ...', result.available);
            //master is unavailable
            self.masterNode = null;
            self.promoteHaMaster(result.available, 0, function() {
                self.updateHaState(result);
            });
            return;
        }

        self.updateHaState(result);
    });
};

WatcherManager.prototype.onPromote = function() {
    logger.info('Promote to watcherManager master node ...')
    this.resetWatcherNodes();
    this.startCollect();
};

// node available
WatcherManager.prototype.onAvailable = function(availableNode) {
    var self = this;
    var availableNodeName = availableNode.name;

    logger.info('watcherNode available, %j', availableNode);
    var masterNode = this.masterNode;
    var masterNodeName = masterNode.name;

    // if this manager is master
    if (self.checkMaster()) {
        if (masterNode) {
            if (availableNodeName !== masterNodeName && availableNode.master != masterNodeName) {
                availableNode.slaveOf(masterNode, function(err) {
                    if (err) {
                        logger.error('%s slave of %s fail, reason: %s', availableNodeName, masterNodeName, err.stack);
                    } else {
                        logger.info('%s slave of %s success ...', availableNodeName, masterNodeName);
                    }
                });
            }
        } else if (availableNode.checkMaster()) {
            masterNode = availableNode;
            self.setMasterNode(masterNode);
            // self.masterNode = availableNode;
            async.eachSeries(self.isolatedNodes, function(n, next) {
                var isolatedNode = self.watcherNodes[n];
                if (!isolatedNode || isolatedNode.master == masterNode.name) {
                    return next();
                }

                isolatedNode.slaveOf(masterNode, function(err) {
                    if (err) {
                        logger.error('%s slave of %s fail, reason: %s', isolatedNode.name, masterNode.name, err.stack);
                    } else {
                        logger.info('%s slave of %s success ...', isolatedNode.name, masterNode.name);
                    }
                    next();
                });
            }, function() {
                self.isolatedNodes = [];
            });
        } else {
            self.isolatedNodes.push(name);
        }
    }

    self.updateData();
};

// node unavailable
WatcherManager.prototype.onUnavailable = function(unavailableNode) {
    var unavailableNodeName = unavailableNode.name;
    logger.warn('node %s is unavailable ...', unavailableNodeName);

    this.removeWatcherNode(unavailableNodeName);
    this.addWatcherNode(unavailableNode.options);

    this.updateData();

    if (this.checkMaster()) {
        this.collectData();
    }
};

WatcherManager.prototype.addWatcherNode = function(opts) {
    var watcherNode = this.getWatcherClient(opts);
    var watcherNodeName = watcherNode.name;

    watcherNode.on('available', this.onAvailable.bind(this));
    watcherNode.on('unavailable', this.onUnavailable.bind(this));

    this.setWatcherNode(watcherNodeName, watcherNode);

    logger.info('add watcherNode, %j', watcherNode);
};

WatcherManager.prototype.removeWatcherNode = function(name) {
    logger.info('remove node name: %s', name);
    var watcherNode = this.getWatcherNode(name);
    watcherNode.close();
    watcherNode = null;
    delete this.watcherNodes[name];
};

WatcherManager.prototype.initWatcherNodes = function() {
    var self = this;
    var watcherServers = self.opts.servers.split(',');
    async.eachSeries(watcherServers, function(server, next) {
        var serverData = server.split(':');
        var host = serverData[0];
        var port = serverData[1];
        var opts = self.getClonedOpts();
        opts['host'] = host;
        opts['port'] = port;
        self.addWatcherNode(opts);
        setTimeout(next, ADD_NODE_DELAY_TIME);
    }, function() {
        setTimeout(function() {
            logger.info('ready for update data to zookeeper!');
            self.readyForCollectAndUpdate = true;
            self.updateData();
        }, ADD_NODES_DELAY_TIME);
    });
};

WatcherManager.prototype.close = function() {
    for (var name in this.watcherNodes) {
        this.removeWatcherNode(name);
    }
    this.zkClient.close();
    this.zkClient = null;
};

WatcherManager.prototype.resetWatcherNodes = function(opts) {
    logger.info('Reset all watcherNodes ...');
    this.readyForCollectAndUpdate = false;
    if (opts) {
        this.opts = opts;
    }

    for (var name in this.watcherNodes) {
        this.removeWatcherNode(name);
    }

    this.initWatcherNodes();
};

WatcherManager.prototype.promoteHaMaster = function(availableNodes, index, callback) {
    var self = this;
    if (this.masterNode && this.masterNode.available) {
        return callback();
    }

    index = index || 0;

    if (index >= availableNodes.length) {
        logger.error('no availableNodes can be promoted to be Master!');
        return callback();
    }

    logger.info('promote new haMaster, current availableNodes: %j', availableNodes);
    var readyPromoteNodeName = availableNodes[index];
    var readyPromoteNode = this.getWatcherNode(readyPromoteNodeName);

    if (!readyPromoteNode || !readyPromoteNode.available) {
        return self.promoteHaMaster(availableNodes, index + 1, callback);
    }

    readyPromoteNode.makeMaster(function(err) {
        if (err) {
            logger.error('promoteHaMaster make %s to master error %s', readyPromoteNode.name, err.stack);
            return self.promoteHaMaster(availableNodes, index + 1, callback);
        }

        var masterNode = readyPromoteNode;
        var masterNodeName = masterNode.name;
        self.setMasterNode(masterNode);

        logger.info('make %s to master success!', readyPromoteNode.name);

        availableNodes.splice(index, 1);
        async.each(availableNodes, function(name, next) {
            var slaveNode = self.availableNodes[name];
            if (!slaveNode) {
                return next();
            }

            slaveNode.slaveOf(masterNode, function(err) {
                if (err) {
                    logger.error('%s slave to master: %s error: %s', name, masterNodeName, err.stack);
                    self.onUnavailable(slaveNode);
                } else {
                    logger.info('%s slave to master: %s success ...', name, masterNodeName);
                }
                next();
            });
        }, function() {
            logger.info('promote a new master: %s success ...', masterNodeName);
            callback();
        });
    });
};

// update ha state to zookeeper
WatcherManager.prototype.updateData = function() {
    if (!this.readyForCollectAndUpdate) {
        return;
    }

    var available = [];
    var unavailable = [];

    for (var name in this.watcherNodes) {
        var watcherNode = this.getWatcherNode(name);
        if (watcherNode.checkAvailable()) {
            available.push(name);
        } else {
            unavailable.push(name);
        }
    }

    this.zkClient.setData(this.path, {
        available: available,
        unavailable: unavailable
    }, function(err) {
        if (err) {
            logger.error('updateData zkClient setData error ' + err.stack);
            throw err;
        }
    });
};

WatcherManager.prototype.updateNodesInfo = function(callback) {
    var self = this;
    var names = Object.keys(self.watcherNodes);
    async.each(names, function(name, next) {
        var watcherNode = self.getWatcherNode(name);
        if (watcherNode.checkAvailable()) {
            return watcherNode.updateInfo(next);
        }

        next();
    }, callback);
};

// master update the elect result to zookeeper
WatcherManager.prototype.updateHaState = function(data) {
    if (!this.checkMaster()) {
        return;
    }

    if (data.available.length === 0) {
        var haState = {
            master: null,
            slaves: [],
            unavailable: data.unavailable
        };
        return this.setState(haState);
    }

    var self = this;
    var masterNodeName = this.masterNode.name;

    self.updateNodesInfo(function() {
        var slaves = [];
        data.available.forEach(function(name) {
            if (name == masterNodeName) {
                return;
            }

            var watcherNode = self.watcherNodes[name];
            if (!watcherNode) {
                return;
            }

            var watcherNodeMaster = watcherNode.getMaster();
            var watcherNodeMasterStatus = watcherNode.getMasterStatus();

            if (watcherNode.checkAvailable() && watcherNodeMaster === masterNodeName && watcherNodeMasterStatus) {
                slaves.push(name);
            } else {
                data.unavailable.push(name);

                if (watcherNodeMaster && !watcherNodeMasterStatus) {
                    logger.warn('%s linked to master: %s fail!', name, watcherNodeMaster);
                }
            }
        });

        var haState = {
            master: masterNodeName,
            slaves: slaves,
            unavailable: data.unavailable
        };

        self.setState(haState);
    });
};

WatcherManager.prototype.setState = function(state) {
    if (this.checkStateChange(state)) {
        this.haState = state;

        this.zkClient.setData(this.rootPath, this.haState, function(err) {
            if (err) {
                logger.error('setState zkClient setData error ' + err.stack);
                throw err;
            }

            logger.info('update ha state success!, state: %j', state);
        });
    }
};

//check the result to local haState
WatcherManager.prototype.checkStateChange = function(state) {
    var oldState = this.haState;
    if (!oldState || oldState.master != state.master) {
        return true;
    }

    if (oldState.slaves.length !== state.slaves.length) {
        return true;
    }

    if (oldState.unavailable.length !== state.unavailable.length) {
        return true;
    }

    return false;
};

WatcherManager.prototype.getLock = function(callback) {
    var self = this;
    self.zkClient.createLock(self.locksPath, function(err, lock) {
        if (err) {
            logger.error('getLock zkClient createLock error ' + err.stack);
            callback(err);
            throw err;
        }

        self.lock = lock;
        self.setMaster(lock.checkMaster());

        self.lock.on('promote', function() {
            if (!self.checkMaster()) {
                self.setMaster(true);
                self.onPromote();
            }
        });

        logger.info('create lock success, this is %s monitor ...', self.checkMaster() ? 'master' : 'spare');
        callback();
    });
};

WatcherManager.prototype.setWatcherClient = function(watcherClient) {
    this.watcherClient = watcherClient;
}

WatcherManager.prototype.getWatcherClient = function(opts) {
    var watcherClient = this.watcherClient;
    if (!watcherClient) {
        var watcherType = this.watcherType;
        var WatcherClient = require('../client/' + watcherType + "Client");
        watcherClient = new WatcherClient(opts);
        this.watcherClient = watcherClient;
    }

    return watcherClient;
}

WatcherManager.prototype.setWatcherNode = function(name, watcherNode) {
    this.watcherNodes[name] = watcherNode;
}

WatcherManager.prototype.getWatcherNode = function(name) {
    return this.watcherNodes[name];
}

WatcherManager.prototype.setMasterNode = function(masterNode) {
    this.masterNode = masterNode;
}

WatcherManager.prototype.checkMaster = function() {
    return this.isMaster;
}

WatcherManager.prototype.setMaster = function(isMaster) {
    this.isMaster = isMaster;
}

WatcherManager.prototype.getClonedOpts = function() {
    var opts = this.opts;
    var r = {};

    for (var key in opts) {
        r[key] = opts[key];
    }

    return r;
}

module.exports = WatcherManager;