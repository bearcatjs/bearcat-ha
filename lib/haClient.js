/*!
 * .______    _______     ___      .______       ______     ___   .__________.
 * (   _  )  (   ____)   /   \     (   _  )     (      )   /   \  (          )
 * |  |_)  ) |  |__     /  ^  \    |  |_)  )   |  ,----'  /  ^  \ `---|  |---`
 * |   _  <  |   __)   /  /_\  \   |      )    |  |      /  /_\  \    |  |
 * |  |_)  ) |  |____ /  _____  \  |  |)  ----.|  `----./  _____  \   |  |
 * (______)  (_______/__/     \__\ ( _| `.____) (______)__/     \__\  |__|
 *
 * Bearcat-ha HaClient
 * Copyright(c) 2015 fantasyni <fantasyni@163.com>
 * MIT Licensed
 */

var logger = require('pomelo-logger').getLogger('bearcat-ha', 'HaClient');
var EventEmitter = require('events').EventEmitter;
var zooKeeper = require("node-zookeeper-client");
var Constant = require('./util/constant');
var zkEvent = zooKeeper.Event;
var async = require('async');
var Util = require('util');

var STATE_INIT = 0;
var STATE_CONNECTING = 1;
var STATE_CONNECTED = 2;

var HaClient = function(opts) {
	EventEmitter.call(this);
	this.indexs = {}; // slaves index
	this.haState = {};
	this.zkClient = null;
	this.state = STATE_INIT;
	this.reconnectTimer = null;
	this.chroot = opts.chroot || Constant.ZK_DEFAULT_CHROOT;
	this.servers = opts.servers;
	this.username = opts.username;
	this.password = opts.password;
	this.zkConnectTimeout = opts.zkConnectTimeout || Constant.ZK_DEFAULT_CONNECT_TIMEOUT;
	this.zkPath = opts.zkPath || Constant.ZK_DEFAULT_PATH;
	this.init();
}

Util.inherits(HaClient, EventEmitter);

HaClient.prototype.init = function() {
	var self = this;

	this.initZK(function() {
		logger.info('connect to ZooKeeper success! data: %j', self.haState);
		self.emit('ready');
	});
}

HaClient.prototype.initZK = function(opts, callback) {
	var self = this;
	var chroot = this.chroot;
	var servers = this.servers;
	var username = this.username;
	var password = this.password;
	var zkConnectTimeout = this.zkConnectTimeout;

	this.state = STATE_CONNECTING;
	var zkClient = zooKeeper.createClient(servers + chroot);
	var timeoutId = setTimeout(function() {
		logger.warn('connect to zookeeper timeout!');
	}, zkConnectTimeout);

	zkClient.once('connected', function() {
		clearTimeout(timeoutId);
		self.zkClient = zkClient;
		if (username) {
			zkClient.addAuthInfo('digest', new Buffer(username + ':' + password));
		}

		self.getZKChildrenData(function(data) {
			self.onChildrenChange(data, callback);
		});
	});

	zkClient.on('connected', function() {
		self.state = STATE_CONNECTED;
		clearInterval(self.reconnectTimer);
		self.reconnectTimer = null;
		logger.info('Connected to the Zookeeper server');
	});

	zkClient.on('disconnected', function() {
		self.state = STATE_INIT;
		logger.error('Disconnected to zookeeper server');
		delete zkClient;
		self.zkReconnect(opts);
	});

	zkClient.connect();
};

HaClient.prototype.zkReconnect = function(opts) {
	var self = this;

	if (self.reconnectTimer) {
		return;
	}

	self.clearUp();
	var s = Math.floor(Math.random(0, 1) * 50);
	var p = Math.floor(Math.random(0, 1) * 100);

	if (!self.reconnectTimer) {
		self.reconnectTimer = setInterval(function() {
			if (self.state < STATE_CONNECTING) {
				logger.info('zookeeper client reconnecting');
				self.initZK(opts, function() {})
			}
		}, 3000 + s * 100 + p);
	}
}

HaClient.prototype.clearUp = function() {
	for (var name in this.clientPool) {
		this.removeClient(name);
	}

	this.clientPool = {};
	delete this.haState;
	this.haState = {};
	delete this.indexs;
	this.indexs = {};
	delete this.zkClient;
	this.zkClient = null;
	this.state = STATE_INIT;
}

HaClient.prototype.onChildrenChange = function(data, callback) {
	var self = this;
	if (!callback) {
		callback = function() {};
	}
	async.each(Object.keys(data), function(path, next) {
		self.setState(path, data[path], function() {
			next();
		})
	}, callback);
};

HaClient.prototype.getZKChildrenData = function(callback) {
	var self = this;
	self.zkClient.getChildren(self.zkPath, function(event) {
		if (event.type == zkEvent.NODE_CHILDREN_CHANGED) {
			self.getZKChildrenData(function(data) {
				self.onChildrenChange(data);
			});
		}
	}, function(err, children) {
		var result = {};
		if (err) {
			logger.error('get ZooKeeper children error: %s', err.message);
			return callback(result);
		}
		async.each(children, function(path, next) {
			self.getZKData(path, function(data) {
				result[path] = data;
				next();
			});
		}, function() {
			callback(result);
		});
	});
};

HaClient.prototype.getZKData = function(path, callback) {
	var self = this;
	var fullPath = this.zkPath + '/' + path;
	this.zkClient.getData(fullPath, function(err, data) {
		if (err) {
			logger.error('get ZooKeeper data error: %s, path: %s', err.stack, fullPath);
		}

		if (data) {
			data = data.toString();
			try {
				data = JSON.parse(data);
			} catch (e) {
				logger.error('JSON parse ZooKeeper path: %s data: %s error: ', fullPath, data, e.stack);
				data = null;
			}
		} else {
			logger.error('zookeeper data is null! path: %s', fullPath);
		}

		callback(data);
	});
};

HaClient.prototype.watchZkData = function(path) {
	var self = this;
	self.zkClient.getData(self.zkPath + '/' + path, function(event) {
		logger.info('watchZkData %s event: %s', path, event.type);
		if (event.type == zkEvent.NODE_DATA_CHANGED) {
			self.getZKData(path, function(data) {
				self.onDataChange(path, data);
			});
		}
		if (event.type === zkEvent.NODE_DELETED) {
			logger.warn('node: %s is deleted', path);
			self.removeState(path);
		} else {
			self.watchZkData(path);
		}
	}, function(err) {
		if (err) {
			logger.error('watch zookeeper data, path: %s, error: %s', self.zkPath + '/' + path, err.stack);
		}
	});
};

// zookeeper data change
HaClient.prototype.onDataChange = function(path, state) {
	if (!state) return;

	var oldState = this.haState[path];
	this.setState(path, state);

	if (!oldState) {
		this.emit('nodeAdd', path, state);
		return;
	}

	this.emit('change', path, state);

	if (oldState.master != state.master) {
		this.emit('masterChange', path, state.master);
	}
};

HaClient.prototype.setState = function(name, state, callback) {
	if (!this.haState[name]) {
		this.watchZkData(name);
	}

	if (!state) {
		return callback && callback();
	}

	this.haState[name] = state;
};

HaClient.prototype.removeState = function(name) {
	var state = this.haState[name];
	if (!state) return;

	this.emit('nodeRemove', name, state);
};

//get client node
HaClient.prototype.getClient = function(name, role) {
	var state = this.haState[name];

	if (!state) {
		this.zkReconnect(this.opts);
		logger.error('redisFailover getClient error %s %s %j \n %s', name, role, this.haState, new Error('').stack);
		return;
	}

	var clientNode = null;

	if (role === 'slave') {
		var index = this.indexs[name] || 0;
		if (index >= state.slaves.length) {
			index = 0;
		}

		clientNode = state.slaves[index++];
		this.indexs[name] = index;
	} else {
		clientNode = state.master;
	}

	if (!clientNode) {
		logger.error('redisFailover clientNode null error %s %s %j \n %s', name, role, this.haState, new Error('').stack);
	}

	return clientNode;
};


module.exports = HaClient;