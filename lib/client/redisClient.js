/*!
 * .______    _______     ___      .______       ______     ___   .__________.
 * (   _  )  (   ____)   /   \     (   _  )     (      )   /   \  (          )
 * |  |_)  ) |  |__     /  ^  \    |  |_)  )   |  ,----'  /  ^  \ `---|  |---`
 * |   _  <  |   __)   /  /_\  \   |      )    |  |      /  /_\  \    |  |
 * |  |_)  ) |  |____ /  _____  \  |  |)  ----.|  `----./  _____  \   |  |
 * (______)  (_______/__/     \__\ ( _| `.____) (______)__/     \__\  |__|
 *
 * Bearcat-ha RedisClient
 * Copyright(c) 2015 fantasyni <fantasyni@163.com>
 * MIT Licensed
 */

var logger = require('pomelo-logger').getLogger('bearcat-ha', 'RedisClient');
var EventEmitter = require('events').EventEmitter;
var Constant = require('../util/constant');
var redis = require('redis');
var Util = require('util');

var RedisClient = function(opts) {
	EventEmitter.call(this);
	this.host = opts.host;
	this.port = opts.port;
	this.maxFailures = opts.maxFailures || Constant.REDIS_MAX_FAILURES;
	this.pingTimeout = opts.pingTimeout || Constant.REDIS_PING_TIMEOUT;
	this.pingInterval = opts.pingInterval || Constant.REDIS_PING_INTERVAL;
	this.retryMaxDelay = opts.retryMaxDelay || Constant.REDIS_RETRY_MAX_DELAY;
	this.password = opts.password;
	this.failures = 0;
	this.client = null;
	this.interval = null;
	this.isMaster = false;
	this.available = false;
	this.name = this.host + ':' + this.port;
}

Util.inherits(RedisClient, EventEmitter);

RedisClient.prototype.init = function() {
	var self = this;
	var retryMaxDelay = this.retryMaxDelay;
	var options = {
		retry_max_delay: retryMaxDelay
	};

	if (this.password) {
		options['auth_pass'] = this.password;
	}

	this.client = redis.createClient(this.port, this.host, options);
	this.client.on('ready', function() {
		self.updateInfo(function() {
			self.watch();
		});
	});

	this.client.on('error', function(err) {
		logger.error('connect to redis %s error: %s', self.name, err.message);
	});

	this.client.on('end', function() {
		if (self.available) {
			self.available = false;
			logger.warn('%s redis client is end, will emit unavailable', self.name);
			self.emit('unavailable', self);
		}
		self.stopWatch();
	});
}

RedisClient.prototype.close = function() {
	this.available = false;
	this.removeAllListeners();
	clearTimeout(this.client.retry_timer);
	this.stopWatch();
	this.client.end();
	this.client.removeAllListeners();
	this.client = null;
};

RedisClient.prototype.fail = function() {
	this.failures += 1;
	if (this.failures >= this.maxFailures) {
		logger.error('%s fail %s times, will be emit unavailable!', this.name, this.failures);
		this.available = false;
		this.stopWatch();
		this.emit('unavailable', this);
		this.failures = 0;
	}
};

RedisClient.prototype.ping = function() {
	var self = this;
	var timeout = setTimeout(function() {
		logger.warn('%s redis ping timeout %s failures %s', self.name, self.pingTimeout, self.failures);
		self.fail();
	}, self.pingTimeout);
	this.client.ping(function(err) {
		clearTimeout(timeout);
		if (err) {
			self.fail();
			logger.warn('%s redis ping error: %s, failures %s', self.name, err.message, self.failures);
		}
	});
};

RedisClient.prototype.watch = function() {
	var self = this;
	if (this.interval) {
		this.stopWatch();
	}
	this.interval = setInterval(function() {
		self.ping();
	}, self.pingInterval);
};

RedisClient.prototype.stopWatch = function() {
	clearInterval(this.interval);
};

RedisClient.prototype.slaveOf = function(master, callback) {
	var self = this;
	this.updateInfo(function() {
		var masterName = master.host + ':' + master.port;
		if (self.name === masterName || self.master === masterName) return callback();

		self.client.slaveof(master.host, master.port, function(err) {
			self.updateInfo(function() {
				callback(err);
			});
		});
	});
};

RedisClient.prototype.makeMaster = function(callback) {
	this.slaveOf({
		host: 'NO',
		port: 'ONE'
	}, callback);
};

RedisClient.prototype.updateHZ = function(hz, cb) {
	this.client.config(['set', 'hz', hz], cb);
}

RedisClient.prototype.getInfo = function(callback) {
	var self = this;
	this.client.info('replication', function(err, info) {
		if (err) {
			logger.error('get %s info error: %s', self.name, err.message);
			return callback();
		}

		var obj = {};
		var lines = info.toString().split("\r\n");
		lines.forEach(function(line) {
			var parts = line.split(':');
			if (parts[1]) {
				obj[parts[0]] = parts[1];
			}
		});

		callback(obj);
	});
};

RedisClient.prototype.updateInfo = function(callback) {
	var self = this;
	this.getInfo(function(info) {
		if (!info) {
			callback && callback();
			return;
		}

		if (self.failures > 0) {
			self.failures = 0;
		}

		if (info['role'] === 'master') {
			self.isMaster = true;
			self.master = null;
			self.linkedMaster = false;
		} else {
			self.isMaster = false;
			self.master = info['master_host'] + ':' + info['master_port'];
			self.linkedMaster = info['master_link_status'] === 'up';
		}

		self.slaves = self.getSlaves(self.client.server_info['redis_version'], info);

		self.syncing = info['master_sync_in_progress'] == '1';
		if (self.syncing) {
			logger.warn('%s is syncing with master %s', self.name, self.master);
			setTimeout(function() {
				if (!self.client) return;
				self.updateInfo();
			}, 10000);
			if (self.available) {
				self.available = false;
				self.emit('unavailable', self);
			}
		} else if (!self.available) {
			self.available = true;
			self.emit('available', self);
		}

		callback && callback();
	});
};

RedisClient.prototype.getSlaves = function(redisVersion, info) {
	redisVersion = parseFloat(redisVersion);
	var slavesCount = parseInt(info['connected_slaves']) || 0;
	var slaves = [];

	for (var i = 0; i < slavesCount; i++) {
		var ary = info['slave' + i].split(',');
		var obj = {};
		if (redisVersion >= 2.8) {
			ary.map(function(item) {
				var k = item.split('=');
				obj[k[0]] = k[1];
			});
		} else {
			obj.ip = ary[0];
			obj.port = ary[1];
			obj.state = ary[2];
		}
		if (obj.state === 'online') {
			slaves[i] = obj.ip + ':' + obj.port;
		}
	}

	return slaves;
};

RedisClient.prototype.toJSON = function() {
	return {
		name: this.name,
		master: this.master,
		slaves: this.slaves,
		isMaster: this.isMaster,
		available: this.available,
		linkedMaster: this.linkedMaster
	};
};

module.exports = RedisClient;