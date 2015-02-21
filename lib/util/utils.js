var os = require('os');

var Utils = {};

Utils.getHostName = function() {
	return os.hostname();
}

module.exports = Utils;