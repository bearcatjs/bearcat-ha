var HaClient = require('./lib/haClient');
var MockHaClient = require('./test/haClient');

var bearcatHa = {};

bearcatHa.createClient = function(opts) {
	// return new HaClient(opts);
	return new MockHaClient(opts);
}

module.exports = bearcatHa;