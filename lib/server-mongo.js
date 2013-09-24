var util = require('./util');
var PeerServer = require('./server').PeerServer;
var MongoClient = require('mongodb').MongoClient,
    ObjectID = require('mongodb').ObjectID;

function PeerServerMongo(options) {
  PeerServer.call(this, options);

  // database connection
  this._mongo = null;

  // clients collection
  this._mongo_client = null;

  // log collection
  this._mongo_log = null;

  // ip collection
  this._mongo_ip = null;

  // Initialize DB connection
  this._initializeDB();
}

util.inherits(PeerServerMongo, PeerServer);

/** Initialize database connection */
PeerServerMongo.prototype._initializeDB = function() {
  var self = this;

  // Connect to MongoDB
  MongoClient.connect(this._options.mongo, function(err, db) {
    if (err) throw err;

    // database and collections
    self._mongo = db;
    self._mongo_client = db.collection('client');
    self._mongo_log    = db.collection('log');
    self._mongo_ip     = db.collection('ip');

    // clean up collections
    self._mongo_client.drop();
    self._mongo_log.drop();
    self._mongo_ip.drop();

    self._mongo_ip.ensureIndex({address: 1}, {w: 1}, function(err, result) {
      if (err) util.prettyError(err);
    });
  });
}

/** Adds a client to collection */
PeerServerMongo.prototype._addPeer = function(key, id, token, ip) {
  var created = PeerServer.prototype._addPeer.call(this, key, id, token, ip);

  // perform an upsert
  this._mongo_client.update({_id: id}, {$set: {token: token, ip: ip}}, {upsert: true, w: 1}, function(err, result) {
    if (err) util.prettyError(err);
  });

  /* useless?
  if (created) {
    this._mongo_ip.update({address: ip}, {$inc: {count: 1}}, {upsert: true, w: 0});
  }
  */

  return created;
};

/** Removes a client from collection */
PeerServerMongo.prototype._removePeer = function(key, id) {
  var ip = this._clients[key][id].ip;
  var removed = PeerServer.prototype._removePeer.call(this, key, id);

  // remove by id
  this._mongo_client.remove({_id: id}, {w: 1}, function(err, numberOfRemovedDocs) {
    if (err) util.prettyError(err);
  });

  /* useless?
  this._mongo_ip.update({address: ip}, {$inc: {count: -1}}, {upsert: true, w: 0});
  */

  return removed;
};

/** Handles passing on a message. */
PeerServerMongo.prototype._handleTransmission = function(key, message) {
  PeerServer.prototype._handleTransmission.call(this, key, message);

  var type = message.type;
  var src = message.src;
  var dst = message.dst;
  var log = {
    type: message.type,
     src: new ObjectID(message.src),
     dst: new ObjectID(message.dst)
  };

  this._mongo.collection('log').insert(log, {w: 0});
};

/** Generates a client ID in mongo ObjectID format */
PeerServerMongo.prototype._generateClientId = function(key) {
  var clientId = new ObjectID;
  if (!this._clients[key]) {
    return clientId;
  }
  while (!!this._clients[key][clientId]) {
    clientId = new ObjectID;
  }
  return clientId;
};

exports.PeerServerMongo = PeerServerMongo;

