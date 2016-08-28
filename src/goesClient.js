/*jslint node: true */
"use strict";

var zmq = require('zmq'),
  uuid = require('uuid'),
  util = require('util'),
  events = require('events');

var uuidRegex = /^([0-9a-f]{8}-?)([0-9a-f]{4}-?){3}([0-9a-f]{12})$/i;

function GoesClient(addr) {
  events.EventEmitter.call(this);
  this._socket = zmq.socket('req');
  this._socket.connect(addr);
  this._responseHandlers = [];
  this._types = {};
  this._socket.on('message', this._handleIncomingMessage.bind(this));
}
util.inherits(GoesClient, events.EventEmitter);

GoesClient.prototype._handleIncomingMessage = function () {
  var handler = this._responseHandlers.shift();
  if (handler === undefined) {
    return this.emit('error', new Error('Handler is missing for incoming response.'))
  }
  handler.handler(null, Array.prototype.slice.call(arguments), handler.cb);
};

GoesClient.prototype._defaultHandler = function (err, response) {
  if (err) {
    return this.emit('error', err);
  }
};

GoesClient.prototype.registerTypes = function (types) {
  Array.prototype.forEach.call(arguments, this._registerType.bind(this));
};

GoesClient.prototype._registerType = function (type) {
  if (typeof type !== 'function') throw new TypeError('type must be a function not ' + typeof type);
  this._types[type.name] = type;
};

GoesClient.prototype.close = function () {
  this._socket.close();
};

GoesClient.prototype._handleAddResponse = function (err, frames, cb) {
  if (err) {
    return cb(err);
  }
  if (frames.length !== 1) {
    return cb(new Error(['Invalid number of frames in the message. Expected 1 got', frames.length, '.'].join(' ')));
  }
  var responseText = frames[0].toString();
  if (responseText === 'Ok') {
    return cb();
  }
  if (responseText.indexOf('Error:') === 0) {
    return cb(ServerError.fromResponse(responseText));
  }
  cb(new Error(['Invalid response: ', responseText, '.'].join('')));
};

GoesClient.prototype._handleReadResponse = function (err, frames, cb) {
  if (err) {
    return cb(err);
  }

  if (frames.length < 1) {
    return cb(new Error('Empty message. Expecting at least 1 frame.'));
  }

  var rawExpectedCount = frames.shift().toString(),
    expectedCount = parseInt(rawExpectedCount);
  if (isNaN(expectedCount)) {
    if (rawExpectedCount.indexOf('Error:') === 0) {
      return cb(ServerError.fromResponse(rawExpectedCount));
    }
    return cb(new Error('Invalid response: ' + rawExpectedCount));
  }

  if (expectedCount * 2 !== frames.length) {
    return cb(new Error(['Incomplete response. Expected', expectedCount, 'events and metadata, message contains', frames.length / 2, '.'].join(' ')));
  }

  var events = [];
  for (var index = 0; index < frames.length;) {
    var evtFrame = parseFrame(frames[index++]);
    var ev = evtFrame.obj;
    var type = this._types[evtFrame.typeId];
    if (type) {
      ev.__proto__ = type.prototype;
    } else {
      //TODO: remove me - not required anymore, left for backward compatibility
      ev.$type = evtFrame.typeId;
    }
    var metadataFrame = parseFrame(frames[index++]);
    events.push({
      creationTime: 0,
      typeId: evtFrame.typeId,
      event: ev,
      metadata: metadataFrame.obj
    });
  }
  cb(null, events);
};

function parseFrame(frame) {
  var str = frame.toString();
  var sep = str.indexOf(' ');
  var typeId = str.substr(0, sep);
  var json = str.substr(sep + 1);
  return {
    typeId: typeId,
    obj: JSON.parse(json)
  };
}

/**
 * Add an event to a stream
 * @param {string} streamId         The stream UUID
 * @param {number} expectedVersion  The expected version
 * @param {Object} event            The event object
 * @param {Object} metadata         The event metadata
 * @param {string} [eventType]      The event type
 * @param {Function<?Error>} [cb]   Node-like callback (err)
 */
GoesClient.prototype.addEvent = function (streamId, expectedVersion, event, metadata, eventType, cb) {
  if (typeof eventType === 'function') {
    cb = eventType;
    eventType = null;
  }
  cb = cb || this._defaultHandler.bind(this);

  if (typeof streamId !== 'string') {
    return cb(new TypeError('streamId MUST be a string.'));
  }
  if (!uuidRegex.test(streamId)) {
    return cb(new Error('streamId MUST be formatted as a UUID.'));
  }
  if (typeof expectedVersion !== 'number') {
    return cb(new TypeError('expectedVersion must be a number.'));
  }
  if (typeof event !== 'object') {
    return cb(new TypeError('event MUST be an object.'));
  }
  if (typeof metadata !== 'object') {
    return cb(new TypeError('metadata MUST be an object.'));
  }
  eventType = eventType || event.constructor.name;
  if (typeof eventType !== 'string') {
    return cb(new TypeError('eventType MUST be a string.'));
  }
  if (eventType.toLowerCase() === 'object') {
    return cb(new Error('You need to specify an eventType when using anonymous object.'));
  }

  this._responseHandlers.push({handler: this._handleAddResponse.bind(this), cb: cb});

  var self = this,
    cmd = 'AddEvent_v2',
    streamUuid = new Buffer(uuid.parse(streamId)),
    expectedVersionBytes = new Buffer(4),
    serializedEvent = [eventType, JSON.stringify(event)].join(' '),
    serializedMetadata = ['Metadata', JSON.stringify(metadata)].join(' ');
  expectedVersionBytes.writeUInt32LE(expectedVersion, 0);
  this._socket.send([cmd, Buffer.concat([streamUuid, expectedVersionBytes]), serializedEvent, serializedMetadata], 0, function (err) {
    if (err) {
      self._responseHandlers.pop();
      cb(err);
    }
  });
};

/**
 * Read a stream of events
 * @param {string} streamId
 * @param {Function<?Error,?EventData[]>} cb
 */
GoesClient.prototype.readStream = function (streamId, cb) {
  cb = cb || this._defaultHandler.bind(this);

  if (typeof streamId !== 'string') {
    return cb(new TypeError('streamId MUST be a string.'));
  }
  if (!uuidRegex.test(streamId)) {
    return cb(new Error('streamId MUST be formatted as a UUID.'));
  }

  this._responseHandlers.push({handler: this._handleReadResponse.bind(this), cb: cb});

  var self = this,
    cmd = 'ReadStream_v2',
    streamUuid = new Buffer(uuid.parse(streamId));
  this._socket.send([cmd, streamUuid], 0, function (err) {
    if (err) {
      self._responseHandlers.pop();
      cb(err);
    }
  });
};

/**
 * Read all events
 * @param {Function<?Error>} cb
 */
GoesClient.prototype.readAll = function (cb) {
  cb = cb || this._defaultHandler.bind(this);

  this._responseHandlers.push({handler: this._handleReadResponse.bind(this), cb: cb});

  var self = this,
    cmd = 'ReadAll_v2';
  this._socket.send([cmd], 0, function (err) {
    if (err) {
      self._responseHandlers.pop();
      cb(err);
    }
  });
};

function ServerError(message, code) {
  Error.captureStackTrace(this, this.constructor);
  this.name = this.constructor.name;
  this.message = message;
  this.code = code;
}
ServerError.fromResponse = function (response) {
  var parts = response.split(': ');
  parts.shift();
  var code = parts.length > 1 ? parts[0] : '';
  var message = parts.join(': ');
  return new ServerError(message, code);
};

module.exports = function (addr) {
  return new GoesClient(addr)
};
module.exports.ServerError = ServerError;