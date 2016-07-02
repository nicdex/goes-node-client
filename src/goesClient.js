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
  this._socket.on('message', this._handleText.bind(this));
}
util.inherits(GoesClient, events.EventEmitter);

GoesClient.prototype._handleText = function() {
  var handler = this._responseHandlers.shift();
  if (handler === undefined) {
    return this.emit('error', new Error('Handler is missing for incoming response.'))
  }
  handler(null, Array.prototype.slice.call(arguments));
};

GoesClient.prototype._defaultHandler = function(err, response) {
  if (err) {
    return this.emit('error', err);
  }
};

GoesClient.prototype.registerTypes = function(types) {
  Array.prototype.forEach.call(arguments, this._registerType.bind(this));
};

GoesClient.prototype._registerType = function(type) {
  if (typeof type !== 'function') throw new TypeError('type must be a function not ' + typeof type);
  this._types[type.name] = type;
};

GoesClient.prototype.close = function() {
  this._socket.close();
};

/**
 * Add an event to a stream
 * @param streamId    The stream UUID
 * @param event       The event object
 * @param [eventType] The event type
 * @param [cb]        Node-like callback (err)
 */
GoesClient.prototype.addEvent = function(streamId, event, eventType, cb) {
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
  if (typeof event !== 'object') {
    return cb(new TypeError('event MUST be an object.'));
  }
  eventType = eventType || event.constructor.name;
  if (typeof eventType !== 'string') {
    return cb(new TypeError('eventType MUST be a string.'));
  }
  if (eventType.toLowerCase() === 'object') {
    return cb(new Error('You need to specify an eventType when using anonymous object.'));
  }

  function handleResponse(err, msg) {
    if (err) {
      return cb(err);
    }
    if (msg.length !== 1) {
      return cb(new Error(['Invalid number of frames in the message. Expected 1 got', msg.length, '.'].join(' ')));
    }
    var responseText = msg[0].toString();
    if (responseText === 'Ok') {
      return cb();
    }
    if (responseText.indexOf('Error:') === 0) {
      return cb(new Error('Server ' + responseText));
    }
    cb(new Error(['Invalid response: ', responseText, '.'].join('')));
  }

  this._responseHandlers.push(handleResponse);

  var self = this,
      cmd = 'AddEvent',
      streamUuid = new Buffer(uuid.parse(streamId)),
      serializedEvent = [eventType, JSON.stringify(event)].join(' ');
  this._socket.send([cmd, streamUuid, serializedEvent], 0, function(err) {
    if (err) {
      self._responseHandlers.pop();
      cb(err);
    }
  });
};

GoesClient.prototype._responseHandlerFor = function(cb) {
  var self = this;
  return function(err, msg) {
    if (err) {
      return cb(err);
    }

    if (msg.length < 1) {
      return cb(new Error('Empty message. Expecting at least 1 frame.'));
    }

    var rawExpectedCount = msg.shift().toString(),
        expectedCount = parseInt(rawExpectedCount);
    if (isNaN(expectedCount)) {
      var errorMessage;
      if (rawExpectedCount.indexOf('Error:') === 0) {
        errorMessage = 'Server ' + rawExpectedCount;
      } else {
        errorMessage = 'Invalid response: ' + rawExpectedCount;
      }
      return cb(new Error(errorMessage));
    }

    if (expectedCount !== msg.length) {
      return cb(new Error(['Incomplete response. Expected', expectedCount, 'events, message contains', msg.length, '.'].join(' ')));
    }

    var events = msg.map(function(m) {
      var s = m.toString(),
          indexOfSep = s.indexOf(' '),
          typeId = s.substr(0, indexOfSep),
          json = s.substr(indexOfSep);
      var ev = JSON.parse(json);
      var type = self._types[typeId];
      if (type) {
        ev.__proto__ = type.prototype;
      } else {
        ev.$type = typeId;
      }
      return ev;
    });
    cb(null, events);
  }
};

GoesClient.prototype.readStream = function(streamId, cb) {
  cb = cb || this._defaultHandler.bind(this);

  if (typeof streamId !== 'string') {
    return cb(new TypeError('streamId MUST be a string.'));
  }
  if (!uuidRegex.test(streamId)) {
    return cb(new Error('streamId MUST be formatted as a UUID.'));
  }

  this._responseHandlers.push(this._responseHandlerFor(cb).bind(this));

  var self = this,
      cmd = 'ReadStream',
      streamUuid = new Buffer(uuid.parse(streamId));
  this._socket.send([cmd, streamUuid], 0, function(err) {
    if (err) {
      self._responseHandlers.pop();
      cb(err);
    }
  });
};

GoesClient.prototype.readAll = function(cb) {
  cb = cb || this._defaultHandler.bind(this);

  this._responseHandlers.push(this._responseHandlerFor(cb).bind(this));

  var self = this,
      cmd = 'ReadAll';
  this._socket.send([cmd], 0, function(err) {
    if (err) {
      self._responseHandlers.pop();
      cb(err);
    }
  });
};


module.exports = function(addr) {
  return new GoesClient(addr)
};