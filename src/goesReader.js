var fs = require('fs');
var path = require('path');
var glob = require('glob');

function GoesStorageReader(storagePath) {
  var stat = fs.statSync(storagePath);
  if (!stat.isDirectory()) {
    throw new Error('storagePath is not a directory.');
  }
  this._storagePath = storagePath;
}

var RELATIVE_PATH_REGEX = /^\/([0-9]{4})([0-9]{2})\/([0-9]{2})\/([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{9})_(.+)$/;

function rebuildDate(matches) {
  var year = parseInt(matches[1]);
  var month = parseInt(matches[2])-1;
  var date = parseInt(matches[3]);
  var hours = parseInt(matches[4]);
  var minutes = parseInt(matches[5]);
  var seconds = parseInt(matches[6]);
  var milliseconds = parseInt(parseInt(matches[7]) / 1000000);
  return new Date(year, month, date, hours, minutes, seconds, milliseconds);
}

GoesStorageReader.prototype._readEventSync = function(absolutePath) {
  var relativePath = absolutePath.substr(this._storagePath.length);
  var content = fs.readFileSync(absolutePath);
  var parts = content.toString().split('\r\n');
  var event = JSON.parse(parts[0]);
  var metadata = parts[1] ? JSON.parse(parts[1]) : null;
  var m = RELATIVE_PATH_REGEX.exec(relativePath);
  var creationTime = rebuildDate(m);
  var typeId = m[8];
  return {
    creationTime: creationTime,
    typeId: typeId,
    event: event,
    metadata: metadata
  };
};

/**
 * @typedef {Object} EventData
 * @property {number} creationTime
 * @property {string} typeId
 * @property {Object} event
 * @property {Object} metadata
 */

/**
 * @typedef {Object} Filters
 * @property {string|string[]} eventType
 * @property {Date} date
 */

/**
 * Get all events matching filters
 * @param {?Filters} filters                  Filter by
 * @param {function<?Error,?EventData[]>} cb  Callback on completion
 */
GoesStorageReader.prototype.getAllFor = function (filters, cb) {
  filters = filters || {};
  var dateFilter = '/**/*';
  if (filters.date instanceof Date) {
    var month = filters.date.getMonth() + 1;
    var date = filters.date.getDate();
    dateFilter = ['/', filters.date.getFullYear(), month < 10 ? '0' : '', month, '/', date < 10 ? '0' : '', date, '/*'].join('')
  }
  var pattern = this._storagePath + dateFilter;
  if (filters.eventType) {
    var eventTypes = Array.isArray(filters.eventType) ? filters.eventType : [filters.eventType];
    var eventTypePatterns = eventTypes.join('|');
    pattern += '_@(' + eventTypePatterns + ')';
  }
  var self = this;
  glob(pattern, function (err, paths) {
    if (err) {
      return cb(err);
    }
    try {
      var events = paths.map(self._readEventSync, self);
      cb(null, events);
    } catch(e) {
      cb(e);
    }
  });
};

module.exports = function(storagePath) {
  return new GoesStorageReader(storagePath);
};