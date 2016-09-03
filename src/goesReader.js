var fs = require('fs');
var path = require('path');
var glob = require('glob');

/**
 * @interface Logger
 */
/**
 * @function
 * @name Logger#debug
 * @param {...*} args Arguments to log
 */
/**
 * @function
 * @name Logger#info
 * @param {...*} args Arguments to log
 */

/**
 * Create a GoesStorageReader instance
 * @param {string} storagePath
 * @param {Logger} [logger]
 * @constructor
 */
function GoesStorageReader(storagePath, logger) {
  var stat = fs.statSync(storagePath);
  if (!stat.isDirectory()) {
    throw new Error('storagePath is not a directory.');
  }
  this._storagePath = storagePath;
  this._logger = logger;
}

GoesStorageReader.prototype._debugLog = function () {
  if (!this._logger) return;
  this._logger.debug.apply(this._logger, Array.prototype.slice.call(arguments));
};

var RELATIVE_PATH_REGEX = /^\/([0-9]{4})([0-9]{2})\/([0-9]{2})\/([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{9})_(.+)$/;

function rebuildDate(matches) {
  var year = parseInt(matches[1]);
  var month = parseInt(matches[2]) - 1;
  var date = parseInt(matches[3]);
  var hours = parseInt(matches[4]);
  var minutes = parseInt(matches[5]);
  var seconds = parseInt(matches[6]);
  var milliseconds = parseInt(parseInt(matches[7]) / 1000000);
  return new Date(year, month, date, hours, minutes, seconds, milliseconds);
}

GoesStorageReader.prototype._readEventDataSync = function (absolutePath) {
  var relativePath = absolutePath.substr(this._storagePath.length);
  var content = fs.readFileSync(absolutePath);
  return parseEventContent(content, relativePath);
};

function parseEventContent(content, relativePath) {
  var parts = content.toString().split('\r\n');
  var event = JSON.parse(parts[0]);
  var metadata = parts[1] ? JSON.parse(parts[1]) : {};
  var m = RELATIVE_PATH_REGEX.exec(relativePath);
  var creationTime = rebuildDate(m);
  var typeId = m[8];
  //TODO: remove me - not required anymore, left for backward compatibility
  event.$type = typeId;
  return {
    creationTime: creationTime,
    typeId: typeId,
    event: event,
    metadata: metadata
  };
}

GoesStorageReader.prototype._readEventData = function (absolutePath, cb) {
  var relativePath = absolutePath.substr(this._storagePath.length);
  fs.readFile(absolutePath, function (err, content) {
    if (err) {
      return cb(err);
    }
    return cb(null, parseEventContent(content, relativePath));
  })
};

function readTextFileAsync(path) {
  return new Promise(function (resolve, reject) {
    fs.readFile(path, function (err, content) {
      if (err) {
        return reject(err);
      }
      resolve(content.toString());
    });
  });
}

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

GoesStorageReader.prototype.getAllFor = function (filters, cb) {
  filters = filters || {};
  cb = cb || function () {
    };
  var hasDate = filters.date instanceof Date;
  var hasEventType = !!filters.eventType;
  var self = this;
  var pathsPromise;
  if (hasDate) {
    var month = filters.date.getMonth() + 1;
    var date = filters.date.getDate();
    var datePath = [self._storagePath, '/', filters.date.getFullYear(), month < 10 ? '0' : '', month, '/', date < 10 ? '0' : '', date].join('');
    pathsPromise = new Promise(function (resolve, reject) {
      fs.readdir(datePath, function (err, files) {
        if (err) return reject(err);
        resolve(files);
      });
    }).then(function (files) {
      if (!hasEventType) return files;
      var eventTypes = Array.isArray(filters.eventType) ? filters.eventType : [filters.eventType];
      return files.filter(function (file) {
        var eventType = file.split('_')[1];
        return eventTypes.indexOf(eventType) >= 0;
      });
    }).then(function (files) {
      return files.map(function (file) {
        return datePath + '/' + file;
      });
    });
  } else if (filters.eventType) {
    var eventTypes = Array.isArray(filters.eventType) ? filters.eventType : [filters.eventType];
    var readIndexesPromises = eventTypes.map(function (eventType) {
      return readTextFileAsync(self._storagePath + '/indexes/types/' + eventType)
        .catch(function (err) {
          //TODO: check type of error only return empty if file not found otherwise throw
          return '';
        });
    });
    pathsPromise = Promise
      .all(readIndexesPromises)
      .then(function (contents) {
        return contents.reduce(function (paths, content) {
          content.split('\r\n')
            .forEach(function (path) {
              if (!path) return;
              paths.push(self._storagePath + '/' + path);
            });
          return paths;
        }, []);
      })
      .then(function (paths) {
        return paths.sort();
      });
  } else {
    pathsPromise = new Promise(function (resolve, reject) {
      glob(self._storagePath + '/!(indexes)/*/*', function (err, paths) {
        if (err) return reject(err);
        resolve(paths);
      });
    });
  }
  pathsPromise
    .then(function (paths) {
      return new Promise(function (resolve, reject) {
        processBatch(paths, 500, function (path, done) {
          self._readEventData(path, done);
        }, function (errors, results) {
          if (errors) return reject(errors[0]);
          resolve(results);
        });
      });
    })
    .then(function (events) {
      cb(null, events);
    })
    .catch(function (err) {
      cb(err);
    });
};

/**
 * Get all events matching filters
 * @param {?Filters} filters                  Filter by
 * @param {function<?Error,?EventData[]>} cb  Callback on completion
 */
GoesStorageReader.prototype.getAllFor_v1 = function (filters, cb) {
  filters = filters || {};
  var dateFilter = '/!(indexes)/*/*';
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
    self._debugLog('Found', paths.length, 'results for pattern', pattern, 'in', self._storagePath);
    try {
      processBatch(paths, 2000, function (path, done) {
        self._readEventData(path, done);
      }, function (errors, results) {
        if (errors) return cb(errors[0]);
        return cb(null, results);
      });
    } catch (e) {
      cb(e);
    }
  });
};

function processAction(action, input, index, output, cb) {
  action(input, function (err, result) {
    if (err) {
      output.errors.push(err);
    } else {
      output.results[index] = result;
    }
    cb();
  });
}

function processBatch(sourceList, batchSize, action, cb, start, output) {
  start = start || 0;
  output = output || {errors: [], results: new Array(sourceList.length)};
  batchSize = Math.min(batchSize, sourceList.length - start);
  if (batchSize === 0) {
    return cb(output.errors.length ? output.errors : null, output.results);
  }
  var completed = 0;
  for (var i = start; i < start + batchSize; i++) {
    processAction(action, sourceList[i], i, output, function () {
      completed++;
      if (completed === batchSize) {
        return processBatch(sourceList, batchSize, action, cb, start + batchSize, output);
      }
    });
  }
}

/**
 * Create a GoesStorageReader instance
 * @param {string} storagePath
 * @param {Logger} [logger]
 * @constructor
 */
module.exports = function (storagePath, logger) {
  return new GoesStorageReader(storagePath, logger);
};