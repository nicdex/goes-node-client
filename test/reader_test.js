var goes = require('../index');
var uuid = require('uuid');
var glob = require('glob');
var fs = require('fs');

function MyEvent(abc) {
  this.abc = abc;
}

module.exports = {
  setUp: function (cb) {
    var path = process.env['TEMP'] + '/events';
    this.reader = goes.reader(path);
    cb();
  },
  'GetAllFor all': function (test) {
    var start = Date.now();
    this.reader.getAllFor({date: null, eventType: null}, function (err, events) {
      if (!events) return test.done(err);
      console.log('Reading', events.length, 'events took', Date.now()-start, 'ms');
      test.ok(events.length > 0, "Expected some events, got 0.");
      events.forEach(function (data) {
        test.ok(data.creationTime instanceof Date, 'missing creationTime');
        test.ok(data.typeId === 'MyEvent', 'wrong typeId');
        test.ok(typeof data.event === 'object' && data.event !== null, 'missing payload');
      });
      test.done(err);
    });
  },
  'GetAllFor today': function (test) {
    var start = Date.now();
    this.reader.getAllFor({date: new Date(), eventType: null}, function (err, events) {
      if (!events) return test.done(err);
      console.log('Reading', events.length, 'events took', Date.now()-start, 'ms');
      test.ok(events.length > 0, "Expected some events, got 0.");
      events.forEach(function (data) {
        test.ok(data.creationTime instanceof Date, 'missing creationTime');
        test.ok(data.typeId === 'MyEvent', 'wrong typeId');
        test.ok(typeof data.event === 'object' && data.event !== null, 'missing payload');
      });
      test.done(err);
    });
  },
  'GetAllFor MyEvent': function (test) {
    var start = Date.now();
    this.reader.getAllFor({date: null, eventType: 'MyEvent'}, function (err, events) {
      if (!events) return test.done(err);
      console.log('Reading', events.length, 'events took', Date.now()-start, 'ms');
      test.ok(events.length > 0, "Expected some events, got 0.");
      events.forEach(function (data) {
        test.ok(data.creationTime instanceof Date, 'missing creationTime');
        test.ok(data.typeId === 'MyEvent', 'wrong typeId');
        test.ok(typeof data.event === 'object' && data.event !== null, 'missing payload');
      });
      test.done(err);
    });
  }
};
