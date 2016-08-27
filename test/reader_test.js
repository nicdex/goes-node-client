var goes = require('../src/index');
var uuid = require('uuid');

function MyEvent(abc) {
  this.abc = abc;
}

var aStreamId = uuid.v4();

module.exports = {
  setUp: function (cb) {
    this.reader = goes.reader(process.env['TEMP'] + '/events');
    cb();
  },
  'GetAllFor today': function (test) {
    var start = Date.now();
    this.reader.getAllFor({date: new Date(), eventType: null}, function (err, events) {
      test.ok(events.length > 0, "Expected some events, got 0.");
      console.log('Reading', events.length, 'events took', Date.now()-start, 'ms');
      test.done(err);
    });
  },
  'GetAllFor MyEvent': function (test) {
    var start = Date.now();
    this.reader.getAllFor({date: null, eventType: 'MyEvent'}, function (err, events) {
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
