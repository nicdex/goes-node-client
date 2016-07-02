var goes = require('../src/index');
var uuid = require('uuid');

function MyEvent(abc) {
  this.abc = abc;
}

var aStreamId = uuid.v4();

module.exports = {
  setUp: function (cb) {
    this.client = goes.client('tcp://127.0.0.1:12345');
    cb();
  },
  tearDown: function (cb) {
    this.client.close();
    delete this.client;
    cb();
  },
  'Reading a stream that doesn\'t exist Should throw a server error': function (test) {
    this.client.readStream(uuid.v4(), function (err, events) {
      test.ok(err !== null, 'Expecting a error, got none.');
      if (err === null) {
        return test.done();
      }
      test.ok(err.message.indexOf('Server Error:') === 0, 'Expecting "Server Error:...", got "' + err.message + '"');
      test.ok(events === undefined, ['Events should be null, got ', JSON.stringify(events), '.'].join(''));
      test.done();
    });
  },
  'Adding an anonymous event without evenType Should throw an error': function (test) {
    this.client.addEvent(uuid.v4(), {'abc': 123}, function (err) {
      var expectedError = 'Error: You need to specify an eventType when using anonymous object.';
      test.ok(err && err.toString() === expectedError, ['Expecting "', expectedError, '", got "', err.toString(), '".'].join(''));
      test.done();
    });
  },
  'Adding an anonymous event with eventType Should not throw an error': function (test) {
    this.client.addEvent(uuid.v4(), {'abc': 123}, 'MyEvent', function (err) {
      test.done(err);
    });
  },
  'Adding a typed event Should not throw an error': function (test) {
    this.client.addEvent(uuid.v4(), new MyEvent(123), function (err) {
      test.done(err);
    });
  },
  'Adding 1000 events to same stream': function (test) {
    var count = 1000;
    var errors = [];
    var streamId = aStreamId;
    var client = this.client;
    var start, end;

    function done() {
      end = Date.now();
      var avgPerEvent = (end - start)/count;
      test.ok(errors.length === 0, 'Expected no error, got ' + errors.length);
      test.ok(avgPerEvent <= 10, 'Expected an avg per event of less than or equal 10ms, got ' + avgPerEvent + 'ms.');
      console.log('Took', (end - start), 'ms');
      test.done();
    }

    function addEvent(event, index) {
      client.addEvent(streamId, event, function (err) {
        if (err) {
          errors.push(err);
        }
        if (index === count - 1) {
          done();
        }
      });
    }

    var events = [];
    for(var i = 0; i < count; i++ ) {
      var v = Math.random();
      events.push(new MyEvent(v));
    }
    start = Date.now();
    events.forEach(addEvent);
  },
  'Adding 1000 events to different streams': function (test) {
    var count = 1000;
    var errors = [];
    var client = this.client;
    var start, end;

    function done() {
      end = Date.now();
      var avgPerEvent = (end - start)/count;
      test.ok(errors.length === 0, 'Expected no error, got ' + errors.length);
      test.ok(avgPerEvent <= 10, 'Expected an avg per event of less than or equal 10ms, got ' + avgPerEvent + 'ms.');
      console.log('Took', (end - start), 'ms');
      test.done();
    }

    function addEvent(data, index) {
      client.addEvent(data[0], data[1], function (err) {
        if (err) {
          errors.push(err);
        }
        if (index === count - 1) {
          done();
        }
      });
    }

    var events = [];
    for(var i = 0; i < count; i++ ) {
      var v = Math.random();
      events.push([uuid.v4(), new MyEvent(v)]);
    }
    start = Date.now();
    events.forEach(addEvent);
  },
  'Read 1000 events from stream': function (test) {
    var start = Date.now();
    this.client.readStream(aStreamId, function (err, events) {
      var end = Date.now();
      test.ok(events.length === 1000);
      console.log('Took', (end - start), 'ms');
      test.done(err);
    });
  }
};
