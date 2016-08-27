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
  'Reading a stream that doesn\'t exist Should not throw and return an empty events array': function (test) {
    this.client.readStream(uuid.v4(), function (err, events) {
      test.ok(err == null, 'Expected no error. got ' + err);
      test.ok(events && events.length === 0, 'Expecting an empty array of events, got ' + JSON.stringify(events));
      test.done();
    });
  },
  'Adding an anonymous event without evenType Should throw an error': function (test) {
    this.client.addEvent(uuid.v4(), 0, {'abc': 123}, {}, function (err) {
      var expectedError = 'Error: You need to specify an eventType when using anonymous object.';
      test.ok(err && err.toString() === expectedError, ['Expecting "', expectedError, '", got "', err.toString(), '".'].join(''));
      test.done();
    });
  },
  'Adding an anonymous event with eventType Should not throw an error': function (test) {
    this.client.addEvent(uuid.v4(), 0, {'abc': 123}, {}, 'MyEvent', function (err) {
      test.done(err);
    });
  },
  'Adding a typed event Should not throw an error': function (test) {
    this.client.addEvent(uuid.v4(), 0, new MyEvent(123), {}, function (err) {
      test.done(err);
    });
  },
  'Adding an event with wrong expected version Should throw an error': function (test) {
    this.client.addEvent(uuid.v4(), 1, new MyEvent(123), {ez: 123}, function (err) {
      test.ok(err, 'Expected an error.');
      err && test.ok(err.code === 'WrongExpectedVersion', ['Expected WrongExpectedVersion error, got "', err.code, '"'].join(''));
      test.done();
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
      test.ok(avgPerEvent <= 20, 'Expected an avg per event of less than or equal 10ms, got ' + avgPerEvent + 'ms.');
      console.log('Took', (end - start), 'ms');
      test.done();
    }

    function addEvent(event, index) {
      client.addEvent(streamId, index, event, {}, function (err) {
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
      client.addEvent(data[0], 0, data[1], {}, function (err) {
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
      console.log('Took', (end - start), 'ms');
      test.ok(events.length === 1000);
      events.forEach(function (event) {
        test.ok(event.typeId, 'no typeId');
        test.ok(event.event, 'no event');
        test.ok(event.metadata, 'no event');
      });
      test.done(err);
    });
  },
  'Read All': function (test) {
    var start = Date.now();
    this.client.readAll(function (err, events) {
      var end = Date.now();
      console.log('Reading', events.length, 'events took', (end - start), 'ms');
      test.ok(events.length > 1000);
      events.forEach(function (event) {
        test.ok(event.typeId, 'no typeId');
        test.ok(event.event, 'no event');
        test.ok(event.metadata, 'no event');
      });
      test.done(err);
    });
  }
};
