var util             = require('util')
  , path             = require('path')
  , fs               = require('fs')

  // third-party
  , _                = require('lodash')
  , rimraf           = require('rimraf')
  , csv              = require('csv')
  , leveldb          = require('leveldb')
  , geohash          = require('ngeohash')

  , Config           =
    {
      source         : path.join(__dirname, 'src'),
      db             : path.join(__dirname, 'data')
    }

  , csvOptions       =
    {
      encoding       : 'utf8',
      columns        : true,
      trim           : true
    }

  , dbHandlers       = {}
  ;


// define process workers
var worker =
{
  stops: function(db, data, index)
  {
    var struct = {}
      , hash   = geohash.encode(data.stop_lat, data.stop_lon)
      , handle = makeHandle(data.stop_id)
      , dataToStore
      ;

    dataToStore =
    {
      geo    : hash,
      handle : handle,
      name   : data.stop_id.replace('Caltrain', '').trim(),
      full   : data.stop_id,
      address: data.stop_desc,
      lat    : data.stop_lat,
      lon    : data.stop_lon,
      zone   : data.zone_id,
      order  : index
    };

    // index by geohash
    struct[hash] = dataToStore;

    // index by handle
//    struct['handle:'+handle] = dataToStore;

    // store it
    saveRecord(db, struct, function(err)
    {
      console.log(['Saved #' + index + ' ' + handle]);
    });
  },

  trips: function(db, data, index)
  {
    var struct = {}
      , id     = data.trip_id
      , train  = data.trip_headsign.replace(/^.*Train ([0-9]+).*$/, '$1').trim()
      , dataToStore
      ;

    dataToStore =
    {
      id       : id,
      train    : train,
      type     : data.route_id.replace(/ct_([a-z])_[0-9]/, '$1'),
      service  : data.service_id,
      end_point: data.trip_headsign.replace(/^(.*)\(Train.*$/, '$1').trim(),
      direction: data.direction_id,
      order    : index
    };

    // index by id
    struct[id] = dataToStore;

    // index by id
//    struct['train:'+train] = dataToStore;

    // store it
    saveRecord(db, struct, function(err)
    {
      console.log(['Saved #' + index + ' ' + id]);
    });
  },

  times: function(db, data, index)
  {
    var struct = {}
      , trip   = data.trip_id
      , stop   = makeHandle(data.stop_id)
      , time   = '1' + data.departure_time.replace(/:/g, '').trim()
      , dataToStore
      ;

    dataToStore =
    {
      trip          : trip,
      stop          : stop,
      time          : time,
      arrival_time  : data.arrival_time,
      departure_time: data.departure_time,
      sequence      : data.stop_sequence,
      order         : index
    };

    // index by time
    struct[time] = dataToStore;

    // store it
    saveRecord(db, struct, function(err)
    {
      console.log(['Saved #' + index + ' ' + time]);
    });
  },

  services: function(db, data, index)
  {
    var struct  = {}
      , service = data.service_id
      , dataToStore
      ;

    dataToStore =
    {
      service  : service,
      monday   : data.monday,
      tuesday  : data.tuesday,
      wednesday: data.wednesday,
      thursday : data.thursday,
      friday   : data.friday,
      saturday : data.saturday,
      sunday   : data.sunday,
      order    : index
    };

    // index by time
    struct[service] = dataToStore;

    // store it
    saveRecord(db, struct, function(err)
    {
      console.log(['Saved #' + index + ' ' + service]);
    });
  }
};


// process stations
initDb('stops.updb', function(err, db)
{
  processCsv('stops.txt', _.partial(worker.stops, db), function(err, count)
  {
    if (err) return fatalError(err, 'Couldn\'t process csv file [stops.txt]');

    console.log(['Done [stops.txt]', count]);
  });

});

// process trains
initDb('trips.updb', function(err, db)
{
  processCsv('trips.txt', _.partial(worker.trips, db), function(err, count)
  {
    if (err) return fatalError(err, 'Couldn\'t process csv file [trips.txt]');

    console.log(['Done [trips.txt]', count]);
  });

});

// process stop times
initDb('times.updb', function(err, db)
{
  processCsv('stop_times.txt', _.partial(worker.times, db), function(err, count)
  {
    if (err) return fatalError(err, 'Couldn\'t process csv file [stop_times.txt]');

    console.log(['Done [stop_times.txt]', count]);
  });

});

// process stop times
initDb('services.updb', function(err, db)
{
  processCsv('calendar.txt', _.partial(worker.services, db), function(err, count)
  {
    if (err) return fatalError(err, 'Couldn\'t process csv file [calendar.txt]');

    console.log(['Done [calendar.txt]', count]);
  });

});

// subroutines

function initDb(file, callback)
{
  filename = path.join(Config.db, file);
  rimraf.sync(filename);

  leveldb.open(filename, { create_if_missing: true }, function onOpen(err, db)
  {
    if (err) return fatalError(err, 'Error creating db file');

    callback(null, db);
  });
}

function processCsv(file, handler, callback)
{
  var stream = csv().from.path( path.join(Config.source, file), csvOptions);

  stream
    .on('record', handler)
    .on('end', function(count)
    {
      callback(null, count);
    })
    .on('error', function(err)
    {
      callback(err);
    });

  return;
}

function saveRecord(db, data, callback)
{
  var ops = [];

  _.forEach(data, function(v, k)
  {
    db.put(k, JSON.stringify(v), function(err)
    {
      if (err) return fatalError(err, 'Error writing to db file');

      callback(null);
    });

    // ops.push(
    // {
    //   type: 'put',
    //   key: k,
    //   value: JSON.stringify(v)
    // });
  });

  // db.batch(ops, function (err)
  // {
  //   if (err) return fatalError(err, 'Error writing to db file');

  //   callback(null);
  // });

  return;
}

function fatalError(err, message)
{
  console.log(message || 'Fatal Error.', err);
  process.exit(1);
}

function makeHandle(name)
{
  return String(name).toLowerCase().replace(/[^a-z0-9-]/g, '_').replace(/[_]{2,}/, '_');
}



function show(data)
{
  return util.inspect(data, false, 4, true);
}


