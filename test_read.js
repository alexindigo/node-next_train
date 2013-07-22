var util             = require('util')
  , path             = require('path')
  , fs               = require('fs')

  // third-party
  , leveldb          = require('levelup')

  , Config           =
    {
      db             : path.join(__dirname, 'data_test')
    }
  ;


// process stations
readDb('stops.updb');

// process trains
readDb('trips.updb');


function initDb(file, callback)
{
  filename = path.join(Config.db, file);

  leveldb(filename, null, function onOpen(err, db)
  {
    if (err) return fatalError(err, 'Error opening db file');

    callback(null, db);
  });
}

function readDb(file)
{
  initDb(file, function(err, db)
  {
    db.readStream()
    .on('data', function (data) {
      console.log(data.key, '=', data.value)
    })
    // .on('error', function (err) {
    //   console.log('Oh my!', err)
    // })
    // .on('close', function () {
    //   console.log('Stream closed')
    // })
    .on('end', function () {
      console.log('Stream ended')
    });

  });
}


