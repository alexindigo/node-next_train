var path             = require('path')
  , util             = require('util')

  , _                = require('lodash')
  , app              = require('tako')()

  // thrid party
  , leveldb          = require('leveldb')
  , geohash          = require('ngeohash')

  // settings
  , Config           =
    {
      port           : 31337,
      path           : 'static',
      index          : 'index.html',
      source         : path.join(__dirname, 'src'),
      db             : path.join(__dirname, 'data')
    }

  , DB               = {}

  ;

// process config settings
Config.host = process.env.host || process.env.npm_package_config_host;
Config.port = process.env.port || process.env.npm_package_config_port || Config.port;

Config.path = process.env.path || process.env.npm_package_config_path || Config.path;
if (Config.path[0] != '/') Config.path = path.join(__dirname, Config.path);

Config.index = process.env.index || process.env.npm_package_config_index || Config.index;
if (Config.index[0] != '/') Config.index = path.join(Config.path, Config.index);


// {{{ define routing

// api
app.route('/api/:action').json(function(req, res)
{
  // just to make it safe
  req.qs = req.qs || {};

  // res.setHeader('Access-Control-Allow-Origin', '*');
  // res.setHeader('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');

  req.on('error', function(err)
  {
    console.log(['error', err, req.url, req.method, req.qs, req.params]);
    res.end({status: 'error', data: (err.message ? err.message : err ) });
  });

  switch (req.params.action)
  {
    case 'station':
      if (req.method == 'GET')
      {
        findStation(req.qs, _.partial(actionCb, res));
      }
      else
      {
        methodNotAllowed(res);
      }
      break;

    case 'train':
      if (req.method == 'GET')
      {
        findTrain(req.qs, _.partial(actionCb, res));
      }
      else
      {
        methodNotAllowed(res);
      }
      break;

    case 'trip':
      if (req.method == 'GET')
      {
        findTrips(req.qs, _.partial(actionCb, res));
      }
      else
      {
        methodNotAllowed(res);
      }
      break;

    case 'service':
      if (req.method == 'GET')
      {
        findService(req.qs, _.partial(actionCb, res));
      }
      else
      {
        methodNotAllowed(res);
      }
      break;

    default:
      return fileNotFound(res);
  }
});

// static files + landing page
app.route('/').files(Config.index);
app.route('*').files(Config.path);

// }}}


// {{{ start server

app.httpServer.listen(Config.port, Config.host);

console.log('Listening on '+Config.host+':'+Config.port);

// }}}

// init db hanlders

initDb('stops.updb', function(err, db)
{
  DB['stops'] = db;
});

initDb('trips.updb', function(err, db)
{
  DB['trips'] = db;
});

initDb('services.updb', function(err, db)
{
  DB['services'] = db;
});

initDb('times.updb', function(err, db)
{
  DB['times'] = db;
});

// --- main thing

function findStation(params, callback)
{
  var hash;

  if (!DB['stops'])
  {
    return callback('Wait for the db.');
  }

  hash = geohash.encode(params.lat, params.lon);

  findNearest(DB['stops'], hash.substr(0, 5), null, function(err, data)
  {
    callback(err, [hash, hash.substr(0, 5), data]);
  });
}

function findTrain(params, callback)
{
  var date, time, conditions = {};

  if (!DB['times'])
  {
    return callback('Wait for the db.');
  }

  date = new Date();
  time = '1' + doubleTrouble(date.getHours()) + doubleTrouble(date.getMinutes()) + doubleTrouble(date.getSeconds());

  if (params.stop)
  {
    conditions['stop'] = params.stop;
  }

  // get services for today
  findService(null, function(err, services)
  {
    if (err) return callback(err);

    // get trips for active services
    findTrips({service: _.pluck(services, 'key')}, function(err, trips)
    {
      if (err) return callback(err);

      // add conditons
      conditions['trip'] = _.pluck(trips, 'key');

      // find nearest next stop
      findNearest(DB['times'], time, conditions, function(err, times)
      {
        callback(err, [times, date.getTimezoneOffset()]);
      });
    });
  });

  // subroutines
  function doubleTrouble(num)
  {
    if (num < 10)
    {
      return '0' + num;
    }

    return num;
  }
}

function findService(params, callback)
{
  var date, conditions = {};

  if (!DB['services'])
  {
    return callback('Wait for the db.');
  }

  date = new Date();

  conditions[['sunday','monday','tuesday','wednesday','thursday','friday','saturday'][date.getDay()]] = 1;

  findAll(DB['services'], conditions, function(err, data)
  {
    callback(err, data);
  });
}

function findTrips(params, callback)
{
  var conditions = {};

  if (!DB['trips'])
  {
    return callback('Wait for the db.');
  }

  if (params.service)
  {
    conditions['service'] = _.isArray(params.service) ? params.service : String(params.service).split(',');
  }

  findAll(DB['trips'], conditions, function(err, data)
  {
    callback(err, data);
  });
}

// --- subroutines

function initDb(file, callback)
{
  leveldb.open( path.join(Config.db, file), { create_if_missing: false }, function(err, db)
  {
    if (err) return fatalError(err, 'Error creating db file');

    callback(null, db);
  });
}

function findNearest(db, key, conditions, callback)
{
  db.iterator(function(err, iterator)
  {
    if (err) return callback(err);

    if (key)
    {
      iterator.seek(key, findNearest_worker);
    }
    else
    {
      iterator.first(findNearest_worker);
    }

    // subroutines

    function findNearest_worker(err)
    {
      if (err) return callback(err);

      // means no more?
      if (!iterator.valid()) return callback(null, {});

      iterator.current(function(err, key, value)
      {
        var data, badGuy;

        if (err) return callback(err);

        try
        {
          data = JSON.parse(value);
        }
        catch (e)
        {
          return callback(e);
        }


        // if any coditions passed check against them
        badGuy = conditions && _.any(conditions, function(test, field)
        {
          if (field in data)
          {
            if (_.isArray(test))
            {
              if (!_.contains(test, data[field]))
              {
                return true;
              }
            }
            else if (data[field] != test)
            {
              return true;
            }
          }
        });

        if (badGuy)
        {
          // try next
          iterator.next(findNearest_worker);
        }
        else
        {
          callback(null, {key: key, value: data});
        }

      });
    }

  });
}

function findAll(db, conditions, callback)
{
  var buffer = [];

  db.iterator(function(err, iterator)
  {
    if (err) return callback(err);

    iterator.first(findAll_worker);

    // subroutines
    function findAll_worker(err, data)
    {
      if (err) return callback(err);

      // means no more?
      if (!iterator.valid()) return callback(null, buffer);

      iterator.current(function(err, key, value)
      {
        var data, badGuy;

        if (err) return callback(err);

        try
        {
          data = JSON.parse(value);
        }
        catch (e)
        {
          return callback(e);
        }

        // if any coditions passed check against them
        badGuy = conditions && _.any(conditions, function(test, field)
        {
          if (field in data)
          {
            if (_.isArray(test))
            {
              if (!_.contains(test, data[field]))
              {
                return true;
              }
            }
            else if (data[field] != test)
            {
              return true;
            }
          }
        });

        if (badGuy)
        {
          // try next
          iterator.next(findAll_worker);
        }
        else
        {
          buffer.push({key: key, value: data});
          iterator.next(findAll_worker);
        }

      });
    }

  });

}

// {{{ Santa's little helpers

function fatalError(err, message)
{
  console.log(message || 'Fatal Error.', err);
  process.exit(1);
}

// generic resourse not found error
function fileNotFound(res)
{
  res.statusCode = 404;
  res.end('Resource Not Found.');
}

// generic method not allowed error
function methodNotAllowed(res)
{
  res.statusCode = 405;
  res.end('Method Not Allowed.');
}

// generic router callback
function actionCb(res, err, data)
{
  if (err) return res.end({status: 'error', data: err});

  res.end({status: 'ok', data: data});
}

// }}}


