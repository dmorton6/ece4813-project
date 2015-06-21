var mongojs = require('mongojs');
var db = mongojs('mongodb://52.5.145.180:27017:27017/main_db', ['songs_v2', 'charts']);
var express = require('express');

module.exports = function(app) {
  
  var router = express.Router();

  // api/songs
  router.get('/api/songs', function(req, res) {
    db.songs_v2.find(function(err, docs) {
      res.json(docs);
    });
  });

  // api/songs/:song_id
  router.get('/api/songs/:song_id', function(req, res) {
    var id = req.params.song_id;

    db.songs_v2.findOne({
      _id: mongojs.ObjectId(id)
    }, function(err, doc) {
      res.json(doc);
    });
  });

  // api/charts
  router.get('/api/charts', function(req, res) {
    db.charts.find(function(err, docs) {
      res.json(docs);
    });
  });

  // api/charts/:year
  router.get('/api/charts/:year', function(req, res) {
    var year = req.params.year;

    db.songs.findOne({
      year: year
    }, function(err, doc) {
      res.json(doc);
    });
  });

  app.use(router);
}