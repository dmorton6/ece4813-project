var mongojs = require('mongojs');
var db = mongojs('mongodb://localhost:27017/main_db', ['songs']);
var express = require('express');

module.exports = function(app) {

  var router = express.Router();

  // api/songs
  router.get('/api/songs', function(req, res) {
    db.songs.find(function(err, docs) {
      res.json(docs);
    });
  });

  // api/songs/:song_id
  router.get('/api/songs/:song_id', function(req, res) {
    var id = req.params.song_id;

    db.songs.findOne({
      _id: mongojs.ObjectId(id)
    }, function(err, doc) {
      res.json(doc);
    });
  });

  app.use(router);
}
