var express = require('express');
var hbs = require('express3-handlebars');
var mongojs = require('mongojs');
var db = mongojs('mongodb://52.5.145.180:27017/main_db', ['songs_v2', 'charts']);
var app = express();

// Serve static files and
app.use(express.static(__dirname + '/public'));
app.engine('hbs',hbs({extname:'hbs'}));
app.set('view engine','hbs');

// Render views.
app.get('/', function(req,res) {
  db.songs_v2.find({}, function(err, docs) {
    res.render('index', {
      songs: docs
    });  
  });
});

app.get('/songs/:song_id', function(req,res) {
  var id = req.params.song_id;

  db.songs_v2.findOne({_id: mongojs.ObjectId(id)}, function(err, song) {
    res.render('song', {
      song: song
    });  
  });
});

app.get('/charts', function(req, res) {
  db.charts.find({}, function(err, docs) {
    res.render('charts', {
      charts: docs
    });  
  });
});

// Route API.
require('./api')(app);

// Create server.
app.listen(80);
console.log('Server started at port 80');
