var express = require('express');
var app = express();

// Serve static files.
app.use(express.static(__dirname + '/public'));

// Route API.
require('./api')(app);
app.engine('hbs',hbs({extname:'hbs', defaultLayout:'main.hbs'}));
app.set('view engine','hbs');

app.get('/:id',function(req,res){
        res.send("I am the song with song id"+req.params.id);
})

// Create server.
app.listen(80);
console.log('Server started at port 80');
