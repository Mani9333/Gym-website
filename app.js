const express = require("express");
const path = require("path"); 
const app = express();
const mongoose = require('mongoose');
const bodyparser = require("body-parser");

mongoose.connect('mongodb://127.0.0.1:27017/manitest1', {useNewUrlParser: true});
const port = 8000;


// Define mongoose schema
var contactSchema = new mongoose.Schema({
    name: String,
    phone: String,
    email: String,
    address: String,
    desc: String
  });

var Contact = mongoose.model('Contact', contactSchema);

var signupSchema = new mongoose.Schema({
    name: {type : String, required : true},
    email: {type : String, required : true},
    password: {type : String, required : true}
  });

var Contact = mongoose.model('Contact', contactSchema);
var Signup = mongoose.model('Signup', signupSchema);

// EXPRESS SPECIFIC STUFF
app.use('/static', express.static('static')) // For serving static files
app.use(express.urlencoded())

// PUG SPECIFIC STUFF
app.set('view engine', 'pug') // Set the template engine as pug
app.set('views', path.join(__dirname, 'views')) // Set the views directory
 
// ENDPOINTS
app.get('/', (req, res)=>{ 
    const params = { }
    res.status(200).render('login.pug', params);
})

app.get('/signup', (req, res)=>{ 
    const params = { }
    res.status(200).render('signup.pug', params);
})

app.get('/contact', (req, res)=>{ 
    const params = { }
    res.status(200).render('contact.pug', params);
})

app.post('/contact', (req, res)=>{ 
    var myData = new Contact(req.body);
    myData.save().then(()=>{
        res.send("This item has been saved to the database")
    }).catch(()=>{
        res.status(400).send("Item was not saved to the database")
    });
})
const User = mongoose.model('Signups', {
    name: { type: String },
    password: { type: String }
});


app.post('/login', async (req, res) => {

    try {
        const check = await Signup.findOne({ name: req.body.name });
        console.log(check)
        if (check.password === req.body.password) {
            res.status(201).render("home.pug")
        }

        else {
            res.send("incorrect password")
        }
    } catch (err) {
        console.error('Error querying user:', err);
        res.status(500).send('Server error');
    }
});

app.post('/signup', (req, res)=>{ 
    var myData = new Signup(req.body);
    myData.save().then(()=>{
        res.send("This item has been saved to the database")
    }).catch(()=>{
        res.status(400).send("Item was not saved to the database")
    });
})

// START THE SERVER
app.listen(port, ()=>{
    console.log(`The application started successfully on port ${port}`);
});