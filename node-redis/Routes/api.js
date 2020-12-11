let express = require("express");
let Route = express();
let redis = require("../Controllers/RedisController");
const { google } = require('googleapis');
const OAuth2Data = require('../oauth.json')
const CLIENT_ID = OAuth2Data.web.client_id;
const CLIENT_SECRET = OAuth2Data.web.client_secret;
const REDIRECT_URL = OAuth2Data.web.redirect_uris;

const oAuth2Client = new google.auth.OAuth2(CLIENT_ID, CLIENT_SECRET, REDIRECT_URL);
const scopes = [
  'https://www.googleapis.com/auth/blogger',
  'https://www.googleapis.com/auth/calendar'
];
const url = oAuth2Client.generateAuthUrl({
  access_type: 'offline',
  scope: scopes
});
var authed = false;

Route.get('/', (req, res) => {
  console.log(req.headers.authorization);
  let token = req.headers.authorization;
var parts = token.split('.');
var headerBuf = new Buffer.from(parts[0], 'base64');
var bodyBuf = new Buffer.from(parts[1], 'base64');
 var header = JSON.parse(headerBuf.toString());
 var body = JSON.parse(bodyBuf.toString());
console.log(bodyBuf.toString());

if (body.aud !== '415227244625-5j07jei9nmdemhaqgmps9norg9fu0f5m.apps.googleusercontent.com' 
|| body.iss !== 'https://accounts.google.com') {
  throw 'Id token is not valid';
}else{
  console.log('Successfully authenticated');
}

})

Route.get('/oauth2callback', function (req, res) {
  const code = req.query.code
  console.log(code);
  const {tokens} = oAuth2Client.getToken(code)
  oAuth2Client.setCredentials(tokens);
  console.log("HERE");
  oAuth2Client.on('tokens', (tokens) => {
    if (tokens.refresh_token) {
      // store the refresh_token in my database!
      console.log(tokens.refresh_token);
    }
    console.log(tokens.access_token);
  });

});

// // return instructions
// Route.get("/", (req, res, next) => {
//   res.send("Node-Redis-Etag CRUD Application");
// });

// get all users
Route.get("/users", redis.get_all_users);

// add a new user
Route.post("/users", redis.add_user);

// delete a user
Route.delete("/users/:id", redis.delete_user);

// delete all
Route.delete("/users", redis.delete_all);

// get a user by id
Route.get('/users/:id', redis.get_user)

//update a user by id
Route.put('/users/:id', redis.update_user)

//modify a user by id
Route.patch('/users/:id', redis.patch_user)


module.exports = Route;
