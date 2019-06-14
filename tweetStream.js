require('dotenv').config();
const Twit = require('twit');
const debug = require('debug')('twitter');

const {
  CONSUMER_KEY,
  CONSUMER_SECRET,
  ACCESS_TOKEN,
  ACCESS_TOKEN_SECRET,
} = process.env;

const T = new Twit({
  consumer_key: CONSUMER_KEY,
  consumer_secret: CONSUMER_SECRET,
  access_token: ACCESS_TOKEN,
  access_token_secret: ACCESS_TOKEN_SECRET,
});

const bitcoinStream = T.stream('statuses/filter', { track: '#bitcoin', language: 'en' })

bitcoinStream.on('tweet', function (tweet) {
  debug(tweet.text)
})