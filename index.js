'use strict';

const config = require('config');
const zmq = require('zmq');
const request = require('request');

// configured constants
const STREAMING_API = config.get('oanda.streamingApi');
const ACCOUNT_ID = config.get('oanda.accountId');
const ACCESS_TOKEN = config.get('oanda.accessToken');
const INSTRUMENTS = config.get('data.instruments');

const socketEngine = zmq.socket('pub');
const socketSaver = zmq.socket('pub');

function init() {
  socketEngine.connect(config.get('mq.engine.uri'));
  socketSaver.connect(config.get('mq.saver.uri'));

  // start streaming data
  stream(INSTRUMENTS);
}

function stream(instruments) {
  const instrumentsParam = typeof instruments === 'string' ?
    instruments :
    instruments.join(',');

  const requestOptions = {
    url: `${STREAMING_API}/v1/prices?accountId=${ACCOUNT_ID}&instruments=${instruments}`,
    headers: {
      Authorization: `Bearer ${ACCESS_TOKEN}`
    }
  };

  request(requestOptions)
    .on('data', data => {
      // format buffer data
      const str = String(data);

      // slice away last \n
      const tickStrs = str.slice(0,-1).split('\n');

      // try to parse and process ticks
      try {
        tickStrs.forEach(s => {
          const tickObj = JSON.parse(s);
          if(tickObj.tick) {
            const tick = tickObj.tick;
            tick.source = 'oanda';
            tick.time = new Date(tick.time);

            console.log('received - ' + tick.instrument);

            // send to engine
            socketEngine.send([
              config.get('mq.engine.topic'),
              JSON.stringify(tick)
            ]);
            // send to saver
            socketSaver.send([
              config.get('mq.saver.topic'),
              JSON.stringify(tick)
            ]);
          }
        });

      } catch(err) {
        console.log('ERROR: ./index.js - parsing data');
        console.error(err);
      }
    });
}

init();

