'use strict';

const config = require('config');
const zmq = require('zmq');
const request = require('request');

const STREAMING_API = config.get('oanda.streamingApi');
const ACCOUNT_ID = config.get('oanda.accountId');
const ACCESS_TOKEN = config.get('oanda.accessToken');

const socket = zmq.socket('pub');

function init() {
  socket.bindSync(config.get('mq.uri'));

  // start streaming data
  const INSTRUMENTS = config.get('data.instruments');
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
            console.log(tick);

            // publish tick in MQ
            socket.send([
              config.get('mq.topic'),
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

