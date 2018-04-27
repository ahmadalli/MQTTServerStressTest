const config = require("./configs");

const mqtt = require('mqtt');

const userClient = mqtt.connect(`mqtt://${config.host}:${config.port}`, {
    clientId: `stress-test-watcher`,
    clean: false
});

userClient.on('connect', function () {
    console.log(`watcher connected`);
    userClient.subscribe("#");
    console.log(`watcher subscribed to all topics`);
});

userClient.on('message', function (topic, message) {
    console.log(`received message: ${message}`);
});

userClient.on('reconnect', function () {
    console.log(`watcher reconnecting`);
});

userClient.on('offline', function () {
    console.log(`watcher offline`);
});

userClient.on('error', function (err) {
    console.log(`watcher error: ${err}`);
});