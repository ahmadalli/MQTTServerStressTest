"use strict";

const mqtt = require('mqtt');

const config = require("./configs");

const usersCount = config.test.usersCount;
const usersPergroup = config.test.usersPergroup;
const userMessagePerSecond = config.test.userMessagePerSecond;

const userMessageInterval = 1000 / userMessagePerSecond;
const groupsCount = usersCount / usersPergroup;

console.log(`user message interval: ${userMessageInterval}`);
console.log(`count of groups: ${groupsCount}`);

for (var groupId = 0; groupId < groupsCount; groupId++) {
    const groupTopic = `test/group/${groupId}`;
    for (var userId = groupId * usersPergroup; userId < (groupId + 1) * usersPergroup; userId++) {
        runUserClient(userId, groupTopic);
    }
}

var shouldLog = true;

function runUserClient(userId, groupTopic) {
    const userClient = mqtt.connect(`mqtt://${config.host}:${config.port}`, {
        clientId: `node-stress-test-${userId}`,
        clean: false
    });

    var retryCount = 0;

    var shouldSetInterval = true

    userClient.on('connect', function () {
        console.log(`user ${userId} connected`);
        userClient.subscribe(groupTopic);
        console.log(`user ${userId} subscribed to group topic ${groupTopic}`);

        var messageNumber = 1;
        if (shouldSetInterval) {
            setInterval(function () {
                const message = `from ${userId} - ${messageNumber}th message`;
                userClient.publish(groupTopic, message, {
                    qos: 0
                });
                messageNumber++;
            }, userMessageInterval);
            shouldSetInterval = false;
        }

        retryCount = 0;
    });

    userClient.on('reconnect', function () {
        console.log(`user ${userId} reconnecting`);
    });

    userClient.on('offline', function () {
        console.log(`user ${userId} offline`);
    });

    // userClient.on('message', function (topic, message) {
    //     console.log(`user ${userId} received message: ${message}`);
    // });

    userClient.on('error', function (err) {
        console.log(`user ${userId} error: ${err}`);
    });
}