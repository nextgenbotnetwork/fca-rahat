/* fca-azadx69x */
"use strict";
// Fixed by @Azadx69x
var utils = require("../utils");
var log = require("npmlog");
var mqtt = require('mqtt');
var websocket = require('websocket-stream');
var HttpsProxyAgent = require('https-proxy-agent');
const EventEmitter = require('events');
const debugSeq = false;
var identity = function () { };
var form = {};
var getSeqID = function () { };
var topics = [
    "/legacy_web",
    "/webrtc",
    "/rtc_multi",
    "/onevc",
    "/br_sr",
    "/sr_res",
    "/t_ms",
    "/thread_typing",
    "/orca_typing_notifications",
    "/notify_disconnect",
    "/orca_presence",
    "/inbox",
    "/mercury",
    "/messaging_events",
    "/orca_message_notifications",
    "/pp",
    "/webrtc_response",
];

// ====== ANTI-SUSPENSION HELPERS ======
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function getRandomUserAgent() {
    const userAgents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
    ];
    return userAgents[getRandomInt(0, userAgents.length - 1)];
}

function getRandomDelay() {
    // Random delay between 5-15 seconds for reconnection
    return getRandomInt(5000, 15000);
}

function listenMqtt(defaultFuncs, api, ctx, globalCallback) {
    // Don't start if not logged in
    if (ctx.loggedIn === false) {
        log.info("listenMqtt", "Not starting - logged out");
        return;
    }
    
    var chatOn = ctx.globalOptions.online;
    var foreground = true;

    var sessionID = Math.floor(Math.random() * 9007199254740991) + 1;
    var GUID = utils.getGUID();
    
    const username = {
        u: ctx.userID,
        s: sessionID,
        chat_on: chatOn,
        fg: foreground,
        d: GUID,
        ct: 'websocket',
        aid: '219994525426954',
        aids: null,
        mqtt_sid: '',
        cp: 3,
        ecp: 10,
        st: [],
        pm: [],
        dc: '',
        no_auto_fg: false,
        gas: null,
        pack: [],
        p: null,
        php_override: ""
    };
    
    var cookies = ctx.jar.getCookies("https://www.facebook.com").join("; ");

    var host;
    if (ctx.mqttEndpoint) host = `${ctx.mqttEndpoint}&sid=${sessionID}&cid=${GUID}`;
    else if (ctx.region) host = `wss://edge-chat.facebook.com/chat?region=${ctx.region.toLocaleLowerCase()}&sid=${sessionID}&cid=${GUID}`;
    else host = `wss://edge-chat.facebook.com/chat?sid=${sessionID}&cid=${GUID}`;

    const currentUserAgent = ctx.globalOptions.userAgent || getRandomUserAgent();

    const options = {
        clientId: 'mqttwsclient',
        protocolId: 'MQIsdp',
        protocolVersion: 3,
        username: JSON.stringify(username),
        clean: true,
        wsOptions: {
            headers: {
                'Cookie': cookies,
                'Origin': 'https://www.facebook.com',
                'User-Agent': currentUserAgent,
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.facebook.com/',
                'Host': new URL(host).hostname,
                'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits',
                'Sec-WebSocket-Version': '13',
            },
            origin: 'https://www.facebook.com',
            protocolVersion: 13,
            binaryType: 'arraybuffer',
        },
        keepalive: 60,
        reschedulePings: true,
        reconnectPeriod: 0, // Disable auto-reconnect, we handle it manually
        connectTimeout: 30000,
    };

    if (typeof ctx.globalOptions.proxy != "undefined") {
        var agent = new HttpsProxyAgent(ctx.globalOptions.proxy);
        options.wsOptions.agent = agent;
    }

    try {
        ctx.mqttClient = new mqtt.Client(_ => websocket(host, options.wsOptions), options);
        global.mqttClient = ctx.mqttClient;
    } catch (e) {
        log.error("listenMqtt", "Failed to create MQTT client:", e);
        return;
    }

    ctx.mqttClient.on('error', function (err) {
        if (ctx.loggedIn === false) return; // Ignore errors if logged out
        
        log.error("listenMqtt", "MQTT Error:", err.message || err);
        
        try {
            ctx.mqttClient.end(true);
        } catch (e) {}
        
        // Only reconnect if still logged in
        if (ctx.loggedIn !== false) {
            setTimeout(() => {
                if (ctx.loggedIn !== false) getSeqID();
            }, getRandomDelay());
        }
    });

    ctx.mqttClient.on('connect', function () {
        if (ctx.loggedIn === false) {
            ctx.mqttClient.end(true);
            return;
        }
        
        topics.forEach(topicsub => {
            try {
                ctx.mqttClient.subscribe(topicsub);
            } catch(e) {}
        });

        var topic;
        var queue = {
            sync_api_version: 10,
            max_deltas_able_to_process: 1000,
            delta_batch_size: 500,
            encoding: "JSON",
            entity_fbid: ctx.userID,
        };

        if (ctx.syncToken) {
            topic = "/messenger_sync_get_diffs";
            queue.last_seq_id = ctx.lastSeqId;
            queue.sync_token = ctx.syncToken;
        }
        else {
            topic = "/messenger_sync_create_queue";
            queue.initial_titan_sequence_id = ctx.lastSeqId;
            queue.device_params = null;
        }

        try {
            ctx.mqttClient.publish(topic, JSON.stringify(queue), { qos: 1, retain: false });
        } catch(e) {
            log.error("listenMqtt", "Publish error:", e);
        }

        var rTimeout = setTimeout(function () {
            if (ctx.loggedIn === false) return;
            
            try {
                ctx.mqttClient.end(true);
            } catch (e) {}
            
            setTimeout(() => {
                if (ctx.loggedIn !== false) getSeqID();
            }, getRandomDelay());
        }, 30000);

        ctx.tmsWait = function () {
            clearTimeout(rTimeout);
            if (ctx.globalOptions.emitReady) {
                globalCallback({
                    type: "ready",
                    error: null
                });
            }
            delete ctx.tmsWait;
        };
    });

    ctx.mqttClient.on('message', function (topic, message, _packet) {
        if (ctx.loggedIn === false) return;
        
        try {
            var jsonMessage = JSON.parse(message);
        }
        catch (ex) {
            return log.error("listenMqtt", "Parse error:", ex);
        }
        
        if (topic === "/t_ms") {
            if (ctx.tmsWait && typeof ctx.tmsWait == "function") ctx.tmsWait();

            if (jsonMessage.firstDeltaSeqId && jsonMessage.syncToken) {
                ctx.lastSeqId = jsonMessage.firstDeltaSeqId;
                ctx.syncToken = jsonMessage.syncToken;
            }

            if (jsonMessage.lastIssuedSeqId) ctx.lastSeqId = parseInt(jsonMessage.lastIssuedSeqId);

            for (var i in jsonMessage.deltas) {
                var delta = jsonMessage.deltas[i];
                parseDelta(defaultFuncs, api, ctx, globalCallback, { "delta": delta });
            }
        }
        else if (topic === "/thread_typing" || topic === "/orca_typing_notifications") {
            var typ = {
                type: "typ",
                isTyping: !!jsonMessage.state,
                from: jsonMessage.sender_fbid.toString(),
                threadID: utils.formatID((jsonMessage.thread || jsonMessage.sender_fbid).toString())
            };
            (function () { globalCallback(null, typ); })();
        }
        else if (topic === "/orca_presence") {
            if (!ctx.globalOptions.updatePresence) {
                for (var i in jsonMessage.list) {
                    var data = jsonMessage.list[i];
                    var userID = data["u"];

                    var presence = {
                        type: "presence",
                        userID: userID.toString(),
                        timestamp: data["l"] * 1000,
                        statuses: data["p"]
                    };
                    (function () { globalCallback(null, presence); })();
                }
            }
        }
    });

    ctx.mqttClient.on('close', function () {
        log.info("listenMqtt", "MQTT connection closed");
    });
}

function parseDelta(defaultFuncs, api, ctx, globalCallback, v) {
    if (v.delta.class == "NewMessage") {
        if (ctx.globalOptions.pageID && ctx.globalOptions.pageID != v.queue) return;

        (function resolveAttachmentUrl(i) {
            if (i == (v.delta.attachments || []).length) {
                let fmtMsg;
                try {
                    fmtMsg = utils.formatDeltaMessage(v);
                } catch (err) {
                    return globalCallback({
                        error: "Problem parsing message object.",
                        detail: err,
                        res: v,
                        type: "parse_error"
                    });
                }
                if (fmtMsg) {
                    if (ctx.globalOptions.autoMarkDelivery) {
                        markDelivery(ctx, api, fmtMsg.threadID, fmtMsg.messageID);
                    }
                }
                return !ctx.globalOptions.selfListen &&
                    (fmtMsg.senderID === ctx.i_userID || fmtMsg.senderID === ctx.userID) ?
                    undefined :
                    (function () { globalCallback(null, fmtMsg); })();
            } else {
                if (v.delta.attachments[i].mercury.attach_type == "photo") {
                    api.resolvePhotoUrl(
                        v.delta.attachments[i].fbid,
                        (err, url) => {
                            if (!err)
                                v.delta.attachments[
                                    i
                                ].mercury.metadata.url = url;
                            return resolveAttachmentUrl(i + 1);
                        }
                    );
                } else {
                    return resolveAttachmentUrl(i + 1);
                }
            }
        })(0);
    }

    if (v.delta.class == "ClientPayload") {
        var clientPayload = utils.decodeClientPayload(v.delta.payload);
        if (clientPayload && clientPayload.deltas) {
            for (var i in clientPayload.deltas) {
                var delta = clientPayload.deltas[i];
                if (delta.deltaMessageReaction && !!ctx.globalOptions.listenEvents) {
                    (function () {
                        globalCallback(null, {
                            type: "message_reaction",
                            threadID: (delta.deltaMessageReaction.threadKey.threadFbId ? delta.deltaMessageReaction.threadKey.threadFbId : delta.deltaMessageReaction.threadKey.otherUserFbId).toString(),
                            messageID: delta.deltaMessageReaction.messageId,
                            reaction: delta.deltaMessageReaction.reaction,
                            senderID: delta.deltaMessageReaction.senderId.toString(),
                            userID: delta.deltaMessageReaction.userId.toString()
                        });
                    })();
                }
                else if (delta.deltaRecallMessageData && !!ctx.globalOptions.listenEvents) {
                    (function () {
                        globalCallback(null, {
                            type: "message_unsend",
                            threadID: (delta.deltaRecallMessageData.threadKey.threadFbId ? delta.deltaRecallMessageData.threadKey.threadFbId : delta.deltaRecallMessageData.threadKey.otherUserFbId).toString(),
                            messageID: delta.deltaRecallMessageData.messageID,
                            senderID: delta.deltaRecallMessageData.senderID.toString(),
                            deletionTimestamp: delta.deltaRecallMessageData.deletionTimestamp,
                            timestamp: delta.deltaRecallMessageData.timestamp
                        });
                    })();
                }
                else if (delta.deltaMessageReply) {
                    var mdata = delta.deltaMessageReply.message === undefined ? [] :
                        delta.deltaMessageReply.message.data === undefined ? [] :
                            delta.deltaMessageReply.message.data.prng === undefined ? [] :
                                JSON.parse(delta.deltaMessageReply.message.data.prng);
                    var m_id = mdata.map(u => u.i);
                    var m_offset = mdata.map(u => u.o);
                    var m_length = mdata.map(u => u.l);

                    var mentions = {};

                    for (var i = 0; i < m_id.length; i++) mentions[m_id[i]] = (delta.deltaMessageReply.message.body || "").substring(m_offset[i], m_offset[i] + m_length[i]);
                    
                    var callbackToReturn = {
                        type: "message_reply",
                        threadID: (delta.deltaMessageReply.message.messageMetadata.threadKey.threadFbId ? delta.deltaMessageReply.message.messageMetadata.threadKey.threadFbId : delta.deltaMessageReply.message.messageMetadata.threadKey.otherUserFbId).toString(),
                        messageID: delta.deltaMessageReply.message.messageMetadata.messageId,
                        senderID: delta.deltaMessageReply.message.messageMetadata.actorFbId.toString(),
                        attachments: (delta.deltaMessageReply.message.attachments || []).map(function (att) {
                            var mercury = JSON.parse(att.mercuryJSON);
                            Object.assign(att, mercury);
                            return att;
                        }).map(att => {
                            var x;
                            try {
                                x = utils._formatAttachment(att);
                            }
                            catch (ex) {
                                x = att;
                                x.error = ex;
                                x.type = "unknown";
                            }
                            return x;
                        }),
                        args: (delta.deltaMessageReply.message.body || "").trim().split(/\s+/),
                        body: (delta.deltaMessageReply.message.body || ""),
                        isGroup: !!delta.deltaMessageReply.message.messageMetadata.threadKey.threadFbId,
                        mentions: mentions,
                        timestamp: delta.deltaMessageReply.message.messageMetadata.timestamp,
                        participantIDs: (delta.deltaMessageReply.message.messageMetadata.cid.canonicalParticipantFbids || delta.deltaMessageReply.message.participants || []).map(e => e.toString())
                    };

                    if (delta.deltaMessageReply.repliedToMessage) {
                        mdata = delta.deltaMessageReply.repliedToMessage === undefined ? [] :
                            delta.deltaMessageReply.repliedToMessage.data === undefined ? [] :
                                delta.deltaMessageReply.repliedToMessage.data.prng === undefined ? [] :
                                    JSON.parse(delta.deltaMessageReply.repliedToMessage.data.prng);
                        m_id = mdata.map(u => u.i);
                        m_offset = mdata.map(u => u.o);
                        m_length = mdata.map(u => u.l);

                        var rmentions = {};

                        for (var i = 0; i < m_id.length; i++) rmentions[m_id[i]] = (delta.deltaMessageReply.repliedToMessage.body || "").substring(m_offset[i], m_offset[i] + m_length[i]);
                        
                        callbackToReturn.messageReply = {
                            threadID: (delta.deltaMessageReply.repliedToMessage.messageMetadata.threadKey.threadFbId ? delta.deltaMessageReply.repliedToMessage.messageMetadata.threadKey.threadFbId : delta.deltaMessageReply.repliedToMessage.messageMetadata.threadKey.otherUserFbId).toString(),
                            messageID: delta.deltaMessageReply.repliedToMessage.messageMetadata.messageId,
                            senderID: delta.deltaMessageReply.repliedToMessage.messageMetadata.actorFbId.toString(),
                            attachments: delta.deltaMessageReply.repliedToMessage.attachments.map(function (att) {
                                var mercury = JSON.parse(att.mercuryJSON);
                                Object.assign(att, mercury);
                                return att;
                            }).map(att => {
                                var x;
                                try {
                                    x = utils._formatAttachment(att);
                                }
                                catch (ex) {
                                    x = att;
                                    x.error = ex;
                                    x.type = "unknown";
                                }
                                return x;
                            }),
                            args: (delta.deltaMessageReply.repliedToMessage.body || "").trim().split(/\s+/),
                            body: delta.deltaMessageReply.repliedToMessage.body || "",
                            isGroup: !!delta.deltaMessageReply.repliedToMessage.messageMetadata.threadKey.threadFbId,
                            mentions: rmentions,
                            timestamp: delta.deltaMessageReply.repliedToMessage.messageMetadata.timestamp
                        };
                    }
                    else if (delta.deltaMessageReply.replyToMessageId) {
                        return defaultFuncs
                            .post("https://www.facebook.com/api/graphqlbatch/", ctx.jar, {
                                "av": ctx.globalOptions.pageID,
                                "queries": JSON.stringify({
                                    "o0": {
                                        "doc_id": "2848441488556444",
                                        "query_params": {
                                            "thread_and_message_id": {
                                                "thread_id": callbackToReturn.threadID,
                                                "message_id": delta.deltaMessageReply.replyToMessageId.id,
                                            }
                                        }
                                    }
                                })
                            })
                            .then(utils.parseAndCheckLogin(ctx, defaultFuncs))
                            .then((resData) => {
                                if (resData[resData.length - 1].error_results > 0) throw resData[0].o0.errors;
                                if (resData[resData.length - 1].successful_results === 0) throw { error: "forcedFetch: there was no successful_results", res: resData };
                                var fetchData = resData[0].o0.data.message;
                                var mobj = {};
                                for (var n in fetchData.message.ranges) mobj[fetchData.message.ranges[n].entity.id] = (fetchData.message.text || "").substr(fetchData.message.ranges[n].offset, fetchData.message.ranges[n].length);

                                callbackToReturn.messageReply = {
                                    threadID: callbackToReturn.threadID,
                                    messageID: fetchData.message_id,
                                    senderID: fetchData.message_sender.id.toString(),
                                    attachments: fetchData.message.blob_attachment.map(att => {
                                        var x;
                                        try {
                                            x = utils._formatAttachment({ blob_attachment: att });
                                        }
                                        catch (ex) {
                                            x = att;
                                            x.error = ex;
                                            x.type = "unknown";
                                        }
                                        return x;
                                    }),
                                    args: (fetchData.message.text || "").trim().split(/\s+/) || [],
                                    body: fetchData.message.text || "",
                                    isGroup: callbackToReturn.isGroup,
                                    mentions: mobj,
                                    timestamp: parseInt(fetchData.timestamp_precise)
                                };
                            })
                            .catch(err => log.error("forcedFetch", err))
                            .finally(function () {
                                if (ctx.globalOptions.autoMarkDelivery) markDelivery(ctx, api, callbackToReturn.threadID, callbackToReturn.messageID);
                                !ctx.globalOptions.selfListen && callbackToReturn.senderID === ctx.userID ? undefined : (function () { globalCallback(null, callbackToReturn); })();
                            });
                    }
                    else callbackToReturn.delta = delta;

                    if (ctx.globalOptions.autoMarkDelivery) markDelivery(ctx, api, callbackToReturn.threadID, callbackToReturn.messageID);

                    return !ctx.globalOptions.selfListen && callbackToReturn.senderID === ctx.userID ? undefined : (function () { globalCallback(null, callbackToReturn); })();
                }
            }
            return;
        }
    }

    if (v.delta.class !== "NewMessage" && !ctx.globalOptions.listenEvents) return;
    switch (v.delta.class) {
        case "JoinableMode": {
            let fmtMsg;
            try {
                fmtMsg = utils.formatDeltaEvent(v.delta);
            } catch (err) {
                return globalCallback({
                    error: "Lỗi gòi!!",
                    detail: err,
                    res: v.delta,
                    type: "parse_error"
                });
            }
            return globalCallback(null, fmtMsg);
        }
        case "AdminTextMessage":
            switch (v.delta.type) {
                case 'confirm_friend_request':
                case 'shared_album_delete':
                case 'shared_album_addition':
                case 'pin_messages_v2':
                case 'unpin_messages_v2':
                case "change_thread_theme":
                case "change_thread_nickname":
                case "change_thread_icon":
                case "change_thread_quick_reaction":
                case "change_thread_admins":
                case "group_poll":
                case "joinable_group_link_mode_change":
                case "magic_words":
                case "change_thread_approval_mode":
                case "messenger_call_log":
                case "participant_joined_group_call":
                    var fmtMsg;
                    try {
                        fmtMsg = utils.formatDeltaEvent(v.delta);
                    }
                    catch (err) {
                        return globalCallback({
                            error: "Problem parsing message object. Please open an issue at https://github.com/Schmavery/facebook-chat-api/issues.",
                            detail: err,
                            res: v.delta,
                            type: "parse_error"
                        });
                    }
                    return (function () { globalCallback(null, fmtMsg); })();
                default:
                    return;
            }
        case "ForcedFetch":
            if (!v.delta.threadKey) return;
            var mid = v.delta.messageId;
            var tid = v.delta.threadKey.threadFbId;
            if (mid && tid) {
                const form = {
                    "av": ctx.globalOptions.pageID,
                    "queries": JSON.stringify({
                        "o0": {
                            "doc_id": "2848441488556444",
                            "query_params": {
                                "thread_and_message_id": {
                                    "thread_id": tid.toString(),
                                    "message_id": mid,
                                }
                            }
                        }
                    })
                };

                defaultFuncs
                    .post("https://www.facebook.com/api/graphqlbatch/", ctx.jar, form)
                    .then(utils.parseAndCheckLogin(ctx, defaultFuncs))
                    .then((resData) => {
                        if (resData[resData.length - 1].error_results > 0) throw resData[0].o0.errors;

                        if (resData[resData.length - 1].successful_results === 0) throw { error: "forcedFetch: there was no successful_results", res: resData };

                        var fetchData = resData[0].o0.data.message;

                        if (utils.getType(fetchData) == "Object") {
                            log.info("forcedFetch", fetchData);
                            switch (fetchData.__typename) {
                                case "ThreadImageMessage":
                                    (!ctx.globalOptions.selfListen && fetchData.message_sender.id.toString() === ctx.userID) ||
                                        !ctx.loggedIn ? undefined : (function () {
                                            globalCallback(null, {
                                                type: "change_thread_image",
                                                threadID: utils.formatID(tid.toString()),
                                                snippet: fetchData.snippet,
                                                timestamp: fetchData.timestamp_precise,
                                                author: fetchData.message_sender.id,
                                                image: {
                                                    attachmentID: fetchData.image_with_metadata && fetchData.image_with_metadata.legacy_attachment_id,
                                                    width: fetchData.image_with_metadata && fetchData.image_with_metadata.original_dimensions.x,
                                                    height: fetchData.image_with_metadata && fetchData.image_with_metadata.original_dimensions.y,
                                                    url: fetchData.image_with_metadata && fetchData.image_with_metadata.preview.uri
                                                }
                                            });
                                        })();
                                    break;
                                case "UserMessage":
                                    globalCallback(null, {
                                        type: "message",
                                        senderID: utils.formatID(fetchData.message_sender.id),
                                        body: fetchData.message.text || "",
                                        threadID: utils.formatID(tid.toString()),
                                        messageID: fetchData.message_id,
                                        attachments: [{
                                            type: "share",
                                            ID: fetchData.extensible_attachment.legacy_attachment_id,
                                            url: fetchData.extensible_attachment.story_attachment.url,

                                            title: fetchData.extensible_attachment.story_attachment.title_with_entities.text,
                                            description: fetchData.extensible_attachment.story_attachment.description.text,
                                            source: fetchData.extensible_attachment.story_attachment.source,

                                            image: ((fetchData.extensible_attachment.story_attachment.media || {}).image || {}).uri,
                                            width: ((fetchData.extensible_attachment.story_attachment.media || {}).image || {}).width,
                                            height: ((fetchData.extensible_attachment.story_attachment.media || {}).image || {}).height,
                                            playable: (fetchData.extensible_attachment.story_attachment.media || {}).is_playable || false,
                                            duration: (fetchData.extensible_attachment.story_attachment.media || {}).playable_duration_in_ms || 0,

                                            subattachments: fetchData.extensible_attachment.subattachments,
                                            properties: fetchData.extensible_attachment.story_attachment.properties,
                                        }],
                                        mentions: {},
                                        timestamp: parseInt(fetchData.timestamp_precise),
                                        participantIDs: (fetchData.participants || (fetchData.messageMetadata ? fetchData.messageMetadata.cid ? fetchData.messageMetadata.cid.canonicalParticipantFbids : fetchData.messageMetadata.participantIds : []) || []),
                                        isGroup: (fetchData.message_sender.id != tid.toString())
                                    });
                            }
                        }
                        else log.error("forcedFetch", fetchData);
                    })
                    .catch((err) => log.error("forcedFetch", err));
            }
            break;
        case "ThreadName":
        case "ParticipantsAddedToGroupThread":
        case "ParticipantsLeftGroupThread":
            var formattedEvent;
            try {
                formattedEvent = utils.formatDeltaEvent(v.delta);
            }
            catch (err) {
                return globalCallback({
                    error: "Problem parsing message object. Please open an issue at https://github.com/Schmavery/facebook-chat-api/issues.",
                    detail: err,
                    res: v.delta,
                    type: "parse_error"
                });
            }
            return (!ctx.globalOptions.selfListen && formattedEvent.author.toString() === ctx.userID) || !ctx.loggedIn ? undefined : (function () { globalCallback(null, formattedEvent); })();
    }
}

function markDelivery(ctx, api, threadID, messageID) {
    if (threadID && messageID) {
        api.markAsDelivered(threadID, messageID, (err) => {
            if (err) log.error("markAsDelivered", err);
            else {
                if (ctx.globalOptions.autoMarkRead) {
                    api.markAsRead(threadID, (err) => {
                        if (err) log.error("markAsDelivered", err);
                    });
                }
            }
        });
    }
}

// ==================== ULTRA STABLE LOGOUT SYSTEM ====================

function stopListening(ctx, globalCallback, callback) {
    callback = callback || (() => { });
    
    // Immediately mark as logged out to prevent any new operations
    ctx.loggedIn = false;
    
    // Replace globalCallback with identity to stop processing messages
    var oldCallback = globalCallback;
    globalCallback = identity;
    
    // Helper to safely end MQTT
    function safeEndMQTT(done) {
        if (!ctx.mqttClient) {
            return done();
        }
        
        var client = ctx.mqttClient;
        ctx.mqttClient = null;
        global.mqttClient = null;
        
        try {
            // Remove all listeners to prevent callbacks
            client.removeAllListeners('message');
            client.removeAllListeners('error');
            client.removeAllListeners('connect');
            client.removeAllListeners('close');
            
            // Try to unsubscribe cleanly
            try {
                topics.forEach(topic => {
                    client.unsubscribe(topic, () => {});
                });
            } catch(e) {}
            
            // Publish disconnect message
            try {
                client.publish("/browser_close", "{}", { qos: 0 });
            } catch(e) {}
            
            // End connection with force
            client.end(true, () => {
                done();
            });
            
            // Force close after 3 seconds if not closed
            setTimeout(() => {
                try {
                    client.end(true);
                } catch(e) {}
                done();
            }, 3000);
        } catch(err) {
            done();
        }
    }
    
    safeEndMQTT(() => {
        // Clear all session data
        ctx.lastSeqId = null;
        ctx.syncToken = undefined;
        ctx.t_mqttCalled = false;
        ctx.tmsWait = null;
        
        log.info("stopListening", "Listener stopped successfully");
        callback(null, { success: true });
    });
}

function logout(defaultFuncs, ctx, callback) {
    callback = callback || function() {};
    
    return new Promise((resolve, reject) => {
        // First stop listening
        stopListening(ctx, identity, function(err) {
            if (err) {
                log.warn("logout", "Warning during stopListening:", err);
            }
            
            // Clear form to prevent new requests
            form = {};
            
            // Call Facebook logout endpoint
            var logoutForm = {
                "ref": "mb",
                "h": ctx.fb_dtsg || ""
            };
            
            defaultFuncs
                .post("https://www.facebook.com/logout.php", ctx.jar, logoutForm)
                .then(function(res) {
                    log.info("logout", "Server logout successful");
                    
                    // Clear cookies
                    try {
                        if (ctx.jar) {
                            var store = ctx.jar._jar || ctx.jar;
                            if (store && store.removeAllCookiesSync) {
                                store.removeAllCookiesSync();
                            } else if (store && store.removeAllCookies) {
                                store.removeAllCookies(() => {});
                            }
                        }
                    } catch(e) {
                        log.warn("logout", "Cookie clear warning:", e.message);
                    }
                    
                    var result = { 
                        success: true, 
                        message: "Logged out successfully",
                        timestamp: Date.now()
                    };
                    resolve(result);
                    if (callback) callback(null, result);
                })
                .catch(function(err) {
                    log.warn("logout", "Server logout failed:", err.message);
                    
                    // Even if server logout fails, we consider it success locally
                    var result = { 
                        success: true, 
                        warning: "Local logout successful, server logout failed",
                        timestamp: Date.now()
                    };
                    resolve(result);
                    if (callback) callback(null, result);
                });
        });
    });
}

// ==================== END ULTRA STABLE LOGOUT ====================

module.exports = function (defaultFuncs, api, ctx) {
    let globalCallback = identity;
    
    // Add logout method to api
    api.logout = function(callback) {
        return logout(defaultFuncs, ctx, callback);
    };
    
    // Add stopListening method to api
    api.stopListening = function(callback) {
        return stopListening(ctx, globalCallback, callback);
    };

    getSeqID = function getSeqID() {
        // CRITICAL: Don't reconnect if logged out
        if (ctx.loggedIn === false) {
            log.info("getSeqID", "Skipping reconnect - user logged out");
            return;
        }
        
        ctx.t_mqttCalled = false;
        defaultFuncs
            .post("https://www.facebook.com/api/graphqlbatch/", ctx.jar, form)
            .then(utils.parseAndCheckLogin(ctx, defaultFuncs))
            .then((resData) => {
                // Check again after async operation
                if (ctx.loggedIn === false) {
                    log.info("getSeqID", "Aborting - logged out during request");
                    return;
                }
                
                if (utils.getType(resData) != "Array") throw { error: "Not logged in", res: resData };
                if (resData && resData[resData.length - 1].error_results > 0) throw resData[0].o0.errors;
                if (resData[resData.length - 1].successful_results === 0) throw { error: "getSeqId: there was no successful_results", res: resData };
                if (resData[0].o0.data.viewer.message_threads.sync_sequence_id) {
                    ctx.lastSeqId = resData[0].o0.data.viewer.message_threads.sync_sequence_id;
                    listenMqtt(defaultFuncs, api, ctx, globalCallback);
                } else throw { error: "getSeqId: no sync_sequence_id found.", res: resData };
            })
            .catch((err) => {
                if (ctx.loggedIn === false) {
                    log.info("getSeqID", "Ignoring error - logged out");
                    return;
                }
                
                log.error("getSeqId", err);
                if (utils.getType(err) == "Object" && err.error === "Not logged in") ctx.loggedIn = false;
                return globalCallback(err);
            });
    };

    return function (callback) {
        class MessageEmitter extends EventEmitter {
            stopListening(callback) {
                return stopListening(ctx, globalCallback, callback);
            }

            async stopListeningAsync() {
                return new Promise((resolve, reject) => {
                    this.stopListening((err, res) => {
                        if (err) reject(err);
                        else resolve(res);
                    });
                });
            }
            
            logout(callback) {
                return api.logout(callback);
            }
            
            async logoutAsync() {
                return new Promise((resolve, reject) => {
                    api.logout((err, res) => {
                        if (err) reject(err);
                        else resolve(res);
                    });
                });
            }
        }

        const msgEmitter = new MessageEmitter();
        globalCallback = (callback || function (error, message) {
            if (error) {
                return msgEmitter.emit("error", error);
            }
            msgEmitter.emit("message", message);
        });

        // Reset some stuff
        if (!ctx.firstListen)
            ctx.lastSeqId = null;
        ctx.syncToken = undefined;
        ctx.t_mqttCalled = false;

        form = {
            "av": ctx.globalOptions.pageID,
            "queries": JSON.stringify({
                "o0": {
                    "doc_id": "3336396659757871",
                    "query_params": {
                        "limit": 1,
                        "before": null,
                        "tags": ["INBOX"],
                        "includeDeliveryReceipts": false,
                        "includeSeqID": true
                    }
                }
            })
        };

        if (!ctx.firstListen || !ctx.lastSeqId) {
            getSeqID(defaultFuncs, api, ctx, globalCallback);
        } else {
            listenMqtt(defaultFuncs, api, ctx, globalCallback);
        }

        ctx.firstListen = true;
        
        return msgEmitter;
    };
};