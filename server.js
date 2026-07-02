const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

// ─── سيرفر HTTP ───
const server = http.createServer((req, res) => {
    if (req.url === '/') {
        fs.readFile(path.join(__dirname, 'dashboard.html'), (err, data) => {
            if (err) {
                res.writeHead(500);
                res.end('Error');
                return;
            }
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(data);
        });
    } else {
        res.writeHead(404);
        res.end('Not found');
    }
});

// ─── سيرفر WebSocket ───
const wss = new WebSocket.Server({ server, path: '/stream' });

const devices = new Map(); // deviceId → ws
const viewers = new Map(); // viewerId → ws

// تخزين آخر إطار لكل جهاز (للمشاهدين الجدد)
const lastFrames = new Map();

wss.on('connection', (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const deviceId = url.searchParams.get('device');
    const viewerId = url.searchParams.get('viewer') || Math.random().toString(36).substring(7);
    const type = url.searchParams.get('type') || 'viewer';

    console.log(`🔌 ${type === 'device' ? '📱 Device' : '🖥️ Viewer'} connected: ${deviceId || viewerId}`);

    if (type === 'device') {
        // ─── اتصال جهاز ───
        devices.set(deviceId, ws);
        
        // إعلام جميع المشاهدين
        broadcastToViewers({
            type: 'device_online',
            deviceId: deviceId,
            timestamp: Date.now()
        });

        ws.on('message', (message) => {
            // استقبال الإطار
            const frame = message.toString();
            
            // حفظ آخر إطار
            lastFrames.set(deviceId, {
                image: frame,
                timestamp: Date.now()
            });
            
            // إعادة الإرسال للمشاهدين
            broadcastToViewers({
                type: 'frame',
                deviceId: deviceId,
                image: frame,
                timestamp: Date.now()
            }, deviceId);
        });

        ws.on('close', () => {
            console.log(`📱 Device disconnected: ${deviceId}`);
            devices.delete(deviceId);
            broadcastToViewers({
                type: 'device_offline',
                deviceId: deviceId
            });
        });

    } else {
        // ─── اتصال مشاهد ───
        viewers.set(viewerId, ws);
        
        // إرسال قائمة الأجهزة
        const deviceList = Array.from(devices.keys()).map(id => ({
            id: id,
            online: true
        }));
        
        ws.send(JSON.stringify({
            type: 'device_list',
            devices: deviceList
        }));

        // إرسال آخر إطار لكل جهاز (للمشاهد الجدد)
        lastFrames.forEach((frame, devId) => {
            if (Date.now() - frame.timestamp < 5000) { // فقط آخر 5 ثوانٍ
                ws.send(JSON.stringify({
                    type: 'frame',
                    deviceId: devId,
                    image: frame.image,
                    timestamp: frame.timestamp
                }));
            }
        });

        ws.on('close', () => {
            viewers.delete(viewerId);
        });
    }
});

// ─── تنقية الإطارات القديمة كل دقيقة ───
setInterval(() => {
    const now = Date.now();
    lastFrames.forEach((frame, deviceId) => {
        if (now - frame.timestamp > 10000) {
            lastFrames.delete(deviceId);
        }
    });
}, 60000);

function broadcastToViewers(data, excludeDevice = null) {
    const message = JSON.stringify(data);
    viewers.forEach((ws) => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(message);
        }
    });
}

// ─── تشغيل ───
const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Server running on http://0.0.0.0:${PORT}`);
    console.log(`📡 WebSocket on ws://0.0.0.0:${PORT}/stream`);
});
