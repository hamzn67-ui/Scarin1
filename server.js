const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

// ─── سيرفر HTTP (لخدمة الـ Dashboard) ───
const server = http.createServer((req, res) => {
    if (req.url === '/') {
        fs.readFile(path.join(__dirname, 'dashboard.html'), (err, data) => {
            if (err) {
                res.writeHead(500);
                res.end('Error loading dashboard');
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
const wss = new WebSocket.Server({ 
    server,
    path: '/stream'
});

// تخزين الاتصالات: deviceId → WebSocket
const deviceConnections = new Map();
// تخزين لوحات التحكم
const viewers = [];

wss.on('connection', (ws, req) => {
    // تصحيح قراءة الرابط ليتوافق مع بروتوكولات wss:// و ws:// عبر بيئة الاستضافة
    const host = req.headers.host;
    const url = new URL(req.url, `http://${host}`);
    const deviceId = url.searchParams.get('device');
    const type = url.searchParams.get('type') || 'device'; // 'device' or 'viewer'

    console.log(`🔌 New connection: ${type} - ${deviceId || 'unknown'}`);

    if (type === 'device') {
        deviceConnections.set(deviceId, ws);
        console.log(`📱 Device registered: ${deviceId}`);

        broadcastToViewers({
            type: 'device_online',
            deviceId: deviceId,
            timestamp: Date.now()
        });

        ws.on('message', (message) => {
            const frameData = message.toString();
            
            broadcastToViewers({
                type: 'frame',
                deviceId: deviceId,
                image: frameData,
                timestamp: Date.now()
            }, deviceId);
        });

        ws.on('close', () => {
            console.log(`📱 Device disconnected: ${deviceId}`);
            deviceConnections.delete(deviceId);
            
            broadcastToViewers({
                type: 'device_offline',
                deviceId: deviceId,
                timestamp: Date.now()
            });
        });

    } else if (type === 'viewer') {
        const viewerId = url.searchParams.get('viewer') || Math.random().toString(36).substring(7);
        viewers.push({ id: viewerId, ws: ws });

        const devices = Array.from(deviceConnections.keys()).map(id => ({
            id: id,
            online: true
        }));
        ws.send(JSON.stringify({
            type: 'device_list',
            devices: devices
        }));

        ws.on('close', () => {
            const index = viewers.findIndex(v => v.id === viewerId);
            if (index !== -1) viewers.splice(index, 1);
        });
    }
});

function broadcastToViewers(data, filterDeviceId = null) {
    const message = JSON.stringify(data);
    viewers.forEach(viewer => {
        if (viewer.ws.readyState === WebSocket.OPEN) {
            viewer.ws.send(message);
        }
    });
}

// ─── تعديل تهيئة تشغيل السيرفر للتوافق السحابي ───
// هنا نتحقق إذا كان هناك منفذ ممرر من Render، وإلا نستخدم 8080 محلياً
const PORT = process.env.PORT || 8080;

server.listen(PORT, () => {
    console.log(`🚀 Server fully operational on port: ${PORT}`);
});
