const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

// ─── استخدام PORT من البيئة أو 8080 ───
const PORT = process.env.PORT || 8080;
const HOST = '0.0.0.0';

// ─── سيرفر HTTP ───
const server = http.createServer((req, res) => {
    // CORS للسماح من أي مصدر
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
    }
    // مسار للتحقق من صحة الاتصال
    else if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'online',
            devices: Array.from(devices.keys()).length,
            viewers: viewers.size,
            uptime: Math.floor((Date.now() - startTime) / 1000) + 's'
        }));
    }
    else {
        res.writeHead(404);
        res.end('Not found');
    }
});

// ─── سيرفر WebSocket مع دعم جميع المنصات ───
const wss = new WebSocket.WebSocketServer({ 
    server,
    // عدم تحديد path ليعمل على جميع المنصات
    maxPayload: 50 * 1024 * 1024 // 50MB حد أقصى
});

const devices = new Map(); // deviceId → { ws, lastSeen }
const viewers = new Map(); // viewerId → ws
const lastFrames = new Map(); // deviceId → { image, timestamp }
let startTime = Date.now();

wss.on('connection', (ws, req) => {
    // استخراج المعاملات من URL
    const url = new URL(req.url, `http://${req.headers.host}`);
    const deviceId = url.searchParams.get('device');
    const viewerId = url.searchParams.get('viewer') || Math.random().toString(36).substring(7);
    const type = url.searchParams.get('type') || 'viewer';
    const protocol = req.headers['sec-websocket-protocol'];

    console.log(`🔌 ${type === 'device' ? '📱 Device' : '🖥️ Viewer'} connected: ${deviceId || viewerId}`);

    // إرسال PING كل 30 ثانية للحفاظ على الاتصال
    const pingInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.ping();
        }
    }, 30000);

    if (type === 'device') {
        // ─── اتصال جهاز ───
        devices.set(deviceId, { ws, lastSeen: Date.now() });
        
        // إعلام جميع المشاهدين بجهاز جديد
        broadcastToAll({
            type: 'device_online',
            deviceId: deviceId,
            timestamp: Date.now()
        }, 'viewer');

        ws.on('message', (message, isBinary) => {
            if (isBinary) {
                // دعم الإطارات الثنائية (أسرع)
                const base64 = message.toString('base64');
                lastFrames.set(deviceId, {
                    image: base64,
                    timestamp: Date.now()
                });
                broadcastToAll({
                    type: 'frame',
                    deviceId: deviceId,
                    image: base64,
                    timestamp: Date.now(),
                    binary: true
                }, 'viewer');
            } else {
                // دعم الإطارات النصية (Base64)
                const frame = message.toString();
                lastFrames.set(deviceId, {
                    image: frame,
                    timestamp: Date.now()
                });
                broadcastToAll({
                    type: 'frame',
                    deviceId: deviceId,
                    image: frame,
                    timestamp: Date.now(),
                    binary: false
                }, 'viewer');
            }
            // تحديث آخر نشاط
            if (devices.has(deviceId)) {
                devices.get(deviceId).lastSeen = Date.now();
            }
        });

        ws.on('close', () => {
            clearInterval(pingInterval);
            console.log(`📱 Device disconnected: ${deviceId}`);
            devices.delete(deviceId);
            broadcastToAll({
                type: 'device_offline',
                deviceId: deviceId,
                timestamp: Date.now()
            }, 'viewer');
        });

        ws.on('error', () => {
            clearInterval(pingInterval);
            devices.delete(deviceId);
        });

    } else {
        // ─── اتصال مشاهد ───
        viewers.set(viewerId, ws);
        
        // إرسال قائمة الأجهزة
        const deviceList = Array.from(devices.keys()).map(id => ({
            id: id,
            online: true,
            lastSeen: devices.get(id).lastSeen
        }));
        
        safeSend(ws, {
            type: 'device_list',
            devices: deviceList
        });

        // إرسال آخر الإطارات لكل جهاز
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

        ws.on('close', () => {
            clearInterval(pingInterval);
            viewers.delete(viewerId);
        });

        ws.on('error', () => {
            clearInterval(pingInterval);
            viewers.delete(viewerId);
        });
    }
});

// ─── دالة البث الآمن ───
function safeSend(ws, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        try {
            ws.send(JSON.stringify(data));
        } catch (e) {
            // تجاهل أخطاء الإرسال
        }
    }
}

function broadcastToAll(data, type = 'all') {
    const message = JSON.stringify(data);
    
    if (type === 'all' || type === 'viewer') {
        viewers.forEach((ws) => {
            if (ws.readyState === WebSocket.OPEN) {
                try {
                    ws.send(message);
                } catch (e) {}
            }
        });
    }
    
    if (type === 'all' || type === 'device') {
        devices.forEach((device) => {
            if (device.ws.readyState === WebSocket.OPEN) {
                try {
                    device.ws.send(message);
                } catch (e) {}
            }
        });
    }
}

// ─── تنظيف الأجهزة غير النشطة ───
setInterval(() => {
    const now = Date.now();
    devices.forEach((device, id) => {
        if (now - device.lastSeen > 60000) { // دقيقة بدون نشاط
            device.ws.close();
            devices.delete(id);
            broadcastToAll({
                type: 'device_offline',
                deviceId: id,
                timestamp: now
            }, 'viewer');
        }
    });
    
    // تنظيف الإطارات القديمة
    lastFrames.forEach((frame, id) => {
        if (now - frame.timestamp > 30000) {
            lastFrames.delete(id);
        }
    });
}, 30000);

// ─── تشغيل السيرفر ───
server.listen(PORT, HOST, () => {
    console.log(`=================================`);
    console.log(`  🚀 NEXUS STREAM SERVER`);
    console.log(`  🌐 http://${HOST}:${PORT}`);
    console.log(`  📡 ws://${HOST}:${PORT}`);
    console.log(`  💡 يعمل على جميع المنصات`);
    console.log(`=================================`);
});
