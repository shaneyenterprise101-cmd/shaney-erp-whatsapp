import express from 'express';
import cors from 'cors';
import { Server } from 'socket.io';
import http from 'http';
import { makeWASocket, useMultiFileAuthState, DisconnectReason } from '@whiskeysockets/baileys';
import pino from 'pino';
import qrcode from 'qrcode';
import sqlite3 from 'sqlite3';

// 🟢 ES Module Setup (No require)
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '50mb' }));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// 📦 Local SQLite Database Setup (3GB Data Store)
const db = new sqlite3.Database('./whatsapp_local.db', (err) => {
    if (err) {
        console.error("DB Connection Error:", err.message);
    } else {
        console.log("📦 Local SQLite Database Connected & Ready!");
        
        db.run(`CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            remoteJid TEXT,
            fromMe BOOLEAN,
            body TEXT,
            timestamp INTEGER
        )`);
        
        db.run(`CREATE TABLE IF NOT EXISTS wa_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT,
            receiver TEXT,
            status TEXT,
            sentOn TEXT
        )`);
    }
});

let waStatus = 'Disconnected';
let qrCodeData = '';
let connectedPhone = '';
let waSocket = null;

async function connectToWhatsApp() {
    const { state, saveCreds } = await useMultiFileAuthState('baileys_auth_info');

    waSocket = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        logger: pino({ level: 'silent' }), // Set to silent to hide annoying internal Signal logs
        browser: ["Shaney ERP", "Chrome", "1.0.0"]
    });

    waSocket.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            waStatus = 'Scanning';
            qrCodeData = await qrcode.toDataURL(qr);
            io.emit('wa-status-update', { status: waStatus, qr: qrCodeData });
        }

        if (connection === 'close') {
            const shouldReconnect = lastDisconnect.error?.output?.statusCode !== DisconnectReason.loggedOut;
            if (shouldReconnect) connectToWhatsApp();
            else {
                waStatus = 'Disconnected';
                io.emit('wa-status-update', { status: waStatus });
            }
        } else if (connection === 'open') {
            waStatus = 'Connected';
            qrCodeData = '';
            connectedPhone = waSocket.user.id.split(':')[0];
            console.log('✅ Baileys Microservice is Ready & Connected!');
            io.emit('wa-status-update', { status: waStatus, phone: connectedPhone });
        }
    });

    waSocket.ev.on('creds.update', saveCreds);

    // 🟢 3GB BACKGROUND SYNC (Historical Messages)
    waSocket.ev.on('messaging-history.set', async ({ messages }) => {
        console.log(`📥 Background Sync Started: Downloading ${messages.length} historical messages...`);
        
        db.serialize(() => {
            db.run("BEGIN TRANSACTION");
            messages.forEach(msg => {
                try {
                    if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

                    const remoteJid = msg.key.remoteJidAlt || msg.key.remoteJid;
                    const fromMe = msg.key.fromMe ? 1 : 0; 
                    const id = msg.key.id;
                    const timestamp = (msg.messageTimestamp * 1000) || Date.now();
                    
                    // 🛡️ SMART UNWRAPPER: Disappearing & View Once messages ko open karo
                    let msgContent = msg.message;
                    if (msgContent.ephemeralMessage) msgContent = msgContent.ephemeralMessage.message;
                    if (msgContent.viewOnceMessage) msgContent = msgContent.viewOnceMessage.message;
                    if (msgContent.viewOnceMessageV2) msgContent = msgContent.viewOnceMessageV2.message;
                    if (msgContent.documentWithCaptionMessage) msgContent = msgContent.documentWithCaptionMessage.message;

                    let body = "";
                    if (msgContent.protocolMessage && (msgContent.protocolMessage.type === 14 || msgContent.protocolMessage.type === 'REVOKE')) {
                        body = "🚫 This message was deleted";
                    } else if (msgContent.conversation) body = msgContent.conversation;
                    else if (msgContent.extendedTextMessage?.text) body = msgContent.extendedTextMessage.text;
                    else if (msgContent.imageMessage?.caption) body = msgContent.imageMessage.caption;
                    else if (msgContent.documentMessage?.caption) body = msgContent.documentMessage.caption;
                    else if (msgContent.documentMessage?.fileName) body = `[Document] ${msgContent.documentMessage.fileName}`;
                    else if (msgContent.imageMessage) body = "[Image Received]";
                    else if (msgContent.videoMessage) body = "[Video Received]";
                    else if (msgContent.audioMessage) body = "[Audio Received]";

                    if (body) {
                        db.run(`INSERT OR IGNORE INTO messages (id, remoteJid, fromMe, body, timestamp) VALUES (?, ?, ?, ?, ?)`,
                            [id, remoteJid, fromMe, body, timestamp]);
                    }
                } catch (err) {} // Ignore error for single corrupt historical message
            });
            db.run("COMMIT");
        });
        console.log("✅ Background Sync Complete: Data saved securely in SQLite!");
    });

    // 🟢 Live Incoming & Outgoing Messages Handler
    waSocket.ev.on('messages.upsert', async (m) => {
        if (m.type === 'notify') {
            for (const msg of m.messages) {
                try {
                    if (!msg.message || msg.key.remoteJid === 'status@broadcast') continue;

                    const remoteJid = msg.key.remoteJidAlt || msg.key.remoteJid;
                    const fromMe = msg.key.fromMe ? 1 : 0; 
                    const id = msg.key.id;
                    const timestamp = (msg.messageTimestamp * 1000) || Date.now();
                    
                    // 🛡️ SMART UNWRAPPER
                    let msgContent = msg.message;
                    if (msgContent.ephemeralMessage) msgContent = msgContent.ephemeralMessage.message;
                    if (msgContent.viewOnceMessage) msgContent = msgContent.viewOnceMessage.message;
                    if (msgContent.viewOnceMessageV2) msgContent = msgContent.viewOnceMessageV2.message;
                    if (msgContent.documentWithCaptionMessage) msgContent = msgContent.documentWithCaptionMessage.message;

                    let body = "";
                    let isRevoke = false;
                    let revokedMsgId = null;

                    // Message Format Detection
                    if (msgContent.protocolMessage && (msgContent.protocolMessage.type === 14 || msgContent.protocolMessage.type === 'REVOKE')) {
                        isRevoke = true;
                        revokedMsgId = msgContent.protocolMessage.key.id;
                        body = "🚫 This message was deleted";
                    } else if (msgContent.conversation) {
                        body = msgContent.conversation;
                    } else if (msgContent.extendedTextMessage?.text) {
                        body = msgContent.extendedTextMessage.text;
                    } else if (msgContent.imageMessage?.caption) {
                        body = msgContent.imageMessage.caption;
                    } else if (msgContent.documentMessage?.caption) {
                        body = msgContent.documentMessage.caption;
                    } else if (msgContent.documentMessage?.fileName) {
                        body = `[Document] ${msgContent.documentMessage.fileName}`;
                    } else if (msgContent.imageMessage) {
                        body = "[Image Received]";
                    } else if (msgContent.videoMessage) {
                        body = "[Video Received]";
                    } else if (msgContent.audioMessage) {
                        body = "[Audio Received]";
                    }

                    let cleanPhone = remoteJid.replace('@s.whatsapp.net', '').replace('@c.us', '').replace(/\D/g, '');

                    // Execute Logic based on Type
                    if (isRevoke && revokedMsgId) {
                        // DB update
                        db.run(`UPDATE messages SET body = '🚫 This message was deleted' WHERE id = ?`, [revokedMsgId]);
                        // Send live delete signal to UI
                        io.emit('message_deleted', { id: revokedMsgId, phone: cleanPhone });
                    } else if (body) {
                        // DB insert
                        db.run(`INSERT OR IGNORE INTO messages (id, remoteJid, fromMe, body, timestamp) VALUES (?, ?, ?, ?, ?)`,
                            [id, remoteJid, fromMe, body, timestamp],
                            function(err) {
                                if (!err) {
                                    // Send live message to UI
                                    io.emit('new_message', {
                                        id: id,
                                        fromMe: msg.key.fromMe, 
                                        body: body,
                                        timestamp: timestamp,
                                        phone: cleanPhone
                                    });
                                }
                            }
                        );
                    }
                } catch (processErr) {
                    console.error("Error processing live message:", processErr);
                }
            }
        }
    });

    // 🟢 Bluetick / Read Listener
    waSocket.ev.on('messages.update', async (updates) => {
        for (const update of updates) {
            if (update.update.status === 4 || update.update.status === 'READ' || update.update.status === 3) {
                let msgId = update.key.id;
                db.get(`SELECT remoteJid FROM messages WHERE id = ?`, [msgId], (err, row) => {
                    if (!err && row && row.remoteJid) {
                        let remoteJid = row.remoteJid;
                        let cleanPhone = remoteJid.replace('@s.whatsapp.net', '').replace('@c.us', '').replace(/\D/g, '');
                        io.emit('message_read', { id: msgId, phone: cleanPhone });
                    }
                });
            }
        }
    });
}

// ==========================================
// 🌐 BACKEND API ROUTES
// ==========================================

app.get('/api/whatsapp/status', (req, res) => {
    res.json({ status: waStatus, qr: qrCodeData, phone: connectedPhone });
});

app.post('/api/whatsapp/connect', (req, res) => {
    if (waStatus === 'Disconnected') {
        connectToWhatsApp();
        waStatus = 'Initializing';
    }
    res.json({ success: true, status: waStatus });
});

app.post('/api/whatsapp/logout', async (req, res) => {
    try {
        console.log("🛑 Disconnect requested from frontend...");

        if (waSocket) {
            try { await waSocket.logout(); } catch (err) { }
            waSocket.end(undefined);
            waSocket = null; 
        }

        const authPath = path.join(__dirname, 'baileys_auth_info');
        if (fs.existsSync(authPath)) {
            fs.rmSync(authPath, { recursive: true, force: true });
        }

        waStatus = 'Disconnected';
        qrCodeData = '';
        connectedPhone = '';
        
        setTimeout(() => { connectToWhatsApp(); }, 2000);

        res.json({ success: true, message: "WhatsApp Disconnected Cleanly" });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/api/whatsapp/messages', (req, res) => {
    const phone = req.query.phone;
    if (!phone) return res.json({ success: false, error: "Phone required" });
    
    let cleanPhone = phone.replace(/\D/g, '');
    if (cleanPhone.length === 10) cleanPhone = "91" + cleanPhone;
    const jid = `${cleanPhone}@s.whatsapp.net`; 

    db.all(`SELECT * FROM messages WHERE remoteJid = ? ORDER BY timestamp ASC`, [jid], (err, rows) => {
        if (err) {
            res.json({ success: true, messages: [] });
        } else {
            const formatted = rows.map(r => ({
                id: r.id,
                fromMe: r.fromMe === 1, 
                body: r.body,
                timestamp: r.timestamp
            }));
            res.json({ success: true, messages: formatted });
        }
    });
});

app.get('/api/whatsapp/profile-pic', async (req, res) => {
    try {
        const phone = req.query.phone;
        if (!phone) return res.json({ success: false, profilePicUrl: '' });
        
        let cleanPhone = phone.replace(/\D/g, '');
        if (cleanPhone.length === 10) cleanPhone = "91" + cleanPhone;
        const jid = `${cleanPhone}@s.whatsapp.net`;
        
        if (waSocket) {
            const ppUrl = await waSocket.profilePictureUrl(jid, 'image').catch(() => null);
            res.json({ success: true, profilePicUrl: ppUrl || '' });
        } else {
            res.json({ success: false, profilePicUrl: '' });
        }
    } catch (error) {
        res.json({ success: false, profilePicUrl: '' });
    }
});

app.post('/api/whatsapp/mark-read', async (req, res) => {
    try {
        const { phone } = req.body;
        if (!phone) return res.json({ success: false, error: "Phone required" });

        let cleanPhone = phone.replace(/\D/g, '');
        if (cleanPhone.length === 10) cleanPhone = "91" + cleanPhone;
        const jid = `${cleanPhone}@s.whatsapp.net`;

        if (waSocket) {
            db.all(`SELECT id FROM messages WHERE remoteJid = ? AND fromMe = 0 ORDER BY timestamp DESC LIMIT 15`, [jid], async (err, rows) => {
                if (!err && rows && rows.length > 0) {
                    const keys = rows.map(r => ({ remoteJid: jid, id: r.id, fromMe: false }));
                    await waSocket.readMessages(keys);
                }
            });
            res.json({ success: true });
        } else {
            res.json({ success: false, error: "Not connected" });
        }
    } catch (error) {
        res.json({ success: false, error: error.message });
    }
});

app.post('/api/whatsapp/send', async (req, res) => {
    try {
        const { phone, message } = req.body;
        if (!phone || !message) return res.json({ success: false, error: "Phone and message required" });
        
        let cleanPhone = phone.replace(/\D/g, '');
        if (cleanPhone.length === 10) cleanPhone = "91" + cleanPhone;
        const jid = `${cleanPhone}@s.whatsapp.net`;
        
        if (waSocket) {
            const sent = await waSocket.sendMessage(jid, { text: message });
            const realId = sent.key.id;
            const timestamp = (sent.messageTimestamp * 1000) || Date.now();

            db.run(`INSERT OR IGNORE INTO messages (id, remoteJid, fromMe, body, timestamp) VALUES (?, ?, ?, ?, ?)`,
                [realId, jid, 1, message, timestamp]
            );

            res.json({ success: true, id: realId, timestamp: timestamp });
        } else {
            res.json({ success: false, error: "Not connected" });
        }
    } catch (error) {
        res.json({ success: false, error: error.message });
    }
});

app.post('/api/whatsapp/send-file', async (req, res) => {
    try {
        const { phone, message, fileBase64, filename, mimetype, type } = req.body;
        if (!phone || !fileBase64) return res.json({ success: false, error: "Phone and file required" });
        
        let cleanPhone = phone.replace(/\D/g, '');
        if (cleanPhone.length === 10) cleanPhone = "91" + cleanPhone;
        const jid = `${cleanPhone}@s.whatsapp.net`;
        
        if (waSocket) {
            const buffer = Buffer.from(fileBase64, 'base64');
            let msgObject = {};

            if (type === 'image') msgObject = { image: buffer, caption: message || '' };
            else if (type === 'video') msgObject = { video: buffer, caption: message || '', mimetype: mimetype };
            else if (type === 'audio') msgObject = { audio: buffer, mimetype: mimetype, ptt: false };
            else msgObject = { document: buffer, mimetype: mimetype || 'application/octet-stream', fileName: filename || 'document.file', caption: message || '' };

            const sent = await waSocket.sendMessage(jid, msgObject);
            const realId = sent.key.id;

            db.run(`INSERT OR IGNORE INTO messages (id, remoteJid, fromMe, body, timestamp) VALUES (?, ?, ?, ?, ?)`,
                [realId, jid, 1, `[File Sent: ${filename}]`, (sent.messageTimestamp * 1000) || Date.now()]
            );

            res.json({ success: true, id: realId, timestamp: (sent.messageTimestamp * 1000) || Date.now() });
        } else {
            res.json({ success: false, error: "Not connected" });
        }
    } catch (error) {
        res.json({ success: false, error: error.message });
    }
});
app.post('/api/whatsapp/send-pdf', async (req, res) => {
    try {
        const { phone, message, pdfBase64, filename } = req.body;
        if (!phone || !pdfBase64) return res.json({ success: false, error: "Phone and pdfBase64 required" });
        
        let cleanPhone = phone.replace(/\D/g, '');
        if (cleanPhone.length === 10) cleanPhone = "91" + cleanPhone;
        const jid = `${cleanPhone}@s.whatsapp.net`;
        
        if (waSocket) {
            const buffer = Buffer.from(pdfBase64, 'base64');
            let msgObject = { 
                document: buffer, 
                mimetype: 'application/pdf', 
                fileName: filename || 'document.pdf', 
                caption: message || '' 
            };

            const sent = await waSocket.sendMessage(jid, msgObject);
            const realId = sent.key.id;

            db.run(`INSERT OR IGNORE INTO messages (id, remoteJid, fromMe, body, timestamp) VALUES (?, ?, ?, ?, ?)`,
                [realId, jid, 1, `[PDF Sent: ${filename}]`, (sent.messageTimestamp * 1000) || Date.now()]
            );

            res.json({ success: true, id: realId, timestamp: (sent.messageTimestamp * 1000) || Date.now() });
        } else {
            res.json({ success: false, error: "Not connected" });
        }
    } catch (error) {
        res.json({ success: false, error: error.message });
    }
});

// 🟢 NEW: DELETE FOR EVERYONE API
app.post('/api/whatsapp/delete-for-everyone', async (req, res) => {
    try {
        const { phone, messageId } = req.body;
        if (!phone || !messageId) return res.json({ success: false, error: "Phone and messageId required" });

        let cleanPhone = phone.replace(/\D/g, '');
        if (cleanPhone.length === 10) cleanPhone = "91" + cleanPhone;
        const jid = `${cleanPhone}@s.whatsapp.net`;

        if (waSocket) {
            await waSocket.sendMessage(jid, {
                delete: {
                    remoteJid: jid,
                    fromMe: true,
                    id: messageId
                }
            });

            db.run(`UPDATE messages SET body = '🚫 This message was deleted' WHERE id = ?`, [messageId]);

            res.json({ success: true, message: "Deleted for everyone successfully" });
        } else {
            res.json({ success: false, error: "WhatsApp not connected" });
        }
    } catch (error) {
        res.json({ success: false, error: error.message });
    }
});

app.get('/api/whatsapp/logs', (req, res) => {
    db.all(`SELECT * FROM wa_logs ORDER BY id DESC LIMIT 50`, [], (err, rows) => {
        if (err) res.json({ success: false, logs: [] });
        else res.json({ success: true, logs: rows });
    });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Shaney Baileys Microservice running on port ${PORT}`);
    connectToWhatsApp();
});