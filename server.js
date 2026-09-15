// WebRTC signaling relay for Breakroom Compilation's web lobby.
// Protocol (JSON text frames over a plain WebSocket):
//
// Client -> Server:
//   { type: "create_room" }
//   { type: "join_room", room }
//   { type: "offer" | "answer", room, target, sdp }
//   { type: "ice_candidate", room, target, media, index, name }
//   { type: "leave_room", room }
//
// Server -> Client:
//   { type: "room_created", room, peer_id }
//   { type: "room_joined", room, peer_id, peers: [int, ...] }
//   { type: "peer_joined", peer_id }
//   { type: "offer" | "answer", from, sdp }
//   { type: "ice_candidate", from, media, index, name }
//   { type: "peer_left", peer_id }
//   { type: "error", code }
//

//Tested with REnder web hosting
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 8; //TODO move this to config file eventually
const ROOM_TTL_MS = 10 * 60 * 1000;
const SWEEP_INTERVAL_MS = 60 * 1000;
const ROOM_CODE_ALPHABET = "23456789ABCDEFGHJKMNPQRSTUVWXYZ";
const ROOM_CODE_LENGTH = 5;

/** @type {Map<string, Room>} */
const rooms = new Map();

class Room {
	constructor(code) {
		this.code = code;
		this.sockets = new Map();
		this.nextId = 2; // host is always 1
		this.createdAt = Date.now();
		this.everHadJoiner = false;
	}
}

function generateRoomCode() {
	let code;
	do {
		code = "";
		for (let i = 0; i < ROOM_CODE_LENGTH; i++) {
			code += ROOM_CODE_ALPHABET[Math.floor(Math.random() * ROOM_CODE_ALPHABET.length)];
		}
	} while (rooms.has(code));
	return code;
}

function send(ws, message) {
	if (ws.readyState === ws.OPEN) {
		ws.send(JSON.stringify(message));
	}
}

function sendError(ws, code) {
	send(ws, { type: "error", code });
}

function handleCreateRoom(ws) {
	const code = generateRoomCode();
	const room = new Room(code);
	room.sockets.set(1, ws);
	ws.room = code;
	ws.peerId = 1;
	rooms.set(code, room);
	send(ws, { type: "room_created", room: code, peer_id: 1 });
}

function handleJoinRoom(ws, msg) {
	const room = rooms.get(msg.room);
	if (!room) {
		sendError(ws, "room_not_found");
		return;
	}
	if (room.sockets.size >= MAX_PLAYERS) {
		sendError(ws, "room_full");
		return;
	}
	const peerId = room.nextId++;
	const existingIds = [...room.sockets.keys()];
	room.sockets.set(peerId, ws);
	room.everHadJoiner = true;
	ws.room = room.code;
	ws.peerId = peerId;

	send(ws, { type: "room_joined", room: room.code, peer_id: peerId, peers: existingIds });
	for (const [id, sock] of room.sockets) {
		if (id !== peerId) {
			send(sock, { type: "peer_joined", peer_id: peerId });
		}
	}
}

function handleRelay(ws, msg) {
	const room = rooms.get(msg.room);
	if (!room || ws.peerId === undefined) {
		sendError(ws, "bad_request");
		return;
	}
	const targetWs = room.sockets.get(msg.target);
	if (!targetWs) {
		sendError(ws, "bad_request");
		return;
	}
	const payload = { ...msg, from: ws.peerId };
	delete payload.room;
	delete payload.target;
	send(targetWs, payload);
}

function removeFromRoom(ws) {
	if (ws.room === undefined || ws.peerId === undefined) {
		return;
	}
	const room = rooms.get(ws.room);
	if (!room) {
		return;
	}
	room.sockets.delete(ws.peerId);
	if (room.sockets.size === 0) {
		rooms.delete(room.code);
		return;
	}
	for (const sock of room.sockets.values()) {
		send(sock, { type: "peer_left", peer_id: ws.peerId });
	}
}

function sweepStaleRooms() {
	const now = Date.now();
	for (const [code, room] of rooms) {
		const stale = now - room.createdAt > ROOM_TTL_MS;
		if (stale && !room.everHadJoiner) {
			for (const sock of room.sockets.values()) {
				sock.close();
			}
			rooms.delete(code);
		}
	}
}

const server = http.createServer((_req, res) => {
	res.writeHead(200, { "Content-Type": "text/plain" });
	res.end("Breakroom Compilation signaling server\n");
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
	ws.on("message", (data) => {
		let msg;
		try {
			msg = JSON.parse(data.toString());
		} catch (_err) {
			sendError(ws, "bad_request");
			return;
		}
		switch (msg.type) {
			case "create_room":
				handleCreateRoom(ws);
				break;
			case "join_room":
				handleJoinRoom(ws, msg);
				break;
			case "offer":
			case "answer":
			case "ice_candidate":
				handleRelay(ws, msg);
				break;
			case "leave_room":
				removeFromRoom(ws);
				ws.room = undefined;
				ws.peerId = undefined;
				break;
			default:
				sendError(ws, "bad_request");
		}
	});

	ws.on("close", () => removeFromRoom(ws));
});

setInterval(sweepStaleRooms, SWEEP_INTERVAL_MS);

server.listen(PORT, () => {
	console.log(`Signaling server listening on port ${PORT}`);
});
