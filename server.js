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

function log(...args) {
	console.log(new Date().toISOString(), ...args);
}

function logError(...args) {
	console.error(new Date().toISOString(), ...args);
}

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
	log(`[create_room] room=${code} host_peer=1 from=${ws.remoteAddress}`);
	send(ws, { type: "room_created", room: code, peer_id: 1 });
}

function handleJoinRoom(ws, msg) {
	log(`[join_room] attempt room=${msg.room} from=${ws.remoteAddress}`);
	const room = rooms.get(msg.room);
	if (!room) {
		log(`[join_room] FAILED room=${msg.room} reason=room_not_found from=${ws.remoteAddress}`);
		sendError(ws, "room_not_found");
		return;
	}
	if (room.sockets.size >= MAX_PLAYERS) {
		log(`[join_room] FAILED room=${msg.room} reason=room_full size=${room.sockets.size} from=${ws.remoteAddress}`);
		sendError(ws, "room_full");
		return;
	}
	const peerId = room.nextId++;
	const existingIds = [...room.sockets.keys()];
	room.sockets.set(peerId, ws);
	room.everHadJoiner = true;
	ws.room = room.code;
	ws.peerId = peerId;

	log(`[join_room] OK room=${room.code} new_peer=${peerId} existing_peers=[${existingIds}] from=${ws.remoteAddress}`);
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
		logError(
			`[relay] FAILED type=${msg.type} room=${msg.room} reason=${!room ? "room_not_found" : "sender_has_no_peer_id"} from=${ws.remoteAddress}`
		);
		sendError(ws, "bad_request");
		return;
	}
	const targetWs = room.sockets.get(msg.target);
	if (!targetWs) {
		logError(
			`[relay] FAILED type=${msg.type} room=${room.code} from_peer=${ws.peerId} target_peer=${msg.target} reason=target_not_found from=${ws.remoteAddress}`
		);
		sendError(ws, "bad_request");
		return;
	}
	if (msg.type === "offer" || msg.type === "answer") {
		log(`[relay] ${msg.type} room=${room.code} from_peer=${ws.peerId} target_peer=${msg.target}`);
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
	log(
		`[disconnect] room=${ws.room} peer=${ws.peerId} remaining=${room.sockets.size} from=${ws.remoteAddress}`
	);
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
			log(`[sweep] closing stale unjoined room=${code} age_ms=${now - room.createdAt}`);
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

wss.on("connection", (ws, req) => {
	ws.remoteAddress = req.socket.remoteAddress;
	log(`[connect] from=${ws.remoteAddress}`);

	ws.on("message", (data) => {
		let msg;
		try {
			msg = JSON.parse(data.toString());
		} catch (err) {
			logError(`[parse] bad JSON from=${ws.remoteAddress} error=${err.message} raw=${data.toString().slice(0, 200)}`);
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
				logError(`[message] unknown type=${msg.type} from=${ws.remoteAddress}`);
				sendError(ws, "bad_request");
		}
	});

	ws.on("close", (code, reason) => {
		log(`[close] from=${ws.remoteAddress} room=${ws.room} peer=${ws.peerId} code=${code} reason=${reason.toString() || "(none)"}`);
		removeFromRoom(ws);
	});

	ws.on("error", (err) => {
		logError(`[socket_error] from=${ws.remoteAddress} room=${ws.room} peer=${ws.peerId} error=${err.message}`);
	});
});

setInterval(sweepStaleRooms, SWEEP_INTERVAL_MS);

server.listen(PORT, () => {
	console.log(`Signaling server listening on port ${PORT}`);
});
