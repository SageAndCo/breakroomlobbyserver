# Breakroom Compilation Signaling Server

Small Node.js WebSocket server that brokers WebRTC connection setup for the
game's Web export lobby. It only relays room lookups and SDP/ICE handshake
messages between two browsers - it never sees game traffic, which flows
peer-to-peer over WebRTC once the handshake completes.

Update config file with remote instance url `globals/network/network_config.tres`
(`signaling_server_url`).

## Run locally

```
cd signaling-server
npm install
npm start
```

Defaults to port 8080 (`ws://localhost:8080`). Override with `PORT=<port>`.
