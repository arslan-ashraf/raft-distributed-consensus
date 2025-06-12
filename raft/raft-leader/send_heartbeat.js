const net = require("net")
const get_timestamp = require("../get_timestamp")

function send_heartbeat_to_followers(PEERS, CURRENT_NODE_ADDRESS){

	const payload = {
		"sender": `${CURRENT_NODE_ADDRESS}`,
		"message_type": "HEARTBEAT"
	}

	for(let i = 0; i < PEERS.length; i++){
		
		const client_socket = new net.Socket()

		client_socket.setTimeout(500)

		client_socket.on("timeout", () => {
			// console.log(`send_heartbeat_to_followers(): timer has timed out!  Closing  client socket at ${get_timestamp()}`)
			client_socket.destroy()
		})
		
		client_socket.connect(PEERS[i].PORT, PEERS[i].IP_ADDRESS, () => {
			console.log(
				// `send_heartbeat_to_followers(): connected to raft peer with address ${PEERS[i].IP_ADDRESS}:${PEERS[i].PORT} at ${get_timestamp()}`
			)
			client_socket.write(JSON.stringify(payload))
			console.log(`send_heartbeat_to_followers(): Heartbeat sent to node ${PEERS[i].IP_ADDRESS}:${PEERS[i].PORT} at ${get_timestamp()}`)
			client_socket.destroy()
		})

		client_socket.on("close", () => {
			// console.log(`send_heartbeat_to_followers(): heartbeat sent, socket has been closed at ${get_timestamp()}`)
		})

		client_socket.on("error", (error) => {
			console.error(`send_heartbeat_to_followers(): error while connecting to node ${error.address}:${error.port} at ${get_timestamp()}`)
		})
	}
}

module.exports = send_heartbeat_to_followers