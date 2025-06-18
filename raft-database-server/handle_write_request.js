const net = require("net")
const get_timestamp = require("../raft/get_timestamp")


async function send_write_to_leader(json_data, LEADER_ADDRESS, CURRENT_NODE_ADDRESS, response, request_type){

	let client_response_payload = { "sender": CURRENT_NODE_ADDRESS }
	
	let write_promise_response = await send_write_to_leader_helper(json_data, LEADER_ADDRESS, CURRENT_NODE_ADDRESS, request_type)
	console.log(`DB SERVER - send_write_to_leader(): write_promise_response at ${get_timestamp()}: ${write_promise_response}`)
	client_response_payload["response"] = write_promise_response

	response.writeHead(200, { 'Content-Type': 'application/json' })
	response.end(JSON.stringify(client_response_payload))

	console.log("=".repeat(100))

}


function send_write_to_leader_helper(json_data, LEADER_ADDRESS, CURRENT_NODE_ADDRESS, request_type){

	let LEADER_IP = LEADER_ADDRESS.substring(0, LEADER_ADDRESS.length - 5)
	let LEADER_PORT = LEADER_ADDRESS.substring(LEADER_ADDRESS.length - 4)

	return new Promise((resolve, reject) => {

		const client_socket = new net.Socket()

		client_socket.setTimeout(1000)
		client_socket.on("timeout", () => { client_socket.destroy() })
			
		client_socket.connect(LEADER_PORT, LEADER_IP, () => {
			console.log(`DB SERVER - send_write_to_leader_helper(): sending write to raft leader ${LEADER_ADDRESS} at ${get_timestamp()}`)
			const payload = {
				"sender": `${CURRENT_NODE_ADDRESS}`,
				"message_type": `WRITE_REQUEST_TO_LEADER`,
				"key": json_data.key,
				"value": json_data.value || "",
				"request_type": json_data.request_type
			}
			client_socket.write(JSON.stringify(payload))
		})

		client_socket.on("data", (server_response) => {
			server_response = JSON.parse(server_response)
			console.log(`DB SERVER - send_write_to_leader_helper(): server_response received from server ${server_response.sender} at ${get_timestamp()}\n`, server_response)

			if (server_response.message_type == "REQUEST_REACHED_FOLLOWER"){
				let leader_address = server_response.message // empty string ("") if not leader
				if (leader_address.length > 0){
					console.log(`DB SERVER - send_write_to_leader_helper(): current leader is ${leader_address} at ${get_timestamp()}`)
				} else {
					console.log(`DB SERVER - send_write_to_leader_helper(): sender doesn't know the leader, message from ${server_response.sender} at ${get_timestamp()}`)
				}
				reject(leader_address)
			} else if (server_response.message_type == (`${json_data.request_type}_SUCCESS`)){
				resolve(`${json_data.request_type}_SUCCESS`)
			}

			client_socket.destroy()
		})

		client_socket.on("close", () => {
			// console.log(`DB SERVER - send_write_to_leader_helper(): client socket has been closed`)
		})

		client_socket.on("error", (error) => {
			console.error(`DB SERVER - send_write_to_leader_helper(): socket error while connecting to leader ${LEADER_ADDRESS} at ${get_timestamp()}`)
			reject("")
			client_socket.destroy()
		})
	})
}


module.exports = send_write_to_leader