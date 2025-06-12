const net = require("net")
const get_timestamp = require("../get_timestamp")


function find_leader(PEERS, CURRENT_NODE_ADDRESS){
	
	let client_promises = []

	for(let i = 0; i < PEERS.length; i++){
		let client_promise = find_leader_helper(PEERS, CURRENT_NODE_ADDRESS, i)
		client_promise.then((promise_result) => {		// promise_result is the value passed to Promise's resolve() function
			console.log(`find_leader(): client_promise then() function, value of promise_result: ${promise_result} at ${get_timestamp()}`)
		}).catch((error) => {
			console.error(`find_leader(): promise error while connecting to node ${error.address}:${error.port} at ${get_timestamp()}`)
		})
		client_promises.push(client_promise)
	}

	return client_promises
}


function find_leader_helper(PEERS, CURRENT_NODE_ADDRESS, index){

	let client_promise = new Promise((resolve, reject) => {

		const client_socket = new net.Socket()

		client_socket.setTimeout(4000)
		client_socket.on("timeout", () => { client_socket.destroy() })
		
		client_socket.connect(PEERS[index].PORT, PEERS[index].IP_ADDRESS, () => {
			console.log(`find_leader_helper(): connected to raft peer with address ${PEERS[index].IP_ADDRESS}:${PEERS[index].PORT} at ${get_timestamp()}`)
			const payload = { 
				"sender": `${CURRENT_NODE_ADDRESS}`,
				"message_type": "IS_LEADER_PRESENT_REQUEST",
			}
			client_socket.write(JSON.stringify(payload))
		})

		client_socket.on("data", (server_response) => {
			server_response = JSON.parse(server_response)
			console.log(`find_leader_helper(): server_response received at ${get_timestamp()}\n`, server_response)

			if (server_response.message_type == "IS_LEADER_PRESENT_RESPONSE"){
				const leader_address = server_response.message // empty string ("") if not leader
				if (leader_address.length > 0){
					console.log(`find_leader_helper(): current leader is ${leader_address} at ${get_timestamp()}`)
				} else {
					console.log(`find_leader_helper(): leader unknown, message from ${server_response.sender} at ${get_timestamp()}`)
				}
				resolve(leader_address)
			}

			client_socket.destroy()
		})

		client_socket.on("close", () => {
			// console.log(`Step 7 - find_leader_helper(): client socket has been closed`)
		})

		client_socket.on("error", (error) => {
			console.error(`find_leader_helper(): socket error error while connecting to node ${error.address}:${error.port} at ${get_timestamp()}`)
			reject(error)
		})
	})

	return client_promise
}


function handle_find_leader_results(all_promise_results){
	let leader_address = null
	all_promise_results.map((promise_result) => {
		// if leader is found, promise_result.value should contain
		// the leader's IP address and port
		let data = promise_result.value 
		if (data != undefined &&
			data.length > 0 &&
			net.isIPv4(data.substring(0, data.length - 5))){
			leader_address = data
		}
	})
	return leader_address
}


function is_leader_present(
	CURRENT_NODE_ADDRESS, 
	CURRENT_NODE_STATE, 
	LEADER_ADDRESS, 
	data
){
	console.log(`is_leader_present(): payload received at ${get_timestamp()}\n`, data)
	console.log(`is_leader_present(): responding to client with address ${data.sender} at ${get_timestamp()}`)

	const payload = { 
		"sender": `${CURRENT_NODE_ADDRESS}`,
		"message_type": "IS_LEADER_PRESENT_RESPONSE",
		"message": ""
	}

	if (CURRENT_NODE_STATE == "LEADER") {
		payload.message = LEADER_ADDRESS
		console.log(`is_leader_present(): leader node here, my address is ${CURRENT_NODE_ADDRESS} at ${get_timestamp()}`)
	} else {
		console.log(`is_leader_present(): follower node here, my address is ${CURRENT_NODE_ADDRESS} at ${get_timestamp()}`)
	}

	return payload
}


module.exports = { 
	find_leader, 
	handle_find_leader_results, 
	is_leader_present 
}