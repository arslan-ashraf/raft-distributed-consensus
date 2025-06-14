const net = require("net")
const get_timestamp = require("../raft/get_timestamp")


async function send_read_to_raft_cluster(key, RAFT_CLUSTER, CURRENT_NODE_ADDRESS, response){

	let client_response_payload = {}
	let read_promise_response = await send_read_to_raft_cluster_helper(key, RAFT_CLUSTER, CURRENT_NODE_ADDRESS)
	console.log(`db-server send_read_to_raft_cluster(): read_promise_response at ${get_timestamp()}: ${read_promise_response}`)
	client_response_payload["response"] = read_promise_response

	response.readHead(200, { 'Content-Type': 'application/json' })
	response.end(JSON.stringify(client_response_payload))

}


function send_read_to_raft_cluster_helper(key, RAFT_CLUSTER, CURRENT_NODE_ADDRESS){

	let read_promises = []

	for(let i = 0; i < RAFT_CLUSTER.length; i++){
		let read_promise = send_read_request(key, RAFT_CLUSTER, CURRENT_NODE_ADDRESS, i)
		read_promise.then((promise_result) => {		// promise_result is the value passed to Promise's resolve() function
			console.log(`send_read_to_raft_cluster_helper(): read_promise then() function, value of promise_result: ${promise_result} at ${get_timestamp()}`)
		}).catch((error) => {
			console.error(`send_read_to_raft_cluster_helper(): promise error while connecting to node ${error.address}:${error.port} at ${get_timestamp()}`)
		})
		read_promises.push(read_promise)
	}

	return read_promises

}


function send_read_request(RAFT_CLUSTER, CURRENT_NODE_ADDRESS, data_object, index){

	let read_promise = new Promise((resolve, reject) => {

		const client_socket = new net.Socket()

		client_socket.setTimeout(4000)
		client_socket.on("timeout", () => { client_socket.destroy() })
		
		client_socket.connect(RAFT_CLUSTER[index].PORT, RAFT_CLUSTER[index].IP_ADDRESS, () => {
			console.log(`send_read_request(): connected to raft server with address ${RAFT_CLUSTER[index].IP_ADDRESS}:${RAFT_CLUSTER[index].PORT} at ${get_timestamp()}`)
			const payload = { 
				"sender": `${CURRENT_NODE_ADDRESS}`,
				"message_type": "READ_KEY_REQUEST",
				"key": key
			}
			client_socket.write(JSON.stringify(payload))
		})

		client_socket.on("data", (server_response) => {
			server_response = JSON.parse(server_response)
			console.log(`send_read_request(): server_response received at ${get_timestamp()}\n`, server_response)

			if (server_response.message_type == "KEY_FOUND"){

				console.log(`send_read_request(): KEY_FOUND`)

				let resolve_response = { 
					"sender": server_response.sender,
					"message_type": server_response.message_type,
					"version_number": server_response.version_number, 
					"value": server_response.value 
				}

				resolve(resolve_response)

			} else if (server_response.message_type == "KEY_NOT_FOUND"){
				console.log(`send_read_request(): KEY_NOT_FOUND`)

				let rejection_response = { 
					"sender": server_response.sender,
					"message_type": server_response.message_type,
				}

				reject(rejection_response)
			}

			client_socket.destroy()
		})

		client_socket.on("close", () => {
			// console.log(`Step 7 - send_read_request(): client socket has been closed`)
		})

		client_socket.on("error", (error) => {
			console.error(`send_read_request(): socket error error while connecting to node ${error.address}:${error.port} at ${get_timestamp()}`)
			reject(error)
		})
	})

	return read_promise

}


module.exports = send_read_to_raft_cluster