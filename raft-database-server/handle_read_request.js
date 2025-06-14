const net = require("net")
const get_timestamp = require("../raft/get_timestamp")


async function send_read_to_raft_cluster(key, RAFT_CLUSTER, CURRENT_NODE_ADDRESS, response){

	let client_response_payload = { "sender": CURRENT_NODE_ADDRESS }

	let read_request_promises = send_read_to_raft_cluster_promises(key, RAFT_CLUSTER, CURRENT_NODE_ADDRESS)

	await Promise.allSettled(read_request_promises).then((all_promise_results) => {
		
		console.log(`DB SERVER - send_read_to_raft_cluster(): all_promise_results at ${get_timestamp()}:`, all_promise_results)
		
		let raft_cluster_responses = []
		all_promise_results.map((promise_result) => {
			if (promise_result.value && promise_result.value.message_type == "KEY_FOUND"){
				raft_cluster_responses.push([promise_result.value.version_number, promise_result.value.value])
			} else if (promise_result.reason && promise_result.reason.message_type == "KEY_NOT_FOUND"){
				console.log(`DB SERVER - Raft node ${promise_result.reason.sender} could not find a matching key`)
			}
		})

		let value = find_value_with_highest_version(raft_cluster_responses, RAFT_CLUSTER.length)

		client_response_payload["response"] = value

		response.writeHead(200, { 'Content-Type': 'application/json' })
		response.end(JSON.stringify(client_response_payload))

		console.log("+".repeat(100))

	}).catch((error) => {
		console.log(`DB SERVER - send_read_to_raft_cluster(): Promise.allSettled() failed with error at ${get_timestamp()}\n`, error)

		response.writeHead(501, { 'Content-Type': 'text/plain' })
		response.end("Internal Server Error")
	})

}


function send_read_to_raft_cluster_promises(key, RAFT_CLUSTER, CURRENT_NODE_ADDRESS){

	let read_promises = []

	for(let i = 0; i < RAFT_CLUSTER.length; i++){
		let read_promise = send_read_request(key, RAFT_CLUSTER, CURRENT_NODE_ADDRESS, i)
		read_promise.then((promise_result) => {		// promise_result is the value passed to Promise's resolve() function
			console.log(`send_read_to_raft_cluster_promises(): read_promise then() function, value of promise_result at ${get_timestamp()}:\n`, promise_result)
		}).catch((error) => {
			console.error(`send_read_to_raft_cluster_promises(): promise error while connecting to node ${error.address}:${error.port} at ${get_timestamp()}`, error)
		})
		read_promises.push(read_promise)
	}

	return read_promises

}


function send_read_request(key, RAFT_CLUSTER, CURRENT_NODE_ADDRESS, index){

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
			console.log(`send_read_request(): server_response received at ${get_timestamp()}`)

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
			console.error(`send_read_request(): socket error error while connecting to node ${error.address}:${error.port} at ${get_timestamp()}\n`, error)
			reject(error)
		})
	})

	return read_promise

}


function find_value_with_highest_version(raft_cluster_responses, cluster_size){
	let largest_version_number = -1
	let value = ""
	
	let QUORUM = cluster_size/2
	let EPSILON = 0.1  // in case there are an even number of nodes in the cluster
	if (QUORUM % 2 == 0){ QUORUM = QUORUM + EPSILON }

	if (raft_cluster_responses.length > QUORUM){

		for (let i = 0; i < raft_cluster_responses.length; i++){

			if (raft_cluster_responses[i][0] > largest_version_number){
				largest_version_number = raft_cluster_responses[i][0]
				value = raft_cluster_responses[i][1]
			}

		}

	} else {
		value = "NO KEY/VALUE FOUND"
	}

	return value
}


module.exports = send_read_to_raft_cluster