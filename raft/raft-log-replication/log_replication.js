const net = require("net")
const fs = require("fs")
const get_timestamp = require("../get_timestamp")
const { write_to_hash_table } = require("../../on-disk-hash-table/hash_table_write")

const { 
	append_writes_to_log, 
	leader_read_missing_entries_on_follower,
	assemble_write
} = require("./handle_writes")

let { get_log_index_and_term } = require("./read_end_of_log")


// only the leader calls this function and CURRENT_NODE_ADDRESS is the leader's address
async function write_and_replicate_write_to_followers(
	PEERS, CURRENT_NODE_ADDRESS, data_object, QUORUM, server_socket, payload, 
	log_last_index, leader_log_file_path, leader_log_file_descriptor, 
	data_point_size, RAFT_LOG_CONSTANTS, hash_table_file_descriptor, HASH_TABLE_CONSTANTS
){
	
	let write_successes = []
	let write_promises = write_to_all_followers_promises(PEERS, CURRENT_NODE_ADDRESS, data_object)

	await Promise.allSettled(write_promises).then((all_promise_results) => {

		console.log(`write_and_replicate_write_to_followers(): all_promise_results:`, all_promise_results)
		all_promise_results.map((promise_result) => {
			let lagging_followers = []
			if (promise_result.status == "rejected" && 
				promise_result.reason.log_last_index != undefined){ // this avoids connection errors
				let follower_info = {
					"follower_address": promise_result.reason.sender,
					"followers_log_last_index": promise_result.reason.log_last_index
				}
				lagging_followers.push(follower_info)
			} else if (promise_result.status == "fulfilled") {
				write_successes.push(promise_result.value)
			}
			if (lagging_followers.length > 0){
				let replicate_log_successes = replicate_writes_to_lagging_followers(
					lagging_followers, CURRENT_NODE_ADDRESS, log_last_index, 
					leader_log_file_descriptor, data_object, RAFT_LOG_CONSTANTS, data_point_size
				)
				replicate_log_successes.then((promise_result) => {
					console.log(`write_and_replicate_write_to_followers(): After replicating to lagging followers, promise_result: at ${get_timestamp()}\n`, promise_result)
					write_successes = write_successes.concat(replicate_log_successes)
					console.log(`write_and_replicate_write_to_followers(): After replicating to lagging followers, write_successes: ${write_successes} at ${get_timestamp()}`)
				}).catch((error) => {
					console.log(`write_and_replicate_write_to_followers(): After replicating to lagging followers, error: at ${get_timestamp()}\n`, error)
				})
			}
		})

	}).catch((error) => {
		console.log(`write_and_replicate_write_to_followers(): Promise.allSettled() failed with error at ${get_timestamp()}\n`, error)
	})

	console.log(`write_and_replicate_write_to_followers(): write_successes:`, write_successes)
	
	let num_writes_succeeded = 0

	write_successes.map((write_status) => { 
		if(write_status == "WRITE_SUCCESS_ON_FOLLOWER") num_writes_succeeded += 1 
	})

	console.log(`-----------> LEADER has received: number of followers who wrote: ${num_writes_succeeded} at ${get_timestamp()}`)
	if (num_writes_succeeded > (QUORUM - 1)){
		
		/////////////////// LEADER APPENDS TO THE LOG HERE ////////////////////
		const line_to_append = assemble_write(data_object, RAFT_LOG_CONSTANTS)
		append_writes_to_log([line_to_append], leader_log_file_path)
		
		console.log(`!!!!!!!!!!! QUORUM satisfied: leader has written to its log !!!!!!!!!!!`)
		payload.message_type = `${data_object.request_type}_SUCCESS`  	// either WRITE OR DELETE
	
		/////////////////// LEADER WRITES TO HASH TABLE HERE ////////////////////
		write_to_hash_table(hash_table_file_descriptor, [line_to_append], HASH_TABLE_CONSTANTS, RAFT_LOG_CONSTANTS)

	} else {
		payload.message_type = `${data_object.request_type}_FAILED`  	// either WRITE OR DELETE
	}

	server_socket.write(JSON.stringify(payload))

}


// only the leader calls this function (indirectly) and CURRENT_NODE_ADDRESS is the leader's address
function write_to_all_followers_promises(PEERS, CURRENT_NODE_ADDRESS, data_object){
	
	let write_promises = []

	for(let i = 0; i < PEERS.length; i++){
		let write_promise = replicate_write_to_follower_promise(PEERS, CURRENT_NODE_ADDRESS, data_object, i)
		write_promise.then((promise_result) => {		// promise_result is the value passed to Promise's resolve() function
			console.log(`write_to_all_followers_promises(): write_promise then() function, value of promise_result: ${promise_result} at ${get_timestamp()}`)
		}).catch((error) => {
			console.error(`write_to_all_followers_promises(): promise error while connecting to node ${error.address}:${error.port} at ${get_timestamp()}`)
		})
		write_promises.push(write_promise)
	}

	return write_promises
}


// only the leader calls this function (indirectly) and CURRENT_NODE_ADDRESS is the leader's address
function replicate_write_to_follower_promise(PEERS, CURRENT_NODE_ADDRESS, data_object, index){

	let write_promise = new Promise((resolve, reject) => {

		const client_socket = new net.Socket()

		client_socket.setTimeout(4000)
		client_socket.on("timeout", () => { client_socket.destroy() })
		
		client_socket.connect(PEERS[index].PORT, PEERS[index].IP_ADDRESS, () => {
			console.log(`replicate_write_to_follower_promise(): connected to raft follower with address ${PEERS[index].IP_ADDRESS}:${PEERS[index].PORT} at ${get_timestamp()}`)
			const payload = { 
				"sender": `${CURRENT_NODE_ADDRESS}`,
				"message_type": "REPLICATE_WRITE_TO_FOLLOWER",
				"term": data_object.term,
				"log_index": data_object.log_index,
				"key": data_object.key,
				"value": data_object.value,
				"request_type": data_object.request_type
			}
			client_socket.write(JSON.stringify(payload))
		})

		client_socket.on("data", (server_response) => {
			server_response = JSON.parse(server_response)
			console.log(`replicate_write_to_follower_promise(): server_response received at ${get_timestamp()}\n`, server_response)

			if (server_response.message_type == "WRITE_SUCCESS_ON_FOLLOWER"){
				console.log(`replicate_write_to_follower_promise(): write succeeded!`)
				resolve("WRITE_SUCCESS_ON_FOLLOWER")
			} else if (server_response.message_type == "WRITE_FAILURE_ON_FOLLOWER"){
				console.log(`replicate_write_to_follower_promise(): write failed!`)
				let rejection_response = { 
					"sender": server_response.sender,
					"message_type": server_response.message_type,
					"log_last_index": server_response.followers_log_last_index 
				}
				reject(rejection_response)
			}

			client_socket.destroy()
		})

		client_socket.on("close", () => {
			// console.log(`Step 7 - replicate_write_to_follower_promise(): client socket has been closed`)
		})

		client_socket.on("error", (error) => {
			console.error(`replicate_write_to_follower_promise(): socket error at ${get_timestamp()} with error object:`, error)
			reject(error)
		})
	})

	return write_promise
}


// only the leader calls this function and CURRENT_NODE_ADDRESS is the leader's address

// if leader sends a write to followers, but some follower's log is behind the leader's
// then the follower will send its highest log index, letting the leader know
// how far behind it is, the leader then sends another write to followers that are behind
// with all of the log entries the lagging follower(s) is missing
async function replicate_writes_to_lagging_followers(
	lagging_followers, CURRENT_NODE_ADDRESS, log_last_index, 
	leader_log_file_descriptor, data_object, RAFT_LOG_CONSTANTS, data_point_size
){
	
	let replicate_log_successes = []
	let replicate_log_promises = gather_writes_to_lagging_followers_promises(
		lagging_followers, CURRENT_NODE_ADDRESS, log_last_index, 
		leader_log_file_descriptor, data_object, RAFT_LOG_CONSTANTS, data_point_size
	)

	await Promise.allSettled(replicate_log_promises).then((all_promise_results) => {

		console.log(`replicate_writes_to_lagging_followers(): all_promise_results:`, all_promise_results)
		all_promise_results.map((promise_result) => {
			if (promise_result.status == "fulfilled") {
				replicate_log_successes.push(promise_result.value)
			}
		})

	}).catch((error) => {
		console.log(`replicate_writes_to_lagging_followers(): Promise.allSettled() failed with error at ${get_timestamp()}\n`, error)
	})

	return replicate_log_successes
}


// only the leader calls this function (indirectly) and CURRENT_NODE_ADDRESS is the leader's address
function gather_writes_to_lagging_followers_promises(
	lagging_followers, CURRENT_NODE_ADDRESS, log_last_index, 
	leader_log_file_descriptor, data_object, RAFT_LOG_CONSTANTS, data_point_size
){
	
	let replicate_log_promises = []

	for(let i = 0; i < lagging_followers.length; i++){
		let replicate_log_promise = replicate_writes_to_lagging_follower_helper(
			lagging_followers, CURRENT_NODE_ADDRESS, log_last_index, 
			leader_log_file_descriptor, data_object, RAFT_LOG_CONSTANTS, 
			data_point_size, i
		)
		replicate_log_promise.then((promise_result) => {		// promise_result is the value passed to Promise's resolve() function
			console.log(`gather_writes_to_lagging_followers_promises(): replicate_log_promise then() function, value of promise_result: ${promise_result} at ${get_timestamp()}`)
		}).catch((error) => {
			console.error(`gather_writes_to_lagging_followers_promises(): promise error while connecting to node ${error.address}:${error.port} at ${get_timestamp()} with error object\n`, error)
		})
		replicate_log_promises.push(replicate_log_promise)
	}

	return replicate_log_promises
}


// only the leader calls this function (indirectly) and CURRENT_NODE_ADDRESS is the leader's address
function replicate_writes_to_lagging_follower_helper(
	lagging_followers, CURRENT_NODE_ADDRESS, log_last_index, 
	leader_log_file_descriptor, data_object, RAFT_LOG_CONSTANTS, 
	data_point_size, index
){

	let lagging_follower_address = lagging_followers[index].follower_address
	let lagging_follower_log_last_index = lagging_followers[index].followers_log_last_index
	let lagging_follower_ip_address = lagging_follower_address.substring(0, lagging_follower_address.length - 5)
	let lagging_follower_port = lagging_follower_address.substring(lagging_follower_address.length - 4)
	
	let replicate_log_promise = new Promise((resolve, reject) => {

		const client_socket = new net.Socket()

		client_socket.setTimeout(4000)
		client_socket.on("timeout", () => { client_socket.destroy() })

		client_socket.connect(lagging_follower_port, lagging_follower_ip_address, () => {
			console.log(`replicate_writes_to_lagging_follower_helper(): connected to lagging follower with address ${lagging_follower_address} at ${get_timestamp()}`)

			let leader_log_incremented = true
			let missing_entries = leader_read_missing_entries_on_follower(leader_log_file_descriptor, log_last_index, lagging_follower_log_last_index, data_point_size, leader_log_incremented)
			
			let most_recent_write = assemble_write(data_object, RAFT_LOG_CONSTANTS)
			missing_entries.push(most_recent_write)

			const payload = { 
				"sender": `${CURRENT_NODE_ADDRESS}`,
				"message_type": "REPLICATE_WRITES_TO_LAGGING_FOLLOWER",
				"num_missing_entries": log_last_index - lagging_follower_log_last_index,
				"missing_log_entries": missing_entries
			}

			console.log(`LEADER ${CURRENT_NODE_ADDRESS} sending the following payload to lagging follower at ${get_timestamp()}:\n`, payload)

			client_socket.write(JSON.stringify(payload))
		})

		client_socket.on("data", (server_response) => {
			server_response = JSON.parse(server_response)
			console.log(`replicate_writes_to_lagging_follower_helper(): server_response received at ${get_timestamp()}\n`, server_response)

			if (server_response.message_type == "WRITES_SUCCESS_ON_LAGGING_FOLLOWER"){
				console.log(`replicate_writes_to_lagging_follower_helper(): all writes succeeded on lagging follower!`)
				resolve("WRITE_SUCCESS_ON_FOLLOWER")
			} else if (server_response.message_type == "WRITES_FAILURE_ON_LAGGING_FOLLOWER"){
				console.log(`replicate_writes_to_lagging_follower_helper(): writes failed on lagging follower!`)
				let rejection_response = { 
					"sender": server_response.sender,
					"message_type": server_response.message_type,
				}
				reject(rejection_response)
			}

			client_socket.destroy()
		})

		client_socket.on("close", () => {
			// console.log(`Step 7 - replicate_writes_to_lagging_follower_helper(): client socket has been closed`)
		})

		client_socket.on("error", (error) => {
			console.error(`replicate_writes_to_lagging_follower_helper(): socket error error while connecting to node ${error.address}:${error.port} at ${get_timestamp()}`)
			reject(error)
		})
	})

	return replicate_log_promise
}


// only a follower calls this function at startup if the follower
// discovers there is already an existing leader
async function catchup_followers_log_at_startup(
	follower_address, followers_log_last_index, follower_log_file_path, 
	LEADER_ADDRESS, RAFT_LOG_CONSTANTS, hash_table_file_descriptor, 
	HASH_TABLE_CONSTANTS
){

	try {
		let new_last_line = ""
		// promise_result is the value passed to Promise's resolve() function
		let promise_result = await catchup_follower_helper(follower_address, followers_log_last_index, LEADER_ADDRESS)
	
		console.log(`catchup_followers_log_at_startup(): value of promise_result at ${get_timestamp()}\n`, promise_result)
		
		// solves a bug in case catchup happens after a new write
		if (promise_result.length > 0){

			let log_index_and_term = get_log_index_and_term(promise_result[0], RAFT_LOG_CONSTANTS)
			let smallest_index_of_missing_entries = log_index_and_term[0]

			if(followers_log_last_index < smallest_index_of_missing_entries){

				/////////////////// STARTING FOLLOWER IN EXISTING CLUSTER APPENDS TO THE LOG HERE ////////////////////
				append_writes_to_log(promise_result, follower_log_file_path)

				/////////////////// STARTING FOLLOWER IN EXISTING CLUSTER WRITES TO HASH TABLE HERE ////////////////////
				write_to_hash_table(hash_table_file_descriptor, promise_result, HASH_TABLE_CONSTANTS, RAFT_LOG_CONSTANTS)

			}
			
			new_last_line = promise_result[promise_result.length - 1]
			console.log(`catchup_followers_log_at_startup(): followers log caught up with new_last_line at ${get_timestamp()}\n`, new_last_line)
		}
		return new_last_line
	} catch(error){
		console.error(`catchup_followers_log_at_startup(): promise error at ${get_timestamp()}`, error)
		return ""
	}

}

// follower calls this function (indirectly) at startup
function catchup_follower_helper(follower_address, followers_log_last_index, LEADER_ADDRESS){

	let leader_ip_address = LEADER_ADDRESS.substring(0, LEADER_ADDRESS.length - 5)
	let leader_port = LEADER_ADDRESS.substring(LEADER_ADDRESS.length - 4)

	let client_promise = new Promise((resolve, reject) => {

		const client_socket = new net.Socket()

		client_socket.setTimeout(4000)
		client_socket.on("timeout", () => { client_socket.destroy() })
		
		client_socket.connect(leader_port, leader_ip_address, () => {
			console.log(`catchup_follower_helper(): connected to raft leader with address ${LEADER_ADDRESS} at ${get_timestamp()}`)
			const payload = { 
				"sender": follower_address,
				"message_type": "CATCHUP_FOLLOWER_LOG_REQUEST",
				"followers_log_last_index": followers_log_last_index
			}
			client_socket.write(JSON.stringify(payload))
		})

		client_socket.on("data", (server_response) => {
			server_response = JSON.parse(server_response)
			console.log(`catchup_follower_helper(): server_response received with message_type: ${server_response.message_type} at ${get_timestamp()}\n`)

			if (server_response.message_type == "CATCHUP_FOLLOWER_LOG_RESPONSE"){
				const _missing_entries_in_log = server_response.missing_entries_in_log
				console.log(`catchup_follower_helper(): follower has received the missing entries from leader at ${get_timestamp()}`)
				resolve(_missing_entries_in_log)
			}

			client_socket.destroy()
		})

		client_socket.on("close", () => {
			// console.log(`Step 7 - catchup_follower_helper(): client socket has been closed`)
		})

		client_socket.on("error", (error) => {
			console.error(`catchup_follower_helper(): socket error error while connecting to node ${error.address}:${error.port} at ${get_timestamp()}`)
			reject(error)
		})
	})

	return client_promise
}

module.exports = { write_and_replicate_write_to_followers, catchup_followers_log_at_startup }