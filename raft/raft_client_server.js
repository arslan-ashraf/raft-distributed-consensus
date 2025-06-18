const net = require("net")
const fs = require("fs")
const path = require("path")
const get_timestamp = require("./get_timestamp")

const { 
	find_leader, 
	handle_find_leader_results, 
	is_leader_present 
} = require("./raft-leader/find_leader")

const { 
	start_election, 
	handle_election_results, 
	vote_or_dont_vote, 
	inform_peers_of_election_victory 
} = require("./raft-leader/leader_election")

const send_heartbeat_to_followers = require("./raft-leader/send_heartbeat")

const { 
	write_and_replicate_write_to_followers, 
	catchup_followers_log_at_startup 
} = require("./raft-log-replication/log_replication")

const { 
	handle_write_to_follower, 
	handle_writes_to_lagging_followers,
	leader_read_missing_entries_on_follower,
	initialize_log_file
} = require("./raft-log-replication/handle_writes")

const { 
	read_last_line, 
	get_log_index_and_term 
} = require("./raft-log-replication/read_end_of_log")

const { initialize_hash_table_file } = require("../on-disk-hash-table/hash_table_write")

const { 
	read_from_hash_table, 
	assemble_read_payload 
} = require("../on-disk-hash-table/hash_table_read")

const SERVER_PORT = Number(process.argv[2])
const SERVER_IP_ADDRESS = "127.0.0.1"
const CURRENT_NODE_ADDRESS = `${SERVER_IP_ADDRESS}:${SERVER_PORT}`

const PEER1 = {
	"IP_ADDRESS": "127.0.0.1",
	"PORT": Number(process.argv[3])
}

const PEER2 = {
	"IP_ADDRESS": "127.0.0.1",
	"PORT": Number(process.argv[4])
}

const PEERS = [PEER1, PEER2]

let QUORUM = (PEERS.length + 1)/2
let EPSILON = 0.1 	// in case there are an even number of nodes in the cluster
if (QUORUM % 2 == 0){ QUORUM = QUORUM + EPSILON }

let CURRENT_NODE_STATE = "FOLLOWER"
let LEADER_ADDRESS = null
let LEADER_KNOWN = false

console.log("CURRENT_NODE_STATE:", CURRENT_NODE_STATE)
console.log("LEADER_ADDRESS:", LEADER_ADDRESS)

console.log(`current working dir: ${process.cwd()}`)
console.log(`parent of current working dir: ${path.dirname(process.cwd())}`)

// the log is an append only file each line in the log file is one entry and each entry is immutable
// log entries are of type: term, log_index, key, value, and live_status
// the first two are monotonically increasing, live_status tells us whether the key has been deleted
// in the hash table on disk, the version number is equal to the log_index to distinguish
// between new and stale writes on different nodes if followers have different
// values for the same key

// 100 bytes because that is the size of each log entry
let data_point_size = 100 		// data that will be appended to the log is this size

const RAFT_LOG_CONSTANTS = {
	"LOG_INDEX_SIZE": 15,
	"TERM_SIZE": 5,
	"KEY_SIZE": 20,
	"VALUE_SIZE": 49,
	"LIVE_STATUS_SIZE": 10
}

const cluster_number = Math.round(((SERVER_PORT + PEER1.PORT + PEER2.PORT) % 9000) / 6) - 1 || 1

const log_file_path = path.join(
	process.cwd(), 
	`raft-log-files`, 
	`raft-cluster-${String(cluster_number)}-log-file`, 
	`${SERVER_PORT}-log.txt`
)

const log_file_descriptor = fs.openSync(log_file_path, "rs+")
initialize_log_file(log_file_descriptor)

const HASH_TABLE_CONSTANTS = {
	"HASH_TABLE_MAX_ENTRIES": 5,
	"NUM_BYTES_PER_ADDRESS": 10,
	"HASH_TABLE_HEADER_SIZE": 267,
	"DATA_POINT_SIZE": 100,
	"VERSION_NUMBER_SIZE": 10,
	"LIVE_STATUS_SIZE": 10,
	"KEY_SIZE": 20,
	"VALUE_SIZE": 49,
	"NEXT_NODE_POINTER_SIZE": 10
}

const hash_table_file_path = path.join(
	path.dirname(process.cwd()), 
	`on-disk-hash-table`, 
	`raft-cluster-${String(cluster_number)}-hash-table`, 
	`${SERVER_PORT}-hash-table.txt`
)
const hash_table_file_descriptor = fs.openSync(hash_table_file_path, "rs+")
initialize_hash_table_file(hash_table_file_descriptor, HASH_TABLE_CONSTANTS)

let heartbeat_received = false

// these variables need to be stored on disk
let term = 0
let log_last_index = 0
let voted_for = null		// vote that a node can cast for another node

const server = net.createServer()

server.on("connection", (server_socket) => {

	// console.log(`Step 2 - Server: a connection from a client with address ${server_socket.remoteAddress}:${server_socket.remotePort}`)

	server_socket.on("data", (data) => {
		
		data = JSON.parse(data)
		const message_type = data.message_type

		if (message_type == "IS_LEADER_PRESENT_REQUEST"){	// Every live server receives this message

			// if (LEADER_KNOWN == true && LEADER_ADDRESS == CURRENT_NODE_ADDRESS){
			// 	CURRENT_NODE_STATE = "LEADER"
			// }

			let payload = is_leader_present(CURRENT_NODE_ADDRESS, CURRENT_NODE_STATE, LEADER_ADDRESS, data)
			server_socket.write(JSON.stringify(payload))

		} else if (message_type == "HEARTBEAT" && CURRENT_NODE_STATE == "FOLLOWER"){ // FOLLOWER receives this message

			console.log(`FOLLOWER ${CURRENT_NODE_ADDRESS} has received HEARTBEAT from LEADER ${data.sender} at ${get_timestamp()}`)
			heartbeat_received = true

		} else if (message_type == "VOTE_REQUEST" && data.node_state == "CANDIDATE"){	// Every live server receives this message

			console.log(`${CURRENT_NODE_STATE} ${CURRENT_NODE_ADDRESS} whose current term is ${term} has received a VOTE_REQUEST from ${data.node_state} ${data.sender} with current term ${data.term} at ${get_timestamp()}`)
			
			if (LEADER_KNOWN){
				console.log(`${CURRENT_NODE_ADDRESS} knows the leader is ${LEADER_ADDRESS} at ${get_timestamp()}`)
			} else {
				console.log(`${CURRENT_NODE_ADDRESS} does not know who the leader is at ${get_timestamp()}`)
			}

			// let last_term_in_log = log.length > 0 ? log[log.length - 1].term : 0
			// let vote_or_not = vote_or_dont_vote(
			// 	CURRENT_NODE_ADDRESS, CURRENT_NODE_STATE, term, log.length, last_term_in_log, voted_for,
			// 	data.sender, data.term, data.log_length, data.last_term_in_log, LEADER_KNOWN, heartbeat_received
			// )

			// log_last_index is the size of the log, same as log.length
			let vote_or_not = vote_or_dont_vote(
				CURRENT_NODE_ADDRESS, CURRENT_NODE_STATE, term, log_last_index, term, voted_for,
				data.sender, data.term, data.log_length, data.last_term_in_log, LEADER_KNOWN, heartbeat_received
			)

			const payload = vote_or_not[0]
			CURRENT_NODE_STATE = vote_or_not[1]
			term = vote_or_not[2]
			voted_for = vote_or_not[3]

			console.log(`${CURRENT_NODE_ADDRESS} voted for: ${voted_for} at ${get_timestamp()}`)
			console.log(`vote payload at ${get_timestamp()}\n`, payload)

			server_socket.write(JSON.stringify(payload))
		
		} else if (message_type == "LEADER_ELECTED"){	// FOLLOWER receives this message

			console.log(`${data.sender} has been elected LEADER at ${get_timestamp()}`)
			LEADER_ADDRESS = data.sender
			LEADER_KNOWN = true
			CURRENT_NODE_STATE = "FOLLOWER"
			heartbeat_received = true

		} else if (message_type == "WRITE_REQUEST_TO_LEADER"){	// LEADER receives this message

			let payload = { "sender": CURRENT_NODE_ADDRESS }

			// ensure request is indeed coming to the leader
			if (LEADER_KNOWN == true && CURRENT_NODE_ADDRESS == LEADER_ADDRESS){
				
				console.log(`=======> WRITE_REQUEST_TO_LEADER coming to ${CURRENT_NODE_ADDRESS} who is a ${CURRENT_NODE_STATE} at ${get_timestamp()} <=======`)

				log_last_index += 1
				let data_object = { "log_index": log_last_index, "term": term, "key": data.key, 
									"value": data.value, "request_type": data.request_type }

				console.log(`%%%%%%%% leaders term: ${term} $$$$$$$$$$$`)
				write_and_replicate_write_to_followers(
					PEERS, CURRENT_NODE_ADDRESS, data_object, QUORUM, server_socket, payload, 
					log_last_index, log_file_path, log_file_descriptor, data_point_size, RAFT_LOG_CONSTANTS, 
					hash_table_file_descriptor, HASH_TABLE_CONSTANTS
				)
				
			} else { // in case the write request is actually coming to a follower

				payload.message_type = "REQUEST_REACHED_FOLLOWER"
				payload.message = LEADER_ADDRESS
				server_socket.write(JSON.stringify(payload))

			}

		} else if (message_type == "REPLICATE_WRITE_TO_FOLLOWER"){	// FOLLOWER receives this message

			console.log(`${CURRENT_NODE_STATE} ${CURRENT_NODE_ADDRESS} received a REPLICATE_WRITE_TO_FOLLOWER message from LEADER ${data.sender} at ${get_timestamp()}`)
			
			let payload = handle_write_to_follower(
				CURRENT_NODE_ADDRESS, CURRENT_NODE_STATE, data, log_last_index, 
				log_file_path, hash_table_file_descriptor, RAFT_LOG_CONSTANTS,
				HASH_TABLE_CONSTANTS
			)

			log_last_index += 1
			server_socket.write(JSON.stringify(payload))
		
		} else if (message_type == "REPLICATE_WRITES_TO_LAGGING_FOLLOWER"){	// FOLLOWER receives this message

			console.log(`${CURRENT_NODE_STATE} ${CURRENT_NODE_ADDRESS} received a REPLICATE_WRITES_TO_LAGGING_FOLLOWER message from LEADER ${data.sender} at ${get_timestamp()}`)
	
			let handled_writes = handle_writes_to_lagging_followers(
				CURRENT_NODE_ADDRESS, data.missing_log_entries, log_file_path,
				RAFT_LOG_CONSTANTS, HASH_TABLE_CONSTANTS
			)

			let bool_writes_succeeded = handled_writes[0]
			
			let payload = { "sender": CURRENT_NODE_ADDRESS }

			if (bool_writes_succeeded == true){
				payload.message_type = "WRITES_SUCCESS_ON_LAGGING_FOLLOWER"
				let log_index_and_term = get_log_index_and_term(handled_writes[1], RAFT_LOG_CONSTANTS)
				log_last_index = log_index_and_term[0]
				term = log_index_and_term[1]
			} else {
				payload.message_type = "WRITES_FAILURE_ON_LAGGING_FOLLOWER"
			}
			
			console.log(`lagging FOLLOWER sending write success/failure with payload at ${get_timestamp()}\n`, payload)
			server_socket.write(JSON.stringify(payload))

		} else if (message_type == "CATCHUP_FOLLOWER_LOG_REQUEST"){	// LEADER receives this message

			console.log(`${CURRENT_NODE_STATE} ${CURRENT_NODE_ADDRESS} received a CATCHUP_FOLLOWER_LOG_REQUEST message from FOLLOWER ${data.sender} at ${get_timestamp()}`)
			
			let leader_log_incremented = false
			let missing_entries = leader_read_missing_entries_on_follower(log_file_descriptor, log_last_index, data.followers_log_last_index, data_point_size, leader_log_incremented)
			
			const payload = {
				"sender": CURRENT_NODE_ADDRESS,
				"message_type": "CATCHUP_FOLLOWER_LOG_RESPONSE",
				"missing_entries_in_log": missing_entries
			}

			server_socket.write(JSON.stringify(payload))

		} else if (message_type == "READ_KEY_REQUEST"){   // all raft servers receive this message

			console.log(`${CURRENT_NODE_STATE} ${CURRENT_NODE_ADDRESS} received a READ_KEY_REQUEST message from DB server ${data.sender} at ${get_timestamp()}`)

			let read_result = read_from_hash_table(hash_table_file_descriptor, data.key, HASH_TABLE_CONSTANTS)

			let payload = assemble_read_payload(read_result, CURRENT_NODE_ADDRESS)

			server_socket.write(JSON.stringify(payload))

		}

	})

	server_socket.on('end', () => {
		// console.log(`Step 8 - Server: client disconnected with address ${server_socket.remoteAddress}:${server_socket.remotePort}`)
		// console.log("-".repeat(100))
	})

	server_socket.on("error", (error) => {
		console.error(`Server: socket error\n`, error)
	})

})

server.listen(SERVER_PORT, SERVER_IP_ADDRESS, () => {
	console.log(`Server: listening on address:`, server.address())
})


// a node starts up or a whole cluster is brought up, the node tries to
// find the leader
if (CURRENT_NODE_STATE != "LEADER"){
	setTimeout(async () => {

		let client_promises = find_leader(PEERS, CURRENT_NODE_ADDRESS)

		await Promise.allSettled(client_promises).then((all_promise_results) => {

			LEADER_ADDRESS = handle_find_leader_results(all_promise_results)
			
			console.log("#".repeat(100))
			console.log(`Raft server: all_promise_results after all promises have been resolved at ${get_timestamp()}`) 
			console.log(`Raft server: every node agrees the leader is ${LEADER_ADDRESS} at ${get_timestamp()}`)
			console.log(`Raft server: Current node address: ${CURRENT_NODE_ADDRESS} and current node state: ${CURRENT_NODE_STATE} at ${get_timestamp()}`)
			console.log("#".repeat(100))
			
			let log_last_line = read_last_line(log_file_descriptor, data_point_size)
			console.log("T".repeat(100))
			console.log(log_last_line)
			let log_index_and_term = get_log_index_and_term(log_last_line, RAFT_LOG_CONSTANTS)
			log_last_index = Number.isNaN(log_index_and_term[0]) ? 0 : log_index_and_term[0]
			term = Number.isNaN(log_index_and_term[1]) ? 0 : log_index_and_term[1]
			console.log(`log_last_index: ${log_last_index}, term: ${term}`)
			console.log("T".repeat(100))

			if (LEADER_ADDRESS != null){

				let catchup_promise = catchup_followers_log_at_startup(
					CURRENT_NODE_ADDRESS, log_last_index, log_file_path, 
					LEADER_ADDRESS, RAFT_LOG_CONSTANTS, hash_table_file_descriptor, 
					HASH_TABLE_CONSTANTS
				)
				
				catchup_promise.then((promise_result) => {
					console.log(`**************** promise_result: ${promise_result} ************`)
					if (promise_result.length > 0){	
						log_index_and_term = get_log_index_and_term(promise_result, RAFT_LOG_CONSTANTS)
						log_last_index = log_index_and_term[0]
						term = log_index_and_term[1]
					}
					console.log(`After follower has started up and asked the leader to catch up, log_index_and_term: ${log_index_and_term} at ${get_timestamp()}`)
				})

			}

		}).catch((error) => {
			console.log(`Raft server: Promise.allSettled() for find_leader() failed with error at ${get_timestamp()}\n`, error)
		})
		

	}, 3000) // in milliseconds
}


// if a node hasn't found a leader or the cluster has started up
// and there is no leader, then the node will not receive a heartbeat
// so, a node/some nodes/all nodes must start an election

// leader sends periodic heartbeats and followers run a constantly 
// running background election timer which resets upon receiving a heartbeat
// or message from the leader

function send_heartbeats(){

	setInterval(() => {
		send_heartbeat_to_followers(PEERS, CURRENT_NODE_ADDRESS)
	}, 3000)

	CURRENT_NODE_STATE = "LEADER"
	console.log(`${'!'.repeat(10)}-> CURRENT_NODE_ADDRESS: ${CURRENT_NODE_ADDRESS} and CURRENT_NODE_STATE: ${CURRENT_NODE_STATE} with term: ${term}  at ${get_timestamp()}`)

}

let interval_id;

if (CURRENT_NODE_STATE == "LEADER"){

	send_heartbeats()

} else if (CURRENT_NODE_STATE == "FOLLOWER") {

	function receive_heartbeat_or_start_election(){

		let heartbeat_timeout = 4000 // in milliseconds

		if (heartbeat_received == true){

			// console.log(`Hearbeat received from leader whose address is ${LEADER_ADDRESS} at ${get_timestamp()}`)
			clearInterval(interval_id)

			interval_id = setInterval(receive_heartbeat_or_start_election, heartbeat_timeout)
			heartbeat_received = false

		} else {

			clearInterval(interval_id)
			LEADER_ADDRESS = null
			LEADER_KNOWN = false
			let random_milliseconds = Math.floor(Math.random() * 3000) // number between 0 and 1000
			
			console.log(`=====> Node with address ${CURRENT_NODE_ADDRESS} is about to start an election in ${random_milliseconds} milliseconds at ${get_timestamp()} <=====`)
			
			setTimeout(async () => {

				voted_for = CURRENT_NODE_ADDRESS
				let CURRENT_NODE_STATE = "CANDIDATE"

				// log_last_index is the same as log.length
				let vote_promises = start_election(
					PEERS, CURRENT_NODE_ADDRESS, CURRENT_NODE_STATE, LEADER_ADDRESS, 
					term + 1, log_last_index, term
				)

				let election_results;

				await Promise.allSettled(vote_promises).then((all_promise_results) => {
					
					election_results = handle_election_results(vote_promises, all_promise_results, CURRENT_NODE_STATE, QUORUM, EPSILON)
					console.log("$".repeat(100))
					console.log(`start_election() election over: vote promise results have all come back at ${get_timestamp()}`)

				}).catch((error) => {
					console.log(`start_election() election over: Promise.allSettled() failed with error at ${get_timestamp()}\n`, error)
				})

				console.log("x".repeat(25) + ` ${CURRENT_NODE_ADDRESS} election_results: ${election_results} ` + "x".repeat(25) + ` at ${get_timestamp()}`)
				console.log("$".repeat(100))
				if (election_results == "WON_ELECTION"){

					LEADER_ADDRESS = CURRENT_NODE_ADDRESS
					LEADER_KNOWN = true
					CURRENT_NODE_STATE = "LEADER"
					term += 1

					inform_peers_of_election_victory(PEERS, LEADER_ADDRESS, term)
					send_heartbeats()

				} else if (election_results == "LOST_ELECTION"){

					console.log(`LOST_ELECTION - Current node remains follower at ${get_timestamp()}`)
					CURRENT_NODE_STATE = "FOLLOWER"
					voted_for = null
					interval_id = setInterval(receive_heartbeat_or_start_election, heartbeat_timeout)

				} else if (election_results == "RESTART_ELECTION"){
					
					console.log(`RESTART_ELECTION - Election unresolved, current node restarting election at ${get_timestamp()}`)
					CURRENT_NODE_STATE = "FOLLOWER"
					voted_for = null
					interval_id = setInterval(receive_heartbeat_or_start_election, random_milliseconds)
					
				}

			}, random_milliseconds)
		}
	}

	interval_id = setInterval(receive_heartbeat_or_start_election, 4000)

}