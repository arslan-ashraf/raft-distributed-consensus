const net = require("net")
const get_timestamp = require("../get_timestamp")


function start_election(
	PEERS, CURRENT_NODE_ADDRESS, CURRENT_NODE_STATE, LEADER_ADDRESS, term,  log_length,  last_term_in_log
){
	
	if (LEADER_ADDRESS != null){
		console.log(`Canceling election, leader already elected and known with address ${LEADER_ADDRESS} at ${get_timestamp()}`)
		return []
	}

	console.log(`=====> Node with address ${CURRENT_NODE_ADDRESS} with term ${term} is starting an election now at ${get_timestamp()} <=====`)

	let vote_promises = []
	for (let i = 0; i < PEERS.length; i++){

		let vote_promise = vote_request(
			PEERS, CURRENT_NODE_ADDRESS, CURRENT_NODE_STATE, term, log_length, last_term_in_log, i
		)

		vote_promise.then((promise_result) => {	// promise_result is the input to Promise's resolve() function
			console.log(`start_election(): vote_promise then() function, value of promise_result: ${promise_result} at ${get_timestamp()}`)
		}).catch((error) => {
			console.error(`start_election(): promise error while connecting to node ${error.address}:${error.port} ${error} at ${get_timestamp()}`)
		})
		vote_promises.push(vote_promise)
	}
	return vote_promises
}


function vote_request(
	PEERS, CURRENT_NODE_ADDRESS, CURRENT_NODE_STATE, term, log_length, last_term_in_log, index
){
	let payload = {
		"sender": `${CURRENT_NODE_ADDRESS}`,
		"message_type": "VOTE_REQUEST",
		"node_state": CURRENT_NODE_STATE,
		"term": term,
		"log_length": log_length,
		"last_term_in_log": last_term_in_log,
	}

	payload = JSON.stringify(payload)

	let vote_promise = new Promise((resolve, reject) => {
		
		const client_socket = new net.Socket()

		client_socket.setTimeout(4000)

		client_socket.on("timeout", () => {
			// console.log(`vote_request(): timer has timed out! Closing socket`)
			client_socket.destroy()
		})
		
		client_socket.connect(PEERS[index].PORT, PEERS[index].IP_ADDRESS, () => {
			console.log(`vote_request(): connected to raft peer with address ${PEERS[index].IP_ADDRESS}:${PEERS[index].PORT} at ${get_timestamp()}`)
			client_socket.write(payload)
		})

		client_socket.on("data", (server_response) => {
			console.log(`vote_request(): ------- VOTE ------- response received from voting node at ${get_timestamp()}`)
			resolve(server_response)
		})

		client_socket.on("close", () => {
			// console.log(`vote_request(): VOTE_REQUEST sent, socket has been closed`)
		})

		client_socket.on("error", (error) => {
			console.error(`vote_request(): socket error with error code ${error.code} at ${get_timestamp()}`)
			reject(error)
			client_socket.destroy()
		})
	})
	return vote_promise
}


// CURRENT_NODE_... is deciding to vote on CANDIDATE_NODE_...
function vote_or_dont_vote(
	CURRENT_NODE_ADDRESS, 
	CURRENT_NODE_STATE, 
	CURRENT_NODE_TERM,
	CURRENT_NODE_LOG_LENGTH,
	CURRENT_NODE_LAST_TERM_IN_LOG,
	CURRENT_NODE_VOTED_FOR,
	CANDIDATE_NODE_ADDRESS, 
	CANDIDATE_NODE_TERM, 
	CANDIDATE_NODE_LOG_LENGTH, 
	CANDIDATE_NODE_LAST_TERM_IN_LOG,
	LEADER_KNOWN,
	heartbeat_received
){

	let voted_for = null
	let last_term = (CURRENT_NODE_LOG_LENGTH > 0) ? CURRENT_NODE_LAST_TERM_IN_LOG : 0

	if (CANDIDATE_NODE_TERM > CURRENT_NODE_TERM){
		CURRENT_NODE_TERM = CANDIDATE_NODE_TERM
		// CURRENT_NODE_STATE = "FOLLOWER" // not needed
	}

	let is_candidate_log_upto_date = false

	// check if candidate's latest log term number is larger than the current node's
	// latest log term number OR if the latest term numbers in both logs are the
	// same, then the candidate node's log is larger than the current node's log
	// the point is to check if the candidate's log is at least upto date 
	// with the current node's log so the current node may vote for the candidate
	if ((CANDIDATE_NODE_LAST_TERM_IN_LOG > CURRENT_NODE_LAST_TERM_IN_LOG) ||
		CANDIDATE_NODE_LAST_TERM_IN_LOG == CURRENT_NODE_LAST_TERM_IN_LOG &&
		CANDIDATE_NODE_LOG_LENGTH >= CURRENT_NODE_LOG_LENGTH){
		is_candidate_log_upto_date = true
	}

	let current_node_votes_for_candidate = "NO"
	
	// CURRENT_NODE_VOTED_FOR is either null or contains the address of a 
	// of a candidate that a node has previously voted for, each
	// node can only vote for one candidate in each term

	if (
			heartbeat_received == false && 
			LEADER_KNOWN == false &&
			CURRENT_NODE_STATE == "FOLLOWER" && 
			CANDIDATE_NODE_TERM == CURRENT_NODE_TERM &&
			is_candidate_log_upto_date == true && 
			CURRENT_NODE_VOTED_FOR == null
		){
		current_node_votes_for_candidate = "YES"
		CURRENT_NODE_VOTED_FOR = CANDIDATE_NODE_ADDRESS
	}

	const payload = {
		"sender": CURRENT_NODE_ADDRESS,
		"message_type": "VOTE_RESPONSE",
		"message": current_node_votes_for_candidate
	}

	return [payload, CURRENT_NODE_STATE, CURRENT_NODE_TERM, CURRENT_NODE_VOTED_FOR]

}


function handle_election_results(vote_promises, all_promise_results, CURRENT_NODE_STATE, QUORUM, EPSILON){

	if (vote_promises.length == 0) return "LOST_ELECTION"

	let election_results;
	let num_votes_in_favor = 1
	let num_votes_not_in_favor = 0
	
	all_promise_results.map((promise_result) => {

		if (promise_result.status == "fulfilled"){
			let promise_result_value = JSON.parse(promise_result.value.toString("utf-8"))
			if (promise_result_value.message == "YES"){
				num_votes_in_favor += 1
		 	} else if (promise_result_value.message == "NO"){
		 		num_votes_not_in_favor += 1
			}
		}
		
	})

	console.log("-".repeat(100))
	console.log(`num_votes_in_favor: ${num_votes_in_favor}`)
	console.log(`num_votes_not_in_favor: ${num_votes_not_in_favor}`)
	console.log("-".repeat(100))

	if (CURRENT_NODE_STATE == "CANDIDATE" && num_votes_in_favor > QUORUM){
		election_results = "WON_ELECTION"
	} else if (num_votes_not_in_favor >= (QUORUM - EPSILON)){
		election_results = "LOST_ELECTION"
	} else {
		election_results = "RESTART_ELECTION"
	}
	return election_results
}


function inform_peers_of_election_victory(PEERS, LEADER_ADDRESS, leaders_term){

	let payload = {
		"sender": LEADER_ADDRESS,
		"message_type": "LEADER_ELECTED",
		"leaders_term": leaders_term
	}

	payload = JSON.stringify(payload)

	for (let i = 0; i < PEERS.length; i++){
		let index = i

		const client_socket = new net.Socket()

		client_socket.setTimeout(4000)

		client_socket.on("timeout", () => {
			// console.log(`inform_peers_of_election_victory(): timer has timed out! Closing socket`)
			client_socket.destroy()
		})
		
		client_socket.connect(PEERS[index].PORT, PEERS[index].IP_ADDRESS, () => {
			console.log(
				`inform_peers_of_election_victory(): connected to raft peer with address ${PEERS[index].IP_ADDRESS}:${PEERS[index].PORT} at ${get_timestamp()}`
			)
			client_socket.write(payload)
		})

		client_socket.on("close", () => {
			// console.log(`inform_peers_of_election_victory(): leader elected message sent, socket has been closed`)
		})

		client_socket.on("error", (error) => {
			console.error(`inform_peers_of_election_victory(): socket error with error code ${error.code} at ${get_timestamp()}`)
			client_socket.destroy()
		})
	}
}

module.exports = { 
	start_election, 
	handle_election_results, 
	vote_or_dont_vote,
	inform_peers_of_election_victory 
}