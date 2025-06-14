const http = require("http")
const net = require("net")

const get_timestamp = require("../raft/get_timestamp")

const { 
	find_leader, 
	handle_find_leader_results 
} = require("../raft/raft-leader/find_leader")

const send_write_to_leader = require("./handle_write_request")

// curl post request
// curl "localhost:4000" -X POST -H 'Content-Type: application/json' -d '{ "method": "write", "key": "key_1", "value": "value_1" }'

// curl get request


let RAFT_CLUSTER = [
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3001 },
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3002 },
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3003 }
]

const CURRENT_NODE_ADDRESS = "127.0.0.1:4000"

let LEADER_ADDRESS = null

const database_server = http.createServer((request, response) => {

	if (request.method == "POST"){
		
		let body = ''
		request.on('data', (chunk) => {
			body += chunk
		})

		request.on('end', () => {
			let client_response_payload = {}

			try {
				const json_data = JSON.parse(body)

				console.log('Received JSON:', json_data)
				console.log(`==== LEADER_ADDRESS: ${LEADER_ADDRESS}`)

				if(json_data.method == "write"){
					
					send_write_to_leader(
						json_data, LEADER_ADDRESS, CURRENT_NODE_ADDRESS, client_response_payload, response
					).catch((error) => {	// error here is the input to the Promise's reject() function
							
						let new_leader = error
						
						if (LEADER_ADDRESS == null || new_leader.length == 0){
							console.log(`Promise rejected because leader at address ${LEADER_ADDRESS} could not be reached, finding the new leader at ${get_timestamp()}`)
							
							db_server_find_leader().then(() => {

								console.log(`------> This should run after finding the new leader: ${LEADER_ADDRESS} at ${get_timestamp()}`)
								if (LEADER_ADDRESS != null){
									send_write_to_leader(json_data, LEADER_ADDRESS, CURRENT_NODE_ADDRESS, client_response_payload, response)
								} else {
									response.writeHead(400, { 'Content-Type': 'application/json' })
									response.end(JSON.stringify({ error: 'Database Server - WRITE_FAILED' }))
								}
							})
							
						} else {
							console.log(`Promise rejected because leadership has changed and the new leader is ${new_leader}`)
							LEADER_ADDRESS = new_leader
							send_write_to_leader(json_data, LEADER_ADDRESS, CURRENT_NODE_ADDRESS, client_response_payload, response)
						}
					})
				
				} else if (json_data.method == "read"){
					
				}

			} catch (error) {
				response.writeHead(400, { 'Content-Type': 'application/json' })
				response.end(JSON.stringify({ error: 'Invalid JSON' }))
			}
		})

	} else {
		res.writeHead(404, { 'Content-Type': 'text/plain' })
		res.end('Not Found')
	}

	// response.writeHead(200, { 'Content-Type': 'text/html' })
	// response.end(`<h1>Database server working</h1>`)

})


async function db_server_find_leader(){

	let client_promises = find_leader(RAFT_CLUSTER, CURRENT_NODE_ADDRESS)

	await Promise.allSettled(client_promises).then((all_promise_results) => {

		LEADER_ADDRESS = handle_find_leader_results(all_promise_results)
		
		console.log("#".repeat(100))
		console.log(`Step 9 - Raft server: all_promise_results after all promises have been resolved at ${get_timestamp()}`) 
		console.log(`Step 10 - Raft server: every node agrees the leader is ${LEADER_ADDRESS} at ${get_timestamp()}`)
		console.log("#".repeat(100))

	}).catch((error) => {
		console.log(`Raft server: Promise.allSettled() for find_leader() failed with error at ${get_timestamp()}\n`, error)
	})

}

db_server_find_leader()

database_server.listen(4000, "127.0.0.1", () => {
	console.log(`database server listening on address:`, database_server.address())
})