const http = require("http")
const net = require("net")

const get_timestamp = require("../raft/get_timestamp")

const { 
	find_leader, 
	handle_find_leader_results 
} = require("../raft/raft-leader/find_leader")

const send_write_to_leader = require("./handle_write_request")

const send_read_to_raft_cluster = require("./handle_read_request")

const ConsistentHashing = require("./consistent_hashing")


// curl post write/update request
// curl localhost:4000 -X POST -H 'Content-Type: application/json' -d '{ "method": "WRITE", "key": "key_1", "value": "value_1" }'

// curl delete request
// curl localhost:4000 -X POST -H 'Content-Type: application/json' -d '{ "method": "DELETE", "key": "key_1" }'

// curl get request
// curl localhost:4000 -X POST -H 'Content-Type: application/json' -d '{ "method": "READ", "key": "key_1" }'

const CURRENT_NODE_ADDRESS = "127.0.0.1:4000"

let RAFT_CLUSTER_1 = [
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3001 },
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3002 },
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3003 }
]

let RAFT_CLUSTER_2 = [
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3004 },
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3005 },
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3006 }
]

let RAFT_CLUSTER_3 = [
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3007 },
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3008 },
	{ "IP_ADDRESS": "127.0.0.1", "PORT": 3009 }
]

let consistent_hash_ring = ConsistentHashing()

consistent_hash_ring.add_cluster("RAFT_CLUSTER_1")
consistent_hash_ring.add_cluster("RAFT_CLUSTER_2")
consistent_hash_ring.add_cluster("RAFT_CLUSTER_3")


let LEADER_ADDRESSES = {
	"RAFT_CLUSTER_1": null,
	"RAFT_CLUSTER_2": null,
	"RAFT_CLUSTER_3": null
}

const database_server = http.createServer((request, response) => {

	if (request.method == "POST"){
		
		let body = ""
		request.on("data", (chunk) => { body += chunk })

		request.on("end", () => {

			try {
				const json_data = JSON.parse(body)

				console.log(`Received JSON:`, json_data)

				if (json_data.method == "READ"){

					console.log("+".repeat(100))

					let RAFT_CLUSTER = consistent_hash_ring.find_cluster(json_data.key)

					console.log(`==== ${json_data.method} REQUEST - key: ${json_data.key} - key_hash: ${consistent_hash_ring.get_hash(key)}, to RAFT_CLUSTER: ${RAFT_CLUSTER} ====`)

					send_read_to_raft_cluster(json_data.key, RAFT_CLUSTER, CURRENT_NODE_ADDRESS, response)

				} else if (json_data.method == "WRITE" || json_data.method == "DELETE"){

					let RAFT_CLUSTER = consistent_hash_ring.find_cluster(json_data.key)
					
					console.log(`==== ${json_data.method} REQUEST - key: ${json_data.key} - key_hash: ${consistent_hash_ring.get_hash(key)}, to RAFT_CLUSTER: ${RAFT_CLUSTER} ====`)

					send_write_to_leader(
						json_data, LEADER_ADDRESSES.RAFT_CLUSTER, CURRENT_NODE_ADDRESS, response
					).catch((error) => {	// error here is the input to the Promise's reject() function
							
						let cluster_cluster_new_leader = error
						
						if (LEADER_ADDRESSES.RAFT_CLUSTER == null || cluster_new_leader.length == 0){
							console.log(`Promise rejected because ${RAFT_CLUSTER} LEADER ${LEADER_ADDRESSES.RAFT_CLUSTER} could not be reached, finding the new leader at ${get_timestamp()}`)
							
							db_server_find_leader(RAFT_CLUSTER).then(() => {

								console.log(`------> The ${RAFT_CLUSTER} new LEADER is ${LEADER_ADDRESSES.RAFT_CLUSTER} at ${get_timestamp()}`)
								if (LEADER_ADDRESSES.RAFT_CLUSTER != null){
									send_write_to_leader(json_data, LEADER_ADDRESSES.RAFT_CLUSTER, CURRENT_NODE_ADDRESS, response)
								} else {
									response.writeHead(400, { 'Content-Type': 'application/json' })
									response.end(JSON.stringify({ error: 'Database Server - WRITE_FAILED' }))
								}
							})
							
						} else {
							console.log(`Promise rejected because leadership has changed in ${RAFT_CLUSTER} and the cluster's new LEADER is ${cluster_new_leader}`)
							LEADER_ADDRESSES.RAFT_CLUSTER = cluster_new_leader
							send_write_to_leader(json_data, LEADER_ADDRESSES.RAFT_CLUSTER, CURRENT_NODE_ADDRESS, response)
						}
					})

				}

			} catch (error) {
				response.writeHead(400, { 'Content-Type': 'application/json' })
				response.end(JSON.stringify({ error: 'Invalid JSON' }))
			}
		})

	} else {
		res.writeHead(404, { "Content-Type": "text/plain" })
		res.end("Not Found")
	}

})


async function db_server_find_leader(RAFT_CLUSTER){

	let client_promises = find_leader(RAFT_CLUSTER, CURRENT_NODE_ADDRESS)

	await Promise.allSettled(client_promises).then((all_promise_results) => {

		let cluster_leader_address = handle_find_leader_results(all_promise_results)
		
		console.log("#".repeat(100))
		console.log(`The leader for cluster ${RAFT_CLUSTER} is ${cluster_leader_address} at ${get_timestamp()}`)
		console.log("#".repeat(100))

		LEADER_ADDRESSES.RAFT_CLUSTER = cluster_leader_address

		console.log(`db_server_find_leader(): LEADER_ADDRESSES:\n`, LEADER_ADDRESSES) 

	}).catch((error) => {
		console.log(`db_server_find_leader(): Promise.allSettled() failed with error at ${get_timestamp()}\n`, error)
	})

}

db_server_find_leader(RAFT_CLUSTER_1)
db_server_find_leader(RAFT_CLUSTER_2)
db_server_find_leader(RAFT_CLUSTER_3)

database_server.listen(4000, "127.0.0.1", () => {
	console.log(`database server listening on address:`, database_server.address())
})