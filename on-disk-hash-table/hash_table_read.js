const fs = require("fs")
const path = require("path")
const crypto = require("crypto")


function read_from_hash_table(hash_table_file_descriptor, key, HASH_TABLE_CONSTANTS){

	let key_found = false
	let version_number = -1
	let value = ""
	
	let index_cell_position = find_index_cell_position(key, HASH_TABLE_CONSTANTS)

	let hash_collision_and_search_address = index_cell_hash_collision(
		hash_table_file_descriptor, index_cell_position, HASH_TABLE_CONSTANTS
	)

	let is_hash_collision = hash_collision_and_search_address[0]  		// boolean
	let search_address = Number(hash_collision_and_search_address[1]) 	// either "" or address number

	// there must be a hash collision in the code if the key/value exists
	// but there might be a hash collision even if the key doesn't exist
	if (is_hash_collision == true){
		let key_found_and_data = find_matching_key_in_linked_list(
			hash_table_file_descriptor, key, search_address, HASH_TABLE_CONSTANTS
		)

		key_found = key_found_and_data[0]
		let data_point = key_found_and_data[1]

		if (key_found == true){
			let version_number_liveness_value = parse_version_number_liveness_value(
				data_point, HASH_TABLE_CONSTANTS
			)

			let liveness_status = version_number_liveness_value[1]

			if (liveness_status == "PRESENT"){
				version_number = version_number_liveness_value[0]
				value = version_number_liveness_value[2]
			} else if (liveness_status == "DELETED") {
				key_found = false
			}
		}
	}

	return [key_found, version_number, value]

}


// hashes the key, takes a modulo by max hash table size and calculates how 
// at what byte offset starting from the file should this key's index be stored
function find_index_cell_position(key, HASH_TABLE_CONSTANTS){
	let hash_function = crypto.createHash("sha256")
	hash_function.update(key)
	let hash_digest_hex = hash_function.digest("hex")	// hashed value in hexadecimal
	let hash_digest_decimal = parseInt(hash_digest_hex, 16) // hashed value in base10
	let index_cell = hash_digest_decimal % HASH_TABLE_CONSTANTS.HASH_TABLE_MAX_ENTRIES
	console.log(`find_index_cell_position(): index_cell: ${index_cell}`)
	let index_cell_position = HASH_TABLE_CONSTANTS.HASH_TABLE_HEADER_SIZE + (index_cell * HASH_TABLE_CONSTANTS.NUM_BYTES_PER_ADDRESS) + 1
	return index_cell_position  	// in bytes
}


function index_cell_hash_collision(hash_table_file_descriptor, index_cell_position, HASH_TABLE_CONSTANTS){
	let index_cell_size = HASH_TABLE_CONSTANTS.NUM_BYTES_PER_ADDRESS
	let index_cell_read_buffer = Buffer.alloc(index_cell_size)
	let cell_index_num_bytes_read = fs.readSync(hash_table_file_descriptor, index_cell_read_buffer, 0, index_cell_size, index_cell_position)
	let index_cell_read = ""
	if (cell_index_num_bytes_read > 0){
		index_cell_read = index_cell_read_buffer.toString('utf-8').trim()
		console.log(`does_key_exist_in_hash_table(): is index_cell_read in the hash table? index_cell_read.length ${index_cell_read.length}, index_cell_read: ${index_cell_read}`)
		if(index_cell_read.length == 0) return [false, index_cell_read]
	}
	return [true, index_cell_read]
}


// traverses the logical linked list to find matching key
function find_matching_key_in_linked_list(hash_table_file_descriptor, input_key, search_address, HASH_TABLE_CONSTANTS){

	let current_data_point_address = search_address
	let key_found = false
	let data_read = ""

	while(true){
		
		let _data_point_size = HASH_TABLE_CONSTANTS.DATA_POINT_SIZE
		let data_buffer = Buffer.alloc(_data_point_size)
		let num_bytes_read = fs.readSync(hash_table_file_descriptor, data_buffer, 0, _data_point_size, current_data_point_address)
		
		if(num_bytes_read > 0){
			data_read = data_buffer.toString('utf-8')
			let next_node_address = data_read.substring(data_read.length - HASH_TABLE_CONSTANTS.NEXT_NODE_POINTER_SIZE).trim()
			
			let key_start_index = HASH_TABLE_CONSTANTS.VERSION_NUMBER_SIZE + HASH_TABLE_CONSTANTS.LIVE_STATUS_SIZE
			let key_end_index = key_start_index + HASH_TABLE_CONSTANTS.KEY_SIZE
			let read_key = data_read.substring(key_start_index, key_end_index).trim()
			console.log(">".repeat(100))
			console.log(`find_matching_key_in_linked_list(): data_read.length: ${data_read.length}, next_node_address: ${next_node_address}, read_key: ${read_key}, data_read: ${data_read}`)
			console.log(`find_matching_key_in_linked_list(): next_node_address == "null": ${next_node_address == "null"}, read_key == input_key: ${read_key == input_key}`)
			console.log("<".repeat(100))
			if (read_key == input_key){
				key_found = true
				break
			} else if (next_node_address == "null"){
				key_found = false
				break
			} else {
				current_data_point_address = Number(next_node_address)
			}
		} else {
			console.log(`find_matching_key_in_linked_list(): unable to traverse or continue traversing linked list`)
			break
		}
	}

	return [key_found, data_read]

}


function parse_version_number_liveness_value(data_point, HASH_TABLE_CONSTANTS){
	let version_number_end_index = HASH_TABLE_CONSTANTS.VERSION_NUMBER_SIZE
	let version_number = Number(data_point.substring(0, version_number_end_index).trim())

	let liveness_status_start_index = version_number_end_index
	let liveness_status_end_index = liveness_status_start_index + HASH_TABLE_CONSTANTS.LIVE_STATUS_SIZE
	let liveness_status = data_point.substring(liveness_status_start_index, liveness_status_end_index).trim()

	let value_start_index = HASH_TABLE_CONSTANTS.VERSION_NUMBER_SIZE + HASH_TABLE_CONSTANTS.LIVE_STATUS_SIZE + HASH_TABLE_CONSTANTS.KEY_SIZE
	let value_end_index = value_start_index + HASH_TABLE_CONSTANTS.VALUE_SIZE
	let value = data_point.substring(value_start_index, value_end_index).trim()

	return [version_number, liveness_status, value]
}


function assemble_read_payload(read_result, CURRENT_NODE_ADDRESS){

	let payload = { "sender": CURRENT_NODE_ADDRESS }

	let key_found = read_result[0]
	let version_number = read_result[1]
	let value = read_result[2]

	if (key_found == true){

		payload.message_type = "KEY_FOUND"
		payload.version_number = version_number
		payload.value = value

	} else if (key_found == false){

		payload.message_type = "KEY_NOT_FOUND"

	}

	return payload

}


module.exports = { read_from_hash_table, assemble_read_payload }