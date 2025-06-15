const fs = require("fs")
const path = require("path")
const crypto = require("crypto")
const get_timestamp = require("../raft/get_timestamp")


// lines_to_write is an array of lines assembled exactly as they 
// are appended to the log, its usually just one line in the array
function write_to_hash_table(
	hash_table_file_descriptor, lines_to_write, HASH_TABLE_CONSTANTS, RAFT_LOG_CONSTANTS
){

	console.log("-".repeat(40) + " WRITING TO HASH TABLE " + "-".repeat(40))
	for(let i = 0; i < lines_to_write.length; i++){

		let data_point = parse_line_of_log(lines_to_write[i], RAFT_LOG_CONSTANTS)

		let line_to_write = assemble_line_to_write(data_point, HASH_TABLE_CONSTANTS)

		console.log(`write_to_hash_table(): line_to_write: ${line_to_write}`)

		let key = data_point[2]
		
		// calculate the cell in the index section where the address of the data point will be stored
		let index_cell_position = find_index_cell_position(key, HASH_TABLE_CONSTANTS)

		// first verify if there is a hash collision or not
		let hash_collision_cell_value = index_cell_hash_collision(hash_table_file_descriptor, index_cell_position, HASH_TABLE_CONSTANTS)
		let is_hash_collision = hash_collision_cell_value[0]
		console.log(`write_to_hash_table(): parsed data_point:\n`, data_point, `\nkey: ${key}, index_cell_position: ${index_cell_position}, is_hash_collision: ${is_hash_collision}`)

		if (is_hash_collision == false && data_point[4] == "PRESENT"){

			let address_of_data_to_write = write_address_in_index_cell(
				hash_table_file_descriptor, index_cell_position, HASH_TABLE_CONSTANTS
			)

			write_data_in_hash_table(
				hash_table_file_descriptor, line_to_write, address_of_data_to_write
			)

		} else if (is_hash_collision == true){

			let address_at_index_cell = Number(hash_collision_cell_value[1])
			let address_new_write_or_update_data = find_address_of_writable_node_of_linked_list(hash_table_file_descriptor, address_at_index_cell, key, HASH_TABLE_CONSTANTS)
			
			let address_of_node_to_write = address_new_write_or_update_data[0]
			let new_write_or_update = address_new_write_or_update_data[1]
			let existing_data_line = address_new_write_or_update_data[2]

			if (new_write_or_update == "UPDATE"){

				update_existing_data_point(hash_table_file_descriptor, line_to_write, existing_data_line, address_of_node_to_write, HASH_TABLE_CONSTANTS)
			
			} else if (new_write_or_update == "NEW_WRITE" && data_point[4] == "PRESENT"){
				
				let address_of_latest_data_point = write_address_of_next_node(hash_table_file_descriptor, address_of_node_to_write, HASH_TABLE_CONSTANTS)
				write_data_in_hash_table(hash_table_file_descriptor, line_to_write, address_of_latest_data_point)
				console.log(`write_to_hash_table(): address_of_latest_data_point: ${address_of_latest_data_point}`)

			}
			console.log(`write_to_hash_table(): address_of_node_to_write: ${address_of_node_to_write}`)

		}
		console.log("=".repeat(100))
	}
}


function parse_line_of_log(line, RAFT_LOG_CONSTANTS){
	let log_start_index = 0
	let log_end_index = log_start_index + RAFT_LOG_CONSTANTS.LOG_INDEX_SIZE
	let _log_index = line.substring(log_start_index, log_end_index).trim()

	let term_start_index = log_end_index
	let term_end_index = term_start_index + RAFT_LOG_CONSTANTS.TERM_SIZE
	let _term = line.substring(term_start_index, term_end_index).trim()

	let key_start_index = term_end_index
	let key_end_index = key_start_index + RAFT_LOG_CONSTANTS.KEY_SIZE
	let _key = line.substring(key_start_index, key_end_index).trim()

	let value_start_index = key_end_index
	let value_end_index = value_start_index + RAFT_LOG_CONSTANTS.VALUE_SIZE
	let _value = line.substring(value_start_index, value_end_index).trim()

	let live_status_start_index = value_end_index
	let live_status_end_index = live_status_start_index + RAFT_LOG_CONSTANTS.LIVE_STATUS_SIZE
	let _live_status = line.substring(live_status_start_index, live_status_end_index).trim()

	return [_log_index, _term, _key, _value, _live_status]
}


// data_point: [_log_index, _term, _key, _value, _live_status]
function assemble_line_to_write(data_point, HASH_TABLE_CONSTANTS){
	
	let term = data_point[1]	// not needed
	
	let version_number = data_point[0] 			// log_index will be the version_number, already a string
	let version_number_num_blanks = HASH_TABLE_CONSTANTS.VERSION_NUMBER_SIZE - version_number.length
	version_number += " ".repeat(version_number_num_blanks)

	let live_status = data_point[4]
	let live_status_num_blanks = HASH_TABLE_CONSTANTS.LIVE_STATUS_SIZE - live_status.length
	live_status += " ".repeat(live_status_num_blanks)

	let key = data_point[2]
	let key_num_blanks = HASH_TABLE_CONSTANTS.KEY_SIZE - key.length
	key += " ".repeat(key_num_blanks)

	let value = data_point[3]
	let value_num_blanks = HASH_TABLE_CONSTANTS.VALUE_SIZE - value.length
	value += " ".repeat(value_num_blanks)

	let next_pointer = "null"
	let next_pointer_num_blanks = HASH_TABLE_CONSTANTS.NEXT_NODE_POINTER_SIZE - next_pointer.length
	next_pointer += " ".repeat(next_pointer_num_blanks)

	return "\n" + version_number + live_status + key + value + next_pointer

}


function find_index_cell_position(key, HASH_TABLE_CONSTANTS){
	let hash_function = crypto.createHash("sha256")
	hash_function.update(key)
	let hash_digest_hex = hash_function.digest("hex")	// hashed value in hexadecimal
	let hash_digest_decimal = parseInt(hash_digest_hex, 16) // hashed value in base10
	let index_cell = hash_digest_decimal % HASH_TABLE_CONSTANTS.HASH_TABLE_MAX_ENTRIES
	console.log(`find_index_cell_position(): index_cell: ${index_cell}`)
	let index_cell_position = HASH_TABLE_CONSTANTS.HASH_TABLE_HEADER_SIZE + (index_cell * HASH_TABLE_CONSTANTS.NUM_BYTES_PER_ADDRESS) + 1
	return index_cell_position
}


function index_cell_hash_collision(hash_table_file_descriptor, index_cell_position, HASH_TABLE_CONSTANTS){
	let index_cell_size = HASH_TABLE_CONSTANTS.NUM_BYTES_PER_ADDRESS
	let index_cell_read_buffer = Buffer.alloc(index_cell_size)
	let cell_index_num_bytes_read = fs.readSync(hash_table_file_descriptor, index_cell_read_buffer, 0, index_cell_size, index_cell_position)
	let index_cell_read = ""
	if (cell_index_num_bytes_read > 0){
		index_cell_read = index_cell_read_buffer.toString('utf-8').trim()
		console.log(`hash_collision_bool(): is index_cell_read already taken? index_cell_read.length ${index_cell_read.length}, index_cell_read: ${index_cell_read}`)
		if(index_cell_read.length == 0) return [false, index_cell_read]
	}
	return [true, index_cell_read]
}


// in this file ${hash_table_file_descriptor}, we write ${address_of_data_to_write}
// at position in the file ${index_cell_position}
function write_address_in_index_cell(
	hash_table_file_descriptor, index_cell_position, HASH_TABLE_CONSTANTS
){

	// because the new data is to be appended, we need to know where to start 
	// appending as the current address_of_data_to_write is the address that will be stored
	// in the index cell at index_cell_position
	let stats = fs.fstatSync(hash_table_file_descriptor)
	let address_of_data_to_write = String(stats.size)
	console.log(`write_address_in_index_cell(): address_of_data_to_write: ${address_of_data_to_write}`)

	let num_blank_spaces = HASH_TABLE_CONSTANTS.NUM_BYTES_PER_ADDRESS - address_of_data_to_write.length
	let full_address_string = address_of_data_to_write + " ".repeat(num_blank_spaces)
	let index_cell_buffer = Buffer.from(full_address_string)

	fs.writeSync(hash_table_file_descriptor, index_cell_buffer, 0, index_cell_buffer.byteLength, index_cell_position)

	return stats.size // numerical address_of_data_to_write
}


// in this file ${hash_table_file_descriptor}, we write ${line_to_write}
// at position in the file ${address_of_data_to_write}
function write_data_in_hash_table(hash_table_file_descriptor, line_to_write, address_of_data_to_write){
	let data_buffer = Buffer.from(line_to_write)
	fs.writeSync(hash_table_file_descriptor, data_buffer, 0, data_buffer.byteLength, address_of_data_to_write)
}


function find_address_of_writable_node_of_linked_list(hash_table_file_descriptor, address_at_index_cell, new_key, HASH_TABLE_CONSTANTS){
	let address_of_writable_node = "null"
	let current_data_point_address = address_at_index_cell
	let new_write_or_update = "NEW_WRITE"
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
			console.log("x".repeat(100))
			console.log(`find_address_of_writable_node_of_linked_list(): data_read.length: ${data_read.length}, next_node_address: ${next_node_address}, read_key: ${read_key}, data_read: ${data_read}`)
			console.log(`find_address_of_writable_node_of_linked_list(): next_node_address == "null": ${next_node_address == "null"}, read_key == new_key: ${read_key == new_key}`)
			console.log("x".repeat(100))
			if (read_key == new_key){
				address_of_writable_node = current_data_point_address
				new_write_or_update = "UPDATE"
				break
			} else if (next_node_address == "null"){
				address_of_writable_node = current_data_point_address
				break
			} else {
				current_data_point_address = Number(next_node_address)
			}
		} else {
			console.log(`find_address_of_writable_node_of_linked_list(): unable to traverse or continue traversing linked list`)
			break
		}
	}
	return [address_of_writable_node, new_write_or_update, data_read]
}


// updates an existing key value pair, only updating the version number and value
function update_existing_data_point(hash_table_file_descriptor, line_to_write, existing_data_line, address_of_writable_node, HASH_TABLE_CONSTANTS){
	
	let new_data_substring = line_to_write.substring(0, HASH_TABLE_CONSTANTS.DATA_POINT_SIZE - HASH_TABLE_CONSTANTS.NEXT_NODE_POINTER_SIZE)
	let existing_data_next_pointer = existing_data_line.substring(HASH_TABLE_CONSTANTS.DATA_POINT_SIZE - HASH_TABLE_CONSTANTS.NEXT_NODE_POINTER_SIZE)

	let updated_data_buffer = Buffer.from(new_data_substring + existing_data_next_pointer)
	
	fs.writeSync(hash_table_file_descriptor, updated_data_buffer, 0, updated_data_buffer.byteLength, address_of_writable_node)

	console.log(`new_data_substring + existing_data_next_pointer: ${new_data_substring + existing_data_next_pointer}`)
}


// writes address of next node's pointer
function write_address_of_next_node(hash_table_file_descriptor, address_of_last_node, HASH_TABLE_CONSTANTS){
	
	// assemble the address value that we're writing
	let stats = fs.fstatSync(hash_table_file_descriptor)
	let address_of_next_data_point = String(stats.size)
	let num_blank_spaces = HASH_TABLE_CONSTANTS.NEXT_NODE_POINTER_SIZE - address_of_next_data_point.length
	address_of_next_data_point += " ".repeat(num_blank_spaces)
	let next_node_address_buffer = Buffer.from(address_of_next_data_point)

	// where we're writing the data, e.g. address_of_last_node = x, then x + 100 - 10
	let address_write_location = address_of_last_node + HASH_TABLE_CONSTANTS.DATA_POINT_SIZE - HASH_TABLE_CONSTANTS.NEXT_NODE_POINTER_SIZE

	fs.writeSync(hash_table_file_descriptor, next_node_address_buffer, 0, next_node_address_buffer.byteLength, address_write_location)

	return stats.size // numerical address_of_next_data_point
}


function initialize_hash_table_file(hash_table_file_descriptor, HASH_TABLE_CONSTANTS){

	let hash_table_index_header = `///////////////////////////////////////// INDEX SECTION ////////////////////////////////////////////
 -------- -------- -------- -------- -------- --------
| cell 0 | cell 1 | cell 2 | cell 3 | cell 4 |   ...  |
 -------- -------- -------- -------- -------- --------\n\n`

	let hash_table_data_header = `///////////////////////////////////////// DATA SECTION /////////////////////////////////////////////\n\n`
	let hash_table_data_format = `------------------------------------------------------------------------------------------------------
verion_number 10 bytes | live_status 10 bytes | key 20 bytes | value 49 bytes | pointer 10 | \\n 1 byte
------------------------------------------------------------------------------------------------------\n`
	
	let stats = fs.fstatSync(hash_table_file_descriptor)
	let file_size = String(stats.size)

	// if file already has data don't do anything
	if (stats.size > 0) return 

	console.log(hash_table_index_header)
	console.log(hash_table_data_header)
	console.log(hash_table_data_format)

	let hash_table_index_buffer = Buffer.from(hash_table_index_header)
	let num_blank_spaces = HASH_TABLE_CONSTANTS.HASH_TABLE_MAX_ENTRIES * HASH_TABLE_CONSTANTS.NUM_BYTES_PER_ADDRESS
	let hash_table_index_spaces_buffer = Buffer.from(" ".repeat(num_blank_spaces) + "\n\n")
	let hash_table_data_header_buffer = Buffer.from(hash_table_data_header)
	let hash_table_data_format_buffer = Buffer.from(hash_table_data_format)

	fs.writeSync(hash_table_file_descriptor, hash_table_index_buffer, 0, hash_table_index_buffer.byteLength, file_size)

	stats = fs.fstatSync(hash_table_file_descriptor)
	file_size = String(stats.size)

	fs.writeSync(hash_table_file_descriptor, hash_table_index_spaces_buffer, 0, hash_table_index_spaces_buffer.byteLength, file_size)

	stats = fs.fstatSync(hash_table_file_descriptor)
	file_size = String(stats.size)

	fs.writeSync(hash_table_file_descriptor, hash_table_data_header_buffer, 0, hash_table_data_header_buffer.byteLength, file_size)

	stats = fs.fstatSync(hash_table_file_descriptor)
	file_size = String(stats.size)

	fs.writeSync(hash_table_file_descriptor, hash_table_data_format_buffer, 0, hash_table_data_format_buffer.byteLength, file_size)
}


module.exports = { write_to_hash_table, initialize_hash_table_file }