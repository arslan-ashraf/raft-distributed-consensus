const fs = require("fs")
const path = require("path")
const crypto = require("crypto")
const get_timestamp = require("../get_timestamp")


const HASH_TABLE_CONSTANTS = {
	"HASH_TABLE_MAX_ENTRIES": 5,
	"NUM_BYTES_PER_ADDRESS": 10,
	"HASH_TABLE_HEADER_SIZE": 100,
	"VERSION_NUMBER_SIZE": 10,
	"LIVE_STATUS_SIZE": 10,
	"KEY_SIZE": 20,
	"VALUE_SIZE": 49,
	"NEXT_NODE_POINTER_SIZE": 10
}

// lines_to_write is an array of lines assembled exactly as they 
// are appended to the log, its usually just one line in the array
function write_to_hash_table(
	hash_table_file_descriptor, lines_to_write, HASH_TABLE_CONSTANTS, RAFT_LOG_CONSTANTS
){

	for(let i = 0; i < lines_to_write.length; i++){

		let data_point = parse_line(lines_to_write[i], RAFT_LOG_CONSTANTS)

		let key = data_point[2]
		
		// calculate the cell in the index section where the address of the data point will be stored
		let index_cell_position = find_index_cell_position(key, HASH_TABLE_CONSTANTS)

		// first verify if there is a hash collision or not
		let is_hash_collision = hash_collision_bool(hash_table_file_descriptor, index_cell_position, HASH_TABLE_CONSTANTS)
		console.log(`write_to_hash_table(): parsed data_point:\n`, data_point, `\nkey: ${key}, index_cell_position: ${index_cell_position}, is_hash_collision: ${is_hash_collision}`)

		if (is_hash_collision == false){

			// because the new data is to be appended, we need to know where to start 
			// appending as the current address_of_data_to_write is the address that will be stored
			// in the index cell at index_cell_position
			let stats = fs.fstatSync(hash_table_file_descriptor)
			let address_of_data_to_write = stats.size

			write_address_in_index_cell(
				hash_table_file_descriptor, String(address_of_data_to_write), index_cell_position, HASH_TABLE_CONSTANTS
			)

			let line_to_write = assemble_line_to_write(data_point, HASH_TABLE_CONSTANTS)

			write_data_in_hash_table(
				hash_table_file_descriptor, line_to_write, address_of_data_to_write
			)

			console.log(`write_to_hash_table(): line_to_write: ${line_to_write}`)

		}

	}
}


function parse_line(line, RAFT_LOG_CONSTANTS){
	let log_start_index = 1
	let log_end_index = log_start_index + RAFT_LOG_CONSTANTS.log_index_size
	let _log_index = line.substring(log_start_index, log_end_index).trim()

	let term_start_index = log_end_index
	let term_end_index = term_start_index + RAFT_LOG_CONSTANTS.term_size
	let _term = line.substring(term_start_index, term_end_index).trim()

	let key_start_index = term_end_index
	let key_end_index = key_start_index + RAFT_LOG_CONSTANTS.key_size
	let _key = line.substring(key_start_index, key_end_index).trim()

	let value_start_index = key_end_index
	let value_end_index = value_start_index + RAFT_LOG_CONSTANTS.value_size
	let _value = line.substring(value_start_index, value_end_index).trim()

	return [_log_index, _term, _key, _value]
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


function hash_collision_bool(hash_table_file_descriptor, index_cell_position, HASH_TABLE_CONSTANTS){
	let index_cell_size = HASH_TABLE_CONSTANTS.NUM_BYTES_PER_ADDRESS
	let index_read_buffer = Buffer.alloc(index_cell_size)
	let cell_index_num_bytes_read = fs.readSync(hash_table_file_descriptor, index_read_buffer, 0, index_cell_size, index_cell_position)

	if (cell_index_num_bytes_read > 0){
		let index_cell_read = index_read_buffer.toString('utf-8').trim()
		console.log(`hash_collision_bool(): is index_cell_read already taken? index_cell_read.length ${index_cell_read.length}, index_cell_read: ${index_cell_read}`)
		if(index_cell_read.length == 0) return false
	}
	return true
}


// in this file ${hash_table_file_descriptor}, we write ${address_of_data_to_write}
// at position in the file ${index_cell_position}
function write_address_in_index_cell(
	hash_table_file_descriptor, address_of_data_to_write, index_cell_position, HASH_TABLE_CONSTANTS
){
	let num_blank_spaces = HASH_TABLE_CONSTANTS.NUM_BYTES_PER_ADDRESS - address_of_data_to_write.length
	let full_address_string = address_of_data_to_write + " ".repeat(num_blank_spaces)
	let index_cell_buffer = Buffer.from(full_address_string)

	fs.writeSync(hash_table_file_descriptor, index_cell_buffer, 0, index_cell_buffer.byteLength, index_cell_position)
}


function assemble_line_to_write(data_point, HASH_TABLE_CONSTANTS){
	
	let term = data_point[1]	// not needed
	
	let log_index = data_point[0]
	let version_number = log_index 			// already a string
	let version_number_num_blanks = HASH_TABLE_CONSTANTS.VERSION_NUMBER_SIZE - version_number.length
	version_number += " ".repeat(version_number_num_blanks)

	let live_status = "true"
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


// in this file ${hash_table_file_descriptor}, we write ${line_to_write}
// at position in the file ${address_of_data_to_write}
function write_data_in_hash_table(hash_table_file_descriptor, line_to_write, address_of_data_to_write){
	let data_buffer = Buffer.from(line_to_write)
	fs.writeSync(hash_table_file_descriptor, data_buffer, 0, data_buffer.byteLength, address_of_data_to_write)
}



function initialize_hash_table_file(hash_table_file_path, hash_table_file_descriptor, HASH_TABLE_CONSTANTS){
	
	fs.writeFileSync(hash_table_file_path, "", (error) => { console.error(error) })

	let hash_table_index_header = "///////////////////////////////////////// INDEX SECTION ///////////////////////////////////////////\n\n"
	let hash_table_data_header = "///////////////////////////////////////// DATA SECTION ////////////////////////////////////////////\n\n"
	let hash_table_data_format = `---------------------------------------------------------------------------------------------------
 \\n 1 byte | verion_number 10 bytes | status 10 bytes | key 20 bytes | value 49 bytes | pointer 10 
---------------------------------------------------------------------------------------------------\n`

	console.log(hash_table_index_header)
	console.log(hash_table_data_header)
	console.log(hash_table_data_format)

	let hash_table_index_buffer = Buffer.from(hash_table_index_header)
	let num_blank_spaces = HASH_TABLE_CONSTANTS.HASH_TABLE_MAX_ENTRIES * HASH_TABLE_CONSTANTS.NUM_BYTES_PER_ADDRESS
	let hash_table_index_spaces_buffer = Buffer.from(" ".repeat(num_blank_spaces) + "\n\n")
	let hash_table_data_header_buffer = Buffer.from(hash_table_data_header)
	let hash_table_data_format_buffer = Buffer.from(hash_table_data_format)

	let stats = fs.fstatSync(hash_table_file_descriptor)
	let file_size = String(stats.size)

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