const fs = require("fs")
const path = require("path")
const crypto = require("crypto")


function read_from_hash_table(hash_table_file_descriptor, key_to_read, HASH_TABLE_CONSTANTS){
	let index_cell_position = find_index_cell_position(key_to_read, HASH_TABLE_CONSTANTS)

	let _does_key_exist_in_hash_table = does_key_exist_in_hash_table(hash_table_file_descriptor, index_cell_position, HASH_TABLE_CONSTANTS)
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

function does_key_exist_in_hash_table(hash_table_file_descriptor, index_cell_position, HASH_TABLE_CONSTANTS){
	let index_cell_size = HASH_TABLE_CONSTANTS.NUM_BYTES_PER_ADDRESS
	let index_read_buffer = Buffer.alloc(index_cell_size)
	let cell_index_num_bytes_read = fs.readSync(hash_table_file_descriptor, index_read_buffer, 0, index_cell_size, index_cell_position)
	let index_cell_read = ""
	if (cell_index_num_bytes_read > 0){
		index_cell_read = index_read_buffer.toString('utf-8').trim()
		console.log(`does_key_exist_in_hash_table(): is index_cell_read in the hash table? index_cell_read.length ${index_cell_read.length}, index_cell_read: ${index_cell_read}`)
		if(index_cell_read.length == 0) return [false, index_cell_read]
	}
	return [true, index_cell_read]
}


module.exports = read_from_hash_table