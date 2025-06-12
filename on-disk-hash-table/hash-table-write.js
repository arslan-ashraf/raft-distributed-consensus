const fs = require("fs")
const path = require("path")
const crypto = require("crypto")
const get_timestamp = require("../get_timestamp")

const HASH_TABLE_SIZE = 3

// lines_to_write is an array of lines assembled exactly as they 
// are appended to the log, its usually just one line in the array
function write_to_hash_table(lines_to_write, NODE_ADDRESS, DATA_SIZE_CONSTANTS){

	let SERVER_PORT = NODE_ADDRESS.substring(NODE_ADDRESS.length - 4)
	const hash_table_file_path = path.join(process.cwd(), `${SERVER_PORT}-hash-table.txt`)

	for(let i = 0; i < lines_to_write.length; i++){

		let data_point = parse_line(lines_to_write[i], DATA_SIZE_CONSTANTS)

		let log_index = data_point[0]
		let term = data_point[1]
		let key = data_point[2]
		let value = data_point[3]
		
		let hash_function = crypto.createHash("sha256")
		hash_function.update(key)
		let result = hash_function.digest("hex")	// hashed value in hexadecimal
		result = parseInt(result, 16)				// hashed value in base10

	}
}


function parse_line(line, DATA_SIZE_CONSTANTS){
	let log_start_index = 1
	let log_end_index = log_start_index + DATA_SIZE_CONSTANTS.log_index_size
	let _log_index = line.substring(log_start_index, log_end_index).trim()

	let term_start_index = log_end_index
	let term_end_index = term_start_index + DATA_SIZE_CONSTANTS.term_size
	let _term = line.substring(term_start_index, term_end_index).trim()

	let key_start_index = term_end_index
	let key_end_index = key_start_index + DATA_SIZE_CONSTANTS.key_size
	let _key = line.substring(key_start_index, key_end_index).trim()

	let value_start_index = key_end_index
	let value_end_index = value_start_index + DATA_SIZE_CONSTANTS.value_size
	let _value = line.substring(value_start_index, value_end_index)

	return [_log_index, _term, _key, _value]
}


module.exports = write_to_hash_table