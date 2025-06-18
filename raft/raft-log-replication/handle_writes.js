const fs = require("fs")
const get_timestamp = require("../get_timestamp")
const { write_to_hash_table } = require("../../on-disk-hash-table/hash_table_write")


// followers call this function
function handle_write_to_follower(
	CURRENT_NODE_ADDRESS, CURRENT_NODE_STATE, data, followers_log_last_index, 
	log_file_path, hash_table_file_descriptor, RAFT_LOG_CONSTANTS, 
	HASH_TABLE_CONSTANTS
){

	let payload = { "sender": CURRENT_NODE_ADDRESS }
	let leaders_log_last_index = data.log_index
	
	if (followers_log_last_index == (leaders_log_last_index - 1)){
		console.log(`---- Log indexes match - ${CURRENT_NODE_STATE} ${CURRENT_NODE_ADDRESS} accepts the write from LEADER ${data.sender} at ${get_timestamp()} ----`)
		
		const data_to_write = { "log_index": data.log_index, "term": data.term, "key": data.key, 
								"value": data.value, "request_type": data.request_type }

		/////////////////// FOLLOWER APPENDS TO THE LOG HERE ////////////////////
		const line_to_append = assemble_write(data_to_write, RAFT_LOG_CONSTANTS)

		append_writes_to_log([line_to_append], log_file_path, RAFT_LOG_CONSTANTS)

		/////////////////// FOLLOWER WRITES TO HASH TABLE HERE ////////////////////
		write_to_hash_table(hash_table_file_descriptor, [line_to_append], HASH_TABLE_CONSTANTS, RAFT_LOG_CONSTANTS)

		payload.message_type = "WRITE_SUCCESS_ON_FOLLOWER"
	} else {
		payload.message_type = "WRITE_FAILURE_ON_FOLLOWER"
		payload.followers_log_last_index = followers_log_last_index
	}
	return payload
}


// followers call this function
function handle_writes_to_lagging_followers(
	CURRENT_NODE_ADDRESS, missing_entries, follower_log_file_path,
	RAFT_LOG_CONSTANTS, HASH_TABLE_CONSTANTS
){

	console.log(`handle_writes_to_lagging_followers(): FOLLOWER ${CURRENT_NODE_ADDRESS} is missing the following entries at ${get_timestamp()}:\n`, missing_entries)
	console.log(`handle_writes_to_lagging_followers(): follower_log_length: ${follower_log_length} - data.num_missing_entries: ${data.num_missing_entries}`)
	
	/////////////////// LAGGING FOLLOWER APPENDS TO THE LOG HERE ////////////////////
	append_writes_to_log(missing_entries, follower_log_file_path)
	
	/////////////////// LAGGING FOLLOWER WRITES TO HASH TABLE HERE ////////////////////
	write_to_hash_table(hash_table_file_descriptor, missing_entries, HASH_TABLE_CONSTANTS, RAFT_LOG_CONSTANTS)

	console.log(`handle_writes_to_lagging_followers(): follower confirming that writes did not succeed`)
	return [true, missing_entries[missing_entries.length - 1]]
}


function append_writes_to_log(data_entries, log_file_path){
	
	const write_stream = fs.createWriteStream(log_file_path, { flags: "a"})
	
	for (let i = 0; i < data_entries.length; i++){	
		write_stream.write(data_entries[i])
	}

	write_stream.close()

	write_stream.on("error", (error) => {
		console.log(`append_writes_to_log(): failed to write data with error object: at ${get_timestamp()}\n`, error)
	})

}


function leader_read_missing_entries_on_follower(fd, leaders_log_last_index, followers_log_last_index, data_point_size, leader_log_incremented){
	
	console.log(`leader_read_missing_entries_on_follower(): leaders_log_last_index: ${leaders_log_last_index}, followers_log_last_index: ${followers_log_last_index}`)
	
	// num_missing_entries has a -= 1 leader_log_incremented is true, this means
	// its log_index was incremented before actually appending to log, hence -= 1
	// because a new write came in
	let num_missing_entries = leaders_log_last_index - followers_log_last_index
	if (leader_log_incremented == true) num_missing_entries -= 1
	const stats = fs.fstatSync(fd)
	const file_size = stats.size
	let buffer = Buffer.alloc(data_point_size)
	let file_offset = file_size - data_point_size
	let missing_entries = []

	for (let i = 0; i < num_missing_entries; i++){
		let num_bytes_read = fs.readSync(fd, buffer, 0, data_point_size, file_offset)
		if (num_bytes_read > 0) {
			let line_read = buffer.toString('utf-8')
			missing_entries.push(line_read)
			file_offset -= data_point_size
		}
	}

	// leader reads latest ones first, so missing_entries is sorted in reverse order
	return missing_entries.reverse()

}


function assemble_write(data_object, RAFT_LOG_CONSTANTS){

	let _log_index = String(data_object.log_index)
	let _log_index_num_spaces = RAFT_LOG_CONSTANTS.LOG_INDEX_SIZE - _log_index.length
	_log_index += " ".repeat(_log_index_num_spaces)
	
	let _term = String(data_object.term)
	let _term_num_spaces = RAFT_LOG_CONSTANTS.TERM_SIZE - _term.length
	_term += " ".repeat(_term_num_spaces)

	let _key = String(data_object.key)
	let _key_num_spaces = RAFT_LOG_CONSTANTS.KEY_SIZE - _key.length
	_key += " ".repeat(_key_num_spaces)

	let _value = String(data_object.value)
	let _value_num_spaces = RAFT_LOG_CONSTANTS.VALUE_SIZE - _value.length
	_value += " ".repeat(_value_num_spaces)

	let _live_status;
	if (data_object.request_type == "WRITE"){
		_live_status = "PRESENT"
	} else if (data_object.request_type == "DELETE") {
		_live_status = "DELETED"
	}
	let _live_status_num_spaces = RAFT_LOG_CONSTANTS.LIVE_STATUS_SIZE - _live_status.length
	_live_status += " ".repeat(_live_status_num_spaces)

	return _log_index + _term + _key + _value + _live_status + "\n"
}


function initialize_log_file(log_file_descriptor){

	let log_initial_text = ` --------------------------------------------------------------------------------------------------
| log_index 15 bytes | term 5 bytes | key 20 bytes | value 49 bytes | status 10 bytes | \\n 1 byte |
 --------------------------------------------------------------------------------------------------

log_index 	   term	key 				value 											 live_status\n\n`

	let stats = fs.fstatSync(log_file_descriptor)
	let file_size = String(stats.size)

	if (file_size == 0){
		console.log(`-------------- INITIALIZING LOG FILE ----------------`)
		let log_initial_text_buffer = Buffer.from(log_initial_text)
		fs.writeSync(log_file_descriptor, log_initial_text_buffer, 0, log_initial_text_buffer.byteLength, file_size)
	} else {
		console.log(`-------------- LOG FILE ALREADY INITIALIZED ---------------`)
	}

}


module.exports = { 
	handle_write_to_follower, 
	handle_writes_to_lagging_followers, 
	append_writes_to_log,
	leader_read_missing_entries_on_follower,
	assemble_write,
	initialize_log_file
}