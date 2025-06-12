const fs = require("fs")
const get_timestamp = require("../get_timestamp")

// followers call this function
function handle_write_to_follower(CURRENT_NODE_ADDRESS, CURRENT_NODE_STATE, data, followers_log_last_index, log_file_path, DATA_SIZE_CONSTANTS){

	let payload = { "sender": CURRENT_NODE_ADDRESS }
	let leaders_log_last_index = data.log_index
	
	if (followers_log_last_index == (leaders_log_last_index - 1)){
		console.log(`---- Log indexes match - ${CURRENT_NODE_STATE} ${CURRENT_NODE_ADDRESS} accepts the write from LEADER ${data.sender} at ${get_timestamp()} ----`)
		const data_to_write = { "log_index": data.log_index, "term": data.term,
								"key": data.key, "value": data.value }
		append_writes_to_log([data_to_write], log_file_path, DATA_SIZE_CONSTANTS)
		payload.message_type = "WRITE_SUCCESS_ON_FOLLOWER"
	} else {
		payload.message_type = "WRITE_FAILURE_ON_FOLLOWER"
		payload.followers_log_last_index = followers_log_last_index
	}
	return payload
}


// followers call this function
function handle_writes_to_lagging_followers(CURRENT_NODE_ADDRESS, data, follower_log_length, follower_log_file_path, buffer_size){

	let missing_entries = data.missing_log_entries

	console.log(`handle_writes_to_lagging_followers(): FOLLOWER ${CURRENT_NODE_ADDRESS} is missing the following entries at ${get_timestamp()}:\n`, missing_entries)
	console.log(`handle_writes_to_lagging_followers(): follower_log_length: ${follower_log_length} - data.num_missing_entries: ${data.num_missing_entries}`)
	
	const follower_log_write_stream = fs.createWriteStream(follower_log_file_path, { flags: "a"})
	for (let i = 0; i < missing_entries.length; i++){	
		follower_log_write_stream.write(missing_entries[i])
	}
	follower_log_write_stream.close()

	follower_log_write_stream.on("error", (error) => {
		console.log(`handle_writes_to_lagging_followers(): failed to write missing data to FOLLOWER with error object: at ${get_timestamp()}\n`, error)
		return [false, missing_entries[missing_entries.length - 1]]
	})

	console.log(`handle_writes_to_lagging_followers(): follower confirming that writes did not succeed`)
	return [true, missing_entries[missing_entries.length - 1]]
}


function append_writes_to_log(data_entries, log_file_path, DATA_SIZE_CONSTANTS){
	
	const file = fs.createWriteStream(log_file_path, { flags: "a"})
	
	for (let i = 0; i < data_entries.length; i++){	
		
		let _log_index = String(data_entries[i].log_index)
		let _log_index_num_spaces = DATA_SIZE_CONSTANTS.log_index_size - _log_index.length
		_log_index += " ".repeat(_log_index_num_spaces)
		
		let _term = String(data_entries[i].term)
		let _term_num_spaces = DATA_SIZE_CONSTANTS.term_size - _term.length
		_term += " ".repeat(_term_num_spaces)

		let _key = String(data_entries[i].key)
		let _key_num_spaces = DATA_SIZE_CONSTANTS.key_size - _key.length
		_key += " ".repeat(_key_num_spaces)

		let _value = String(data_entries[i].value)
		let _value_num_spaces = DATA_SIZE_CONSTANTS.value_size - _value.length
		_value += " ".repeat(_value_num_spaces)

		let data = "\n" + _log_index + _term + _key + _value

		file.write(data)
		
	}

	file.close()
}


function leader_read_missing_entries_on_follower(fd, leaders_log_last_index, followers_log_last_index, buffer_size, leader_log_incremented){
	
	console.log(`leader_read_missing_entries_on_follower(): leaders_log_last_index: ${leaders_log_last_index}, followers_log_last_index: ${followers_log_last_index}`)
	
	// num_missing_entries has a -= 1 leader_log_incremented is true, this means
	// its log_index was incremented before actually appending to log, hence -= 1
	// because a new write came in
	let num_missing_entries = leaders_log_last_index - followers_log_last_index
	if (leader_log_incremented == true) num_missing_entries -= 1
	const stats = fs.fstatSync(fd)
	const file_size = stats.size
	let buffer = Buffer.alloc(buffer_size)
	let file_offset = file_size - buffer_size
	let missing_entries = []

	for (let i = 0; i < num_missing_entries; i++){
		let num_bytes_read = fs.readSync(fd, buffer, 0, buffer_size, file_offset)
		if (num_bytes_read > 0) {
			let line_read = buffer.toString('utf-8')
			missing_entries.push(line_read)
			file_offset -= buffer_size
		}
	}

	// leader reads latest ones first, so missing_entries is sorted in reverse order
	return missing_entries.reverse()

}


function assemble_write(data_object, DATA_SIZE_CONSTANTS){

	let _log_index = String(data_object.log_index)
	let _log_index_num_spaces = DATA_SIZE_CONSTANTS.log_index_size - _log_index.length
	_log_index += " ".repeat(_log_index_num_spaces)
	
	let _term = String(data_object.term)
	let _term_num_spaces = DATA_SIZE_CONSTANTS.term_size - _term.length
	_term += " ".repeat(_term_num_spaces)

	let _key = String(data_object.key)
	let _key_num_spaces = DATA_SIZE_CONSTANTS.key_size - _key.length
	_key += " ".repeat(_key_num_spaces)

	let _value = String(data_object.value)
	let _value_num_spaces = DATA_SIZE_CONSTANTS.value_size - _value.length
	_value += " ".repeat(_value_num_spaces)

	return "\n" + _log_index + _term + _key + _value
}


module.exports = { 
	handle_write_to_follower, 
	handle_writes_to_lagging_followers, 
	append_writes_to_log,
	leader_read_missing_entries_on_follower,
	assemble_write
}