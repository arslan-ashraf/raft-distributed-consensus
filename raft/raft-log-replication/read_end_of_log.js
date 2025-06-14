const fs = require("fs")
const path = require("path")

// read the log's last line
function read_last_line(fd, data_point_size){

	const stats = fs.fstatSync(fd)
	const file_size = stats.size
	console.log(`read_last_line(): file_size: ${file_size}`)
	const buffer = Buffer.alloc(data_point_size)
	let num_bytes_read = -1

	if (file_size - data_point_size >= 0){
		num_bytes_read = fs.readSync(fd, buffer, 0, data_point_size, file_size - data_point_size)
	}

	if (num_bytes_read > 0) {
		const data_read = buffer.toString('utf-8')
		return data_read
	}

	return ""
}


// get the log_index and term number out of the line in the log
function get_log_index_and_term(line, RAFT_LOG_CONSTANTS){

	let log_start_index = 0
	let log_end_index = log_start_index + RAFT_LOG_CONSTANTS.LOG_INDEX_SIZE
	let _log_index = Number(line.substring(log_start_index, log_end_index).trim())

	let term_start_index = log_end_index
	let term_end_index = term_start_index + RAFT_LOG_CONSTANTS.TERM_SIZE
	let _term = Number(line.substring(term_start_index, term_end_index).trim())
	
	console.log(`get_log_index_and_term(): _log_index: ${_log_index}, _term: ${_term}`)
	return [_log_index, _term]
}


module.exports = { read_last_line, get_log_index_and_term }