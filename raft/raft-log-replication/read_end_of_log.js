const fs = require("fs")
const path = require("path")

// read the log's last line
function read_last_line(fd, buffer_size){

	const stats = fs.fstatSync(fd)
	const file_size = stats.size
	console.log(`read_last_line(): file_size: ${file_size}`)
	const buffer = Buffer.alloc(buffer_size)
	let num_bytes_read = -1

	if (file_size - buffer_size >= 0){
		num_bytes_read = fs.readSync(fd, buffer, 0, buffer_size, file_size - buffer_size)
	}

	if (num_bytes_read > 0) {
		const data_read = buffer.toString('utf-8')
		return data_read
	}

	return ""
}


// get the log_index and term number out of the line in the log
function get_log_index_and_term(line, DATA_SIZE_CONSTANTS){

	let log_index_start = 1
	let log_index_end = log_index_start + DATA_SIZE_CONSTANTS.log_index_size
	let _log_index = Number(line.substring(log_index_start, log_index_end).trim())

	let term_index_start = log_index_end
	let term_index_end = term_index_start + DATA_SIZE_CONSTANTS.term_size
	let _term = Number(line.substring(term_index_start, term_index_end).trim())
	console.log(`get_log_index_and_term(): _log_index: ${_log_index}, _term: ${_term}`)
	return [_log_index, _term]
}


module.exports = { read_last_line, get_log_index_and_term }