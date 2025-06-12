const fs = require("fs")
const get_timestamp = require("../get_timestamp")



// lines_to_write is an array of lines assembled exactly as they 
// are appended to the log, its usually just one line in the array
function write_to_hash_table(lines_to_write, NODE_ADDRESS, DATA_SIZE_CONSTANTS){

	const hash_table_file_path = path.join(process.cwd(), "raft-files", `${SERVER_PORT}-log.txt`)
}

