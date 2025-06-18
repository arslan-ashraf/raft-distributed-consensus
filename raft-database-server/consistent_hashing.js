const crypto = require("crypto")


class ConsistentHashing {
	
	constructor(){
		this.sorted_hashes = []
		this.hash_to_name_map = {}
	}

	get_hash(str){
		let hash_function = crypto.createHash("blake2b512")
		hash_function.update(str)
		let hash_digest_hex = hash_function.digest("hex")	// hashed value in hexadecimal
		let hash_digest_decimal = parseInt(hash_digest_hex, 16) // hashed value in base10
		let hashed_string = hash_digest_decimal % 10000
		return hashed_string	// an integer
	}

	add_cluster(cluster_name){
		let cluster_hash = this.get_hash(cluster_name)
		let insert_index = this.binary_search(cluster_hash,false)
		this.sorted_hashes.splice(insert_index, 0, cluster_hash)
		this.hash_to_name_map[cluster_hash] = cluster_name

		console.log(`add_cluster(): cluster_name: ${cluster_name}, cluster_hash: ${cluster_hash}, insert_index: ${insert_index}`)
		console.log(`add_cluster(): this.sorted_hashes:\n`, this.sorted_hashes)
		console.log(`add_cluster(): this.hash_to_name_map:\n`, this.hash_to_name_map)
		console.log(`add_cluster(): this.hash_to_name_map[cluster_hash]: ${this.hash_to_name_map[cluster_hash]}`)
	}

	remove_cluster(cluster_name){
		let cluster_hash = this.get_hash(cluster_name)
		let remove_index = this.binary_search(cluster_hash, false)
		this.sorted_hashes.splice(remove_index, 1)
		delete this.hash_to_name_map[cluster_hash]
	}

	find_cluster(key){
		let key_hash = this.get_hash(key)
		let cluster_index = this.binary_search(key_hash, true)
		if (cluster_index > 0 && key_hash > this.sorted_hashes[cluster_index]){ cluster_index += 1 }
		let cluster_hash = this.sorted_hashes[cluster_index]
		let cluster_name = this.hash_to_name_map[cluster_hash]
		console.log(`find_cluster(): key: ${key}, key_hash: ${key_hash}, cluster_index: ${cluster_index}`)
		console.log(`find_cluster(): this.sorted_hashes[cluster_index]:`, this.sorted_hashes[cluster_index])
		console.log(`find_cluster(): cluster_hash: ${cluster_hash}, this.hash_to_name_map[cluster_hash]: ${this.hash_to_name_map[cluster_hash]}`)
		console.log(`find_cluster(): cluster_name: ${cluster_name}`)
		return cluster_name
	}

	binary_search(target, key_search){
		let left = 0
	    let right = this.sorted_hashes.length - 1

		if (key_search == true && target > this.sorted_hashes[right]){
			return 0	// here is where the ring wraps around
		}
	    
	    while (left <= right){
	        let middle = Math.floor((left + right)/2)
	        if (this.sorted_hashes[middle] < target){
	            left = middle + 1
	        } else if (this.sorted_hashes[middle] > target) {
	            right = middle - 1
	        }
	    }
	    return left
	}

}


module.exports = ConsistentHashing