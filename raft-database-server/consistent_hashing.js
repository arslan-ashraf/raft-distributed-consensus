const crypto = require("crypto")


class ConsistentHashing {
	
	constructor(){
		this.sorted_hashes = []
	}

	get_hash(str){
		let hash_function = crypto.createHash("blake2b512")
		hash_function.update(str)
		let hash_digest_hex = hash_function.digest("hex")	// hashed value in hexadecimal
		let hash_digest_decimal = parseInt(hash_digest_hex, 16) // hashed value in base10
		let hashed_string = hash_digest_decimal % 100
		return hashed_string	// an integer
	}

	add_cluster(cluster_name){
		let cluster_hash = this.get_hash(cluster_name)
		let insert_index = this.binary_search(cluster_hash)
		this.sorted_hashes.splice(insert_index, 0)
	}

	remove_cluster(cluster_name){
		let cluster_hash = this.get_hash(cluster_name)
		let remove_index = this.binary_search(cluster_hash)
		this.sorted_hashes.splice(remove_index, 1)
	}

	binary_search(target){
		if (target > this.sorted_hashes[this.sorted_hashes.length - 1]){
			return this.sorted_hashes[0]	// here is where the ring wraps around
		}
	    let left = 0
	    let right = this.sorted_hashes.length - 1
	    while (left <= right){
	        let middle = Math.floor((left + right)/2)
	        if (this.sorted_hashes[middle] < target){
	            left = middle + 1
	        } else if (this.sorted_hashes[middle] > target) {
	            right = middle - 1
	        }
	    }
	    return target > left ? left + 1 : left
	}

}


[0,1,2,3,4,5,6,7,8,9,10]