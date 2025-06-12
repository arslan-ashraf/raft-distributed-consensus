function get_timestamp() {
	const now = new Date();
	const hours = String(now.getHours())
	const minutes = String(now.getMinutes())
	const seconds = String(now.getSeconds())
	const milliseconds = String(now.getMilliseconds())
	return `${hours}:${minutes}:${seconds}:${milliseconds}`
}

module.exports = get_timestamp