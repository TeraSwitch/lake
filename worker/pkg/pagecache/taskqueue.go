package pagecache

// TaskQueue is the Temporal task queue for page cache refresh workflows.
// This queue is consumed by an embedded worker in the API process.
const TaskQueue = "page-cache"
