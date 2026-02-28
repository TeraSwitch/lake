package statuscache

// TaskQueue is the Temporal task queue for status cache refresh workflows.
// This queue is consumed by an embedded worker in the API process.
const TaskQueue = "status-cache"
