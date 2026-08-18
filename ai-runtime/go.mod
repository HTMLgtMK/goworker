module github.com/tinguo/goworker/ai-runtime

go 1.26.1

require (
	github.com/tinguo/goworker/ai-core v0.0.0-00010101000000-000000000000
	github.com/tinguo/goworker/ai-memory v0.0.0-00010101000000-000000000000
	github.com/tinguo/goworker/ai-sandbox v0.0.0-00010101000000-000000000000
	gopkg.in/yaml.v3 v3.0.1
)

replace (
	github.com/tinguo/goworker/ai-core => ../ai-core
	github.com/tinguo/goworker/ai-memory => ../ai-memory
	github.com/tinguo/goworker/ai-sandbox => ../ai-sandbox
)
