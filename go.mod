module go-noti-server

go 1.24.0

require (
	github.com/joho/godotenv v1.5.1
	github.com/newrelic/go-agent/v3 v3.41.0
	google.golang.org/grpc v1.76.0
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/newrelic/go-agent/v3/integrations/logcontext-v2/nrlogrus v1.1.2
	github.com/sirupsen/logrus v1.9.3
)

require github.com/stretchr/testify v1.11.1 // indirect

require (
	golang.org/x/net v0.46.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/text v0.30.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251029180050-ab9386a59fda // indirect
)
