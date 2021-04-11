module github.com/adobe/ferry

go 1.16

require (
	github.com/adobe/blackhole v0.1.5-0.20210405174844-4ec273713a87
	github.com/apple/foundationdb/bindings/go v0.0.0-20210409175928-8627fa1f16be
	github.com/google/uuid v1.2.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.5.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.7.1
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20210326060303-6b1517762897 // indirect
	google.golang.org/genproto v0.0.0-20210325224202-eed09b1b5210 // indirect
	google.golang.org/grpc v1.36.1
	google.golang.org/protobuf v1.26.0

)

replace github.com/adobe/blackhole => ../blackhole
