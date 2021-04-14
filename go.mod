module github.com/adobe/ferry

go 1.16

require (
	github.com/adobe/blackhole v0.1.5
	github.com/apple/foundationdb/bindings/go v0.0.0-20210409175928-8627fa1f16be
	github.com/google/uuid v1.2.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.5.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.7.1
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20210410081132-afb366fc7cd1 // indirect
	golang.org/x/sys v0.0.0-20210414055047-fe65e336abe0 // indirect
	google.golang.org/genproto v0.0.0-20210414175830-92282443c685 // indirect
	google.golang.org/grpc v1.37.0
	google.golang.org/protobuf v1.26.0

)

// uncomment during local developement across both repos
// replace github.com/adobe/blackhole => ../blackhole
