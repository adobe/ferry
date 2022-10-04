module github.com/adobe/ferry

go 1.16

require (
	github.com/adobe/blackhole v0.1.5
	github.com/apple/foundationdb/bindings/go v0.0.0-20220711033714-dfe8dacba348
	github.com/google/uuid v1.3.0
	github.com/inconshreveable/mousetrap v1.0.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.6.0
	github.com/spf13/afero v1.9.2 // indirect
	github.com/spf13/cobra v1.5.0
	github.com/spf13/viper v1.13.0
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/zap v1.23.0
	golang.org/x/net v0.0.0-20220826154423-83b083e8dc8b // indirect
	golang.org/x/sys v0.0.0-20220928140112-f11e5e49a4ec // indirect
	google.golang.org/genproto v0.0.0-20220822174746-9e6da59bd2fc // indirect
	google.golang.org/grpc v1.49.0
	google.golang.org/protobuf v1.28.1

)

// uncomment during local developement across both repos
// replace github.com/adobe/blackhole => ../blackhole
replace github.com/adobe/blackhole => ../blackhole
