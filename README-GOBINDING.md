# Installing Go bindings

You must install Go binding explicitly with a command like the following

    go get github.com/apple/foundationdb/bindings/go/src/fdb@7.1.15

# Things to note

- FoundationDB releases don't use the typical `vX.Y.Z` tag format typically used by other github projects and something `go mod` relies on. So you must be careful during upgrades and avoid accidentally upgrading to an incompatible/HEAD binding version

    - https://github.com/apple/foundationdb/issues/3338#issuecomment-787210479

    - https://github.com/apple/foundationdb/issues/4431
