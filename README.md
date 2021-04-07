# ferry (Work in Progress)

## Goals

Provide a management utility to import and export data out of FoundationDB.
There is experimental support for transparent write to `file://`, `s3://`, or `az://` urls. 
`az://` is a non-standard shorthand for azure blobstore urls.

## Non-Goals

The utility is not designed to replace the wonderful `fdbbackup` tool.
We do not attempt to copy or follow the foundationdb mutation log.
We are also unable to fetch the entire DB in one transaction because
of the 5-second transaction limit. 

### Installation

(Work in progress - Build repo using `go build`)

### Quickstart

    # Install TLS certificates and AWS/Azure credentails on all nodes
	# in standard places. See 
	# https://aws.amazon.com/blogs/security/a-new-and-standardized-way-to-manage-credentials-in-the-aws-sdks/

	ferry serve -v  # Run on each node

	ferry export -s /path/to/local/directory/

	ferry export -s s3://bucket/path/to/directory
	# credentials via `aws configure`

	ferry export -s az://container/path/to/directory
	# credentials via environment. See help from error message

### Usage

	$ ./ferry -h
	This utility will export all (or filtered) data from FoundationDB
	to one of the possible stores - a local file-system folder, Azure blobstore or Amazon S3
	Export is not done in a single transaction and that implies you should only do this
	if your data is static or you don't care for it being a point-in-time snapshot

	Usage:
	ferry [flags]
	ferry [command]

	Available Commands:
	export      Export all (or filtered set of) keys and values from FoundationDB
	help        Help about any command
	import      Import all (or filtered set of) keys and values from an export
	info        Print info on effective config
	manage      manages blobstore directory (list or delete recursively)
	serve       Serve exporter grpc server

	Flags:
		--config string      config file (default is $HOME/.ferry.yaml)
	-h, --help               help for ferry
	-p, --port serve         Port to bind to (applies to serve and `export` commands
		--profile string     mem|cpu (Go performance profiling)
	-s, --store-url string   Source/target for export/import/manage (default "/tmp/")
	-v, --verbose            Verbose logging

	Use "ferry [command] --help" for more information about a command.

### Usage (Server - Rquired on each fdb node)

After configuring the bind port (defaults to 8001) via `.ferry.yaml` in working-directory or home-directory, along with TLS settings, start a server on each node with

	ferry serve


### Usage (Client - a host with access to fdb.cluster file)

	ferry export -s s3://bucket/path/to/directory

	Usage:
	ferry export [flags]

	Flags:
		--collect string   Bring backup files to this host at this directory. Only applies to file:// targets
	-c, --compress         Compress export files (.lz4)
	-n, --dryrun           Dryrun connectivity check
	-h, --help             help for export
	-m, --sample           Sample - fetch only 1000 keys per range
	-t, --threads int      How many threads per range

	Global Flags:
		--config string      config file (default is $HOME/.ferry.yaml)
	-p, --port serve         Port to bind to (applies to serve and `export` commands
		--profile string     mem|cpu (Go performance profiling)
	-s, --store-url string   Source/target for export/import/manage (default "/tmp/")
	-v, --verbose            Verbose logging


## Export format

Export format, in oversimplied term is a length-prefixed binary dump of the form

```
[[ length ] [ key-bytes ] [ value-bytes ]] . . . .

length = 4 bytes in little-endian format.
Once read as an Unsigned Int 32, split them as follows

higher 14 bits is key length
lower 18 bits is value length
both considered enough for FDB limits

Psuedo code below
key-length = length >> 18 & ((1 << 14) - 1)
value-length = length & (1 << 18) - 1

```

We may look at flatbuffer for output format, but that depends on subsequent usage needs

