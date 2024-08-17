import argparse

from rhio import Cli, Config

def parse_config():
    # parse arguments
    parser = argparse.ArgumentParser(description="Python Rhio Node")
    parser.add_argument("-p", "--port", type=int, default=2024, help="node bind port")
    parser.add_argument(
        "-t",
        "--ticket",
        type=str,
        action="append",
        default=[],
        help="connection ticket string",
    )
    parser.add_argument("-k", "--private-key", type=str, help="path to private key")
    parser.add_argument("-s", "--sync-dir", type=str, help="path to sync directory")
    parser.add_argument("-b", "--blobs-dir", type=str, help="path to blob store and database")
    parser.add_argument("-i", "--import-path", type=str, help="path or URL to file which should be imported to the blob store")
    parser.add_argument("-n", "--bucket-name", type=str, help="bucket name")
    parser.add_argument("-a", "--bucket-address", type=str, help="bucket address in the format <ENDPOINT>:<REGION>")
    parser.add_argument("-c", "--credentials", type=str, help="minio credentials in the format <ACCESS_KEY>:<SECRET_KEY>")
    parser.add_argument("-r", "--relay", type=str, help="relay addresses")

    args = parser.parse_args()

    cli = Cli()
    cli.bind_port = args.port
    cli.ticket = args.ticket
    cli.private_key = args.private_key
    cli.sync_dir = args.sync_dir
    cli.blobs_dir = args.blobs_dir
    cli.bucket_name = args.bucket_name
    cli.bucket_address = args.bucket_address
    cli.import_path = args.import_path
    cli.credentials = args.credentials
    cli.relay = args.relay
    return Config.from_cli(cli)