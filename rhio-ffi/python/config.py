import argparse

from rhio import Cli, Config


def parse_args():
    # parse arguments
    parser = argparse.ArgumentParser(
        description="p2p blob syncing node for minio databases"
    )
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
    parser.add_argument(
        "-s",
        "--sync-dir",
        type=str,
        help="path to sync directory (for use with example/sync)",
    )
    parser.add_argument(
        "-b", "--blobs-dir", type=str, help="path to blob store and database"
    )
    parser.add_argument("-n", "--minio-bucket-name", type=str, help="minio bucket name")
    parser.add_argument(
        "-e",
        "--minio-endpoint",
        type=str,
        help="minio instance endpoint address",
    )
    parser.add_argument(
        "-g",
        "--minio-region",
        type=str,
        help="minio instance region",
    )
    parser.add_argument(
        "-c",
        "--minio-credentials",
        type=str,
        help="minio credentials in the format <ACCESS_KEY>:<SECRET_KEY>",
    )
    parser.add_argument("-r", "--relay", type=str, help="relay addresses")

    return parser.parse_args()

def config_from_args(args):
    cli = Cli()
    cli.bind_port = args.port
    cli.ticket = args.ticket
    cli.private_key = args.private_key
    cli.sync_dir = args.sync_dir
    cli.blobs_dir = args.blobs_dir
    cli.minio_bucket_name = args.minio_bucket_name
    cli.minio_endpoint = args.minio_endpoint
    cli.minio_region = args.minio_region
    cli.minio_credentials = args.minio_credentials
    cli.relay = args.relay
    return Config.from_cli(cli)
