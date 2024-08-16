import argparse

from rhio import Config

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
    parser.add_argument("-c", "--credentials", type=str, help="minio credentials in the format <ACCESS_KEY>:<SECRET_KEY>")
    parser.add_argument("-r", "--relay", type=str, help="relay addresses")

    args = parser.parse_args()

    # construct node config
    config = Config()
    config.bind_port = args.port
    config.ticket = args.ticket
    config.private_key = args.private_key
    config.sync_dir = args.sync_dir
    config.blobs_dir = args.blobs_dir
    config.import_path = args.import_path
    config.credentials = args.credentials
    config.relay = args.relay
    return config