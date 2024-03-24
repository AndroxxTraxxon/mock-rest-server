"""mock_rest_server"""

from http.server import HTTPServer
from argparse import ArgumentParser
from threading import Thread
from time import sleep
from pathlib import Path
from ssl import SSLContext, PROTOCOL_TLS_SERVER
import subprocess

from mock_rest_server.database import JsonDatabase
from mock_rest_server.server import JsonHttpRequestHandler

SSL_CERT_CONFIG_TEMPLATE = """[dn]
CN={cn}
[req]
distinguished_name = dn
[EXT]
subjectAltName=DNS:{cn}
keyUsage=digitalSignature
extendedKeyUsage=serverAuth"""


def _generate_localhost_cert_args(cert_path: Path, key_path: Path, cn: str):
    config_path = Path(f"tmp_mock.ssl.{cn}.conf").resolve()
    with config_path.open("w+", encoding="utf-8") as config_file:
        print(f"Writing Certificate config to {str(config_path)}")
        config_file.write(SSL_CERT_CONFIG_TEMPLATE.format(cn=cn))
    return [
        "openssl",
        "req",
        "-x509",
        "-out",
        str(cert_path.resolve()),
        "-keyout",
        str(key_path.resolve()),
        "-newkey",
        "rsa:4096",
        "-nodes",
        "-sha256",
        "-subj",
        "/CN=localhost",
        "-extensions",
        "EXT",
        "-config",
        str(config_path),
    ]


def main() -> None:
    """Main entry point to run the HTTP webserver from the CLI."""
    parser = ArgumentParser(description="HTTP Server")
    parser.add_argument(
        "--port", "-p", type=int, help="Listening port for HTTP Server", default=8080
    )
    parser.add_argument("--address", "-ip", help="HTTP Server IP", default="0.0.0.0")
    parser.add_argument("--dbfile", "-db", type=Path, default="mock-rest.db.json")
    parser.add_argument(
        "--db-id-field", default="id", help="record field to use for record ID lookups."
    )
    parser.add_argument(
        "--db-min-persist-period",
        default=30,
        help="number of seconds to wait between persisting updated database contents.",
    )
    parser.add_argument("--secure", "-s", action="store_true")
    parser.add_argument(
        "--ssl-keyfile", "-key", type=Path, default=Path("localhost.key")
    )
    parser.add_argument(
        "--ssl-certfile", "-cert", type=Path, default=Path("localhost.cert")
    )
    parser.add_argument("--ssl-generate", "-gen", action="store_true")
    parser.add_argument("--ssl-cn", "-cn", default="localhost")
    args = parser.parse_args()
    db_file: Path = args.dbfile.resolve()

    database = JsonDatabase(db_file, args.db_id_field, args.db_min_persist_period)
    JsonHttpRequestHandler.configure(database=database)
    protocol, hostname, server = configure_server(args, JsonHttpRequestHandler)

    print(f"Serving Mock REST database server from {args.dbfile}")
    print(f"Web services available at {protocol}://{hostname}:{args.port}")

    server_thread = Thread(target=server.serve_forever)
    database_thread = Thread(target=database.maintain_data_persistence)
    server_thread.start()
    database_thread.start()
    try:
        while server_thread.is_alive():
            sleep(0.5)
    except KeyboardInterrupt:
        print("\nShutting Down...")
        Thread(target=server.shutdown).start()
        Thread(target=database.shutdown).start()
        server_thread.join(5)
        database_thread.join(5)

        if server_thread.is_alive():
            print(f"Unable to stop server thread (id {server_thread.native_id})")
        if database_thread.is_alive():
            print(f"Unable to stop database (id {database_thread.native_id})")


def configure_server(args, handler: type):
    """Configure the HTTP server for runtime"""
    protocol = "http"
    hostname = args.address if args.address != "0.0.0.0" else "localhost"
    server = HTTPServer((args.address, args.port), handler)
    if args.secure:
        protocol = "https"
        hostname = enable_https(args, server)
    return protocol, hostname, server


def enable_https(args, server: HTTPServer):
    """
    Enable HTTPS on the provided HTTPServer instance,
    returning the registered hostname

    Optionally, generates a self-signed certificate to serve the site.

    """
    if args.ssl_generate:
        if args.ssl_keyfile.is_file() and args.ssl_certfile.is_file():
            print(
                f"SSL Private Key ({args.ssl_keyfile}) "
                f"and Certificate ({args.ssl_certfile}) "
                "already exist."
            )
        else:
            subprocess.run(
                _generate_localhost_cert_args(
                    args.ssl_certfile, args.ssl_keyfile, args.ssl_cn
                ),
                check=False,
            )
    ssl_context = SSLContext(PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(
        certfile=str(args.ssl_certfile.resolve()),
        keyfile=str(args.ssl_keyfile.resolve()),
    )
    server.socket = ssl_context.wrap_socket(server.socket, server_side=True)
    hostname = (
        subprocess.run(
            [
                "openssl",
                "x509",
                "-noout",
                "-subject",
                "-in",
                str(args.ssl_certfile.resolve()),
            ],
            capture_output=True,
            check=False,
        )
        .stdout.decode()
        .strip()  # stdout ends with newline
        .rsplit(" ", 1)[-1]  # format is 'subject=CN = <hostname>'
    )

    return hostname


if __name__ == "__main__":
    main()
